"""
Historical NRL data scraper.
Fetches match data from 2020-2026, including team lists and try scorers.
Runs at startup and stores everything in SQLite.
"""

import asyncio
import httpx
import logging
import time

from database import (
    init_db, is_round_scraped, mark_round_scraped, unmark_round,
    insert_match, bulk_insert_match_data, get_total_match_count,
)

logger = logging.getLogger(__name__)

BASE_URL = "https://www.nrl.com"
COMPETITION_ID = 111
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json, text/html, */*",
}

# Season config: (year, total_rounds_including_finals)
SEASONS = [
    (2020, 24),  # 20 rounds + 4 finals
    (2021, 29),  # 25 rounds + 4 finals
    (2022, 29),  # 25 rounds + 4 finals
    (2023, 31),  # 27 rounds + 4 finals
    (2024, 31),  # 27 rounds + 4 finals
    (2025, 31),  # 27 rounds + 4 finals
    (2026, 27),  # current season - 27 rounds (only completed ones)
]


async def scrape_all():
    """Main entry point - scrape all historical data."""
    init_db()

    existing = get_total_match_count()
    logger.info(f"Database has {existing} completed matches. Starting scrape...")

    async with httpx.AsyncClient(headers=HEADERS, timeout=20.0, follow_redirects=True) as client:
        for season, total_rounds in SEASONS:
            for rnd in range(1, total_rounds + 1):
                if is_round_scraped(season, rnd):
                    continue

                logger.info(f"Scraping {season} round {rnd}...")
                try:
                    await scrape_round(client, season, rnd)
                    mark_round_scraped(season, rnd)
                except Exception as e:
                    logger.error(f"Error scraping {season} R{rnd}: {e}")

                # Rate limiting - be respectful
                await asyncio.sleep(0.5)

    final = get_total_match_count()
    logger.info(f"Scrape complete. Database now has {final} completed matches.")


async def scrape_round(client: httpx.AsyncClient, season: int, round_number: int):
    """Scrape all matches in a round."""
    url = f"{BASE_URL}/draw/data?competition={COMPETITION_ID}&season={season}&round={round_number}"
    resp = await client.get(url)
    if resp.status_code != 200:
        logger.warning(f"Failed to fetch {season} R{round_number}: {resp.status_code}")
        return

    data = resp.json()
    fixtures = data.get("fixtures", [])

    for match in fixtures:
        if not isinstance(match, dict):
            continue

        match_state = match.get("matchState", "")
        if match_state not in ("FullTime", "PostMatch"):
            continue  # Only store completed matches

        home = match.get("homeTeam", {})
        away = match.get("awayTeam", {})
        clock = match.get("clock", {})
        match_url = match.get("matchCentreUrl", "")

        if not match_url:
            continue

        round_title = match.get("roundTitle", f"Round {round_number}")
        home_team = home.get("nickName", "Unknown")
        away_team = away.get("nickName", "Unknown")
        home_score = home.get("score")
        away_score = away.get("score")
        venue = match.get("venue", "")
        venue_city = match.get("venueCity", "")
        kickoff = clock.get("kickOffTimeLong", "") if isinstance(clock, dict) else ""

        match_id = insert_match(
            season, round_number, round_title, match_url, match_state,
            home_team, away_team, home_score, away_score, venue, kickoff,
            venue_city=venue_city
        )

        if not match_id:
            continue

        # Fetch match detail for team lists and try scorers
        try:
            await scrape_match_detail(client, match_id, match_url, home_team, away_team)
            await asyncio.sleep(0.3)  # Rate limit between match details
        except Exception as e:
            logger.error(f"Error fetching detail for {match_url}: {e}")


# NRL jersey number → field side mapping
# From the ATTACKING team's perspective
JERSEY_FIELD_SIDE = {
    1: "fullback",  # Fullback — own category, scores across all edges
    2: "right",     # Right Wing
    3: "right",     # Right Centre
    4: "left",      # Left Centre
    5: "left",      # Left Wing
    6: "left",      # Five-Eighth (typically left side)
    7: "right",     # Halfback (typically right side)
    8: "middle",    # Prop
    9: "middle",    # Hooker
    10: "middle",   # Prop
    11: "left",     # Left 2nd Row
    12: "right",    # Right 2nd Row
    13: "middle",   # Lock
}


def _determine_field_side(player_name, jersey_number, side, players_data,
                          interchanges_data, try_game_seconds):
    """
    Determine the field side for a try scorer.
    For starters (1-13): use their jersey number directly.
    For bench (14+): find which starter they replaced via interchange data,
    and use that starter's jersey number to infer field side.
    """
    if 1 <= jersey_number <= 13:
        return JERSEY_FIELD_SIDE.get(jersey_number, "middle")

    # Bench player (14+) — find the most recent interchange where they came on
    # before or around the try time, to determine whose spot they filled
    relevant_subs = [
        ic for ic in interchanges_data
        if ic["side"] == side
        and ic["player_on"] == player_name
        and ic["game_seconds"] <= try_game_seconds
    ]

    if relevant_subs:
        # Use the most recent substitution before the try
        relevant_subs.sort(key=lambda x: x["game_seconds"], reverse=True)
        replaced_name = relevant_subs[0]["player_off"]
        replaced_jersey = relevant_subs[0].get("jersey_off", 0)

        # If we know the replaced player's jersey, use it
        if 1 <= replaced_jersey <= 13:
            return JERSEY_FIELD_SIDE.get(replaced_jersey, "middle")

        # Fall back: look up the replaced player's jersey in the team list
        for p in players_data:
            if p["name"] == replaced_name and p["side"] == side:
                j = p["jersey_number"]
                if 1 <= j <= 13:
                    return JERSEY_FIELD_SIDE.get(j, "middle")
                break

    # Can't determine — fall back to middle
    return "middle"


def _parse_minute_to_seconds(minute_str: str) -> int:
    """Convert a minute string like \"27'\" to approximate game seconds."""
    try:
        mins = int(minute_str.replace("'", "").strip())
        return mins * 60
    except (ValueError, AttributeError):
        return 0


async def scrape_match_detail(client: httpx.AsyncClient, match_id: int,
                               match_url: str, home_team: str, away_team: str):
    """Fetch and store team lists, try scorers, and interchanges for a single match."""
    detail_url = f"{BASE_URL}{match_url.rstrip('/')}/data"
    resp = await client.get(detail_url)
    if resp.status_code != 200:
        logger.warning(f"Failed to fetch match detail: {detail_url}")
        return

    data = resp.json()
    players_data = []
    tries_data = []
    interchanges_data = []

    # Build player ID → name/jersey lookup from both teams
    player_id_map = {}  # {playerId: {"name": ..., "jersey": ..., "side": ...}}

    for side, team_key, team_name in [("home", "homeTeam", home_team), ("away", "awayTeam", away_team)]:
        team = data.get(team_key, {})

        # Parse players
        player_list = team.get("players", [])
        for p in player_list:
            if not isinstance(p, dict):
                continue
            first = p.get("firstName", "")
            last = p.get("lastName", "")
            name = f"{first} {last}".strip()
            if not name:
                continue

            jersey = p.get("number", 0)
            player_id = p.get("playerId", 0)

            if player_id:
                player_id_map[player_id] = {
                    "name": name,
                    "jersey": jersey,
                    "side": side,
                    "team": team_name,
                }

            players_data.append({
                "team": team_name,
                "side": side,
                "name": name,
                "jersey_number": jersey,
                "position": p.get("position", ""),
                "is_interchange": not p.get("isOnField", True),
            })

        # Parse tries (from scoring summaries)
        scoring = team.get("scoring", {})
        tries = scoring.get("tries", {})
        summaries = tries.get("summaries", [])
        for s in summaries:
            if not isinstance(s, str):
                continue
            parts = s.rsplit(" ", 1)
            player_name = parts[0].strip() if len(parts) == 2 else s.strip()
            minute = parts[1].strip() if len(parts) == 2 else ""

            tries_data.append({
                "team": team_name,
                "side": side,
                "player_name": player_name,
                "minute": minute,
            })

    # Parse timeline for interchange events
    timeline = data.get("timeline", [])
    # Also collect try events from timeline for better game_seconds data
    timeline_tries = {}  # {(side, game_seconds): player_id}

    for event in timeline:
        if not isinstance(event, dict):
            continue
        etype = event.get("type", "")

        if etype == "Interchange":
            on_id = event.get("playerId", 0)
            off_id = event.get("offPlayerId", 0)
            game_secs = event.get("gameSeconds", 0)
            team_id = event.get("teamId", 0)

            on_info = player_id_map.get(on_id, {})
            off_info = player_id_map.get(off_id, {})

            if on_info and off_info:
                interchanges_data.append({
                    "side": on_info.get("side", ""),
                    "player_on": on_info["name"],
                    "player_off": off_info["name"],
                    "jersey_on": on_info.get("jersey", 0),
                    "jersey_off": off_info.get("jersey", 0),
                    "game_seconds": game_secs,
                })

    # Now determine field_side for each try
    for t in tries_data:
        # Find the try scorer's jersey number from the player list
        scorer_jersey = 0
        for p in players_data:
            if p["name"] == t["player_name"] and p["side"] == t["side"]:
                scorer_jersey = p["jersey_number"]
                break

        try_seconds = _parse_minute_to_seconds(t["minute"])
        t["field_side"] = _determine_field_side(
            t["player_name"], scorer_jersey, t["side"],
            players_data, interchanges_data, try_seconds
        )

    # Update weather/ground conditions from match detail data
    weather = data.get("weather", "")
    ground_conditions = data.get("groundConditions", "")
    if weather or ground_conditions:
        from database import get_db as _get_db
        _conn = _get_db()
        _conn.execute(
            "UPDATE matches SET weather=%s, ground_conditions=%s WHERE id=%s",
            (weather or "", ground_conditions or "", match_id)
        )
        _conn.commit()
        _conn.close()

    if players_data or tries_data:
        bulk_insert_match_data(match_id, players_data, tries_data, interchanges_data)


CURRENT_SEASON = 2026
CURRENT_SEASON_ROUNDS = 27


async def sync_current_season():
    """
    Re-scrape recent rounds of the current season to pick up
    newly completed games (e.g. last night's results).
    Unmarks the last 3 rounds and re-scrapes them.
    """
    init_db()
    logger.info("Starting current season sync...")

    # Find the approximate current round by checking which rounds have data
    async with httpx.AsyncClient(headers=HEADERS, timeout=20.0, follow_redirects=True) as client:
        # Check rounds from the end backwards to find the latest active round
        latest_round = 1
        for rnd in range(CURRENT_SEASON_ROUNDS, 0, -1):
            url = f"{BASE_URL}/draw/data?competition={COMPETITION_ID}&season={CURRENT_SEASON}&round={rnd}"
            try:
                resp = await client.get(url)
                if resp.status_code == 200:
                    data = resp.json()
                    fixtures = data.get("fixtures", [])
                    has_completed = any(
                        f.get("matchState") in ("FullTime", "PostMatch")
                        for f in fixtures if isinstance(f, dict)
                    )
                    if has_completed:
                        latest_round = rnd
                        break
            except Exception:
                continue
            await asyncio.sleep(0.2)

        # Re-scrape the last 3 rounds (covers current + recent)
        start_round = max(1, latest_round - 2)
        end_round = min(CURRENT_SEASON_ROUNDS, latest_round + 1)

        for rnd in range(start_round, end_round + 1):
            logger.info(f"Syncing {CURRENT_SEASON} round {rnd}...")
            unmark_round(CURRENT_SEASON, rnd)
            try:
                await scrape_round(client, CURRENT_SEASON, rnd)
                mark_round_scraped(CURRENT_SEASON, rnd)
            except Exception as e:
                logger.error(f"Error syncing {CURRENT_SEASON} R{rnd}: {e}")
            await asyncio.sleep(0.5)

    count = get_total_match_count()
    logger.info(f"Sync complete. Database now has {count} completed matches.")


def run_scraper():
    """Synchronous wrapper to run the async scraper."""
    asyncio.run(scrape_all())


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_scraper()
