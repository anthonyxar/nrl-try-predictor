"""
Client for fetching live data from the NRL website API.

Draw endpoint:  /draw/data?competition=111&season=2026&round=N
Match endpoint: /draw/nrl-premiership/2026/round-N/team-v-team/data
"""

import httpx
import logging
from typing import Optional

logger = logging.getLogger(__name__)

BASE_URL = "https://www.nrl.com"
COMPETITION_ID = 111
SEASON = 2026
TOTAL_ROUNDS = 27

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json, text/html, */*",
    "Accept-Language": "en-AU,en;q=0.9",
}


async def fetch_round(round_number: int) -> Optional[dict]:
    """Fetch all fixtures for a given round from the NRL API."""
    url = f"{BASE_URL}/draw/data?competition={COMPETITION_ID}&season={SEASON}&round={round_number}"
    async with httpx.AsyncClient(headers=HEADERS, timeout=15.0, follow_redirects=True) as client:
        resp = await client.get(url)
        if resp.status_code != 200:
            logger.error(f"NRL round API returned {resp.status_code}")
            return None
        return resp.json()


async def fetch_match_detail(match_url_path: str) -> Optional[dict]:
    """
    Fetch full match detail including team lists.
    match_url_path: /draw/nrl-premiership/2026/round-5/rabbitohs-v-bulldogs/
    """
    # Ensure trailing slash and append 'data'
    path = match_url_path.rstrip("/") + "/data"
    url = f"{BASE_URL}{path}"
    async with httpx.AsyncClient(headers=HEADERS, timeout=15.0, follow_redirects=True) as client:
        resp = await client.get(url)
        if resp.status_code != 200:
            logger.error(f"NRL match API returned {resp.status_code} for {url}")
            return None
        return resp.json()


def parse_fixtures(raw_data: dict) -> list:
    """
    Parse raw NRL draw data into a clean list of fixtures.
    Structure: raw_data["fixtures"] = list of match dicts
    """
    fixtures = []
    raw_fixtures = raw_data.get("fixtures", [])

    for match in raw_fixtures:
        if not isinstance(match, dict):
            continue

        home = match.get("homeTeam", {})
        away = match.get("awayTeam", {})
        clock = match.get("clock", {})

        match_state = match.get("matchState", "")
        is_completed = match_state in ("FullTime", "PostMatch")

        # Team lists are announced if the match has started/completed,
        # or if team lists are published (we check in match detail API)
        # For the draw view, we mark completed and upcoming matches as having lists
        # The actual check happens when clicking into a match
        has_team_lists = is_completed or match_state in ("HalfTime", "InProgress", "Upcoming", "Pre")

        # Get theme key for colour lookup
        home_theme = home.get("theme", {})
        away_theme = away.get("theme", {})

        fixtures.append({
            "match_id": match.get("matchId", ""),
            "home_team": home.get("nickName", "Unknown"),
            "away_team": away.get("nickName", "Unknown"),
            "home_theme_key": home_theme.get("key", "") if isinstance(home_theme, dict) else "",
            "away_theme_key": away_theme.get("key", "") if isinstance(away_theme, dict) else "",
            "home_score": home.get("score"),
            "away_score": away.get("score"),
            "venue": match.get("venue", ""),
            "venue_city": match.get("venueCity", ""),
            "kickoff": clock.get("kickOffTimeLong", "") if isinstance(clock, dict) else "",
            "match_url": match.get("matchCentreUrl", ""),
            "match_state": match_state,
            "team_lists_announced": has_team_lists,
            "home_odds": home.get("odds", ""),
            "away_odds": away.get("odds", ""),
        })

    # Sort by kickoff time
    fixtures.sort(key=lambda f: f["kickoff"] or "")

    # Also extract byes
    byes = []
    for bye in raw_data.get("byes", []):
        if isinstance(bye, dict):
            byes.append(bye.get("teamNickName", "Unknown"))

    return fixtures, byes


def parse_team_list(raw_data: dict, team_key: str) -> list:
    """
    Parse team list from match detail data.
    team_key: 'homeTeam' or 'awayTeam'

    Player fields from NRL API:
    - firstName, lastName
    - number (jersey number)
    - position (e.g. "Fullback", "Winger", "Prop")
    - isOnField (true = starting, false = interchange)
    """
    players = []
    team = raw_data.get(team_key, {})
    player_list = team.get("players", [])

    if not player_list:
        return []

    for p in player_list:
        if not isinstance(p, dict):
            continue

        first = p.get("firstName", "")
        last = p.get("lastName", "")
        name = f"{first} {last}".strip() or p.get("name", "Unknown")

        # Build headshot URL
        head_img = p.get("headImage", "")
        if head_img:
            # Strip NRL's remote.axd proxy (returns 503 cross-origin)
            proxy_marker = "remote.axd?"
            idx = head_img.find(proxy_marker)
            if idx >= 0:
                head_img = head_img[idx + len(proxy_marker):]
            elif not head_img.startswith("http"):
                head_img = f"{BASE_URL}{head_img}"
            # Strip ?center= crop param
            center_idx = head_img.find("?center=")
            if center_idx >= 0:
                head_img = head_img[:center_idx]
            # Keep all absolute URLs as-is — browsers can load cross-origin
            # images via <img> tags without CORS issues

        players.append({
            "name": name,
            "number": p.get("number", 0),
            "position": p.get("position", "Interchange"),
            "is_interchange": not p.get("isOnField", True),
            "is_captain": p.get("isCaptain", False),
            "headshot": head_img,
        })

    # Sort by jersey number
    players.sort(key=lambda x: x["number"])
    return players


def parse_team_stats(raw_data: dict) -> dict:
    """
    Extract team season stats from the stats.groups structure.
    stats.groups[0] contains current season stats with home/away values.
    """
    stats_obj = raw_data.get("stats", {})
    groups = stats_obj.get("groups", [])

    result = {"home": {}, "away": {}}

    if not groups:
        return result

    # First group = current season stats
    season_group = groups[0] if groups else {}
    stat_list = season_group.get("stats", [])

    for stat in stat_list:
        if not isinstance(stat, dict):
            continue
        title = stat.get("title", "").lower()
        home_val = _extract_stat_value(stat.get("homeValue"))
        away_val = _extract_stat_value(stat.get("awayValue"))

        if "completion" in title:
            result["home"]["completion_rate"] = home_val
            result["away"]["completion_rate"] = away_val
        elif "tackle" in title and "eff" in title:
            result["home"]["tackle_efficiency"] = home_val
            result["away"]["tackle_efficiency"] = away_val
        elif "average" in title and "scored" in title:
            result["home"]["avg_points_scored"] = home_val
            result["away"]["avg_points_scored"] = away_val
        elif "average" in title and "conceded" in title:
            result["home"]["avg_points_conceded"] = home_val
            result["away"]["avg_points_conceded"] = away_val
        elif "points scored" in title and "average" not in title:
            result["home"]["points_scored"] = home_val
            result["away"]["points_scored"] = away_val
        elif "points conceded" in title and "average" not in title:
            result["home"]["points_conceded"] = home_val
            result["away"]["points_conceded"] = away_val
        elif "win" in title:
            result["home"]["win_rate"] = home_val
            result["away"]["win_rate"] = away_val
            # Extract W/L from numerator/denominator
            home_raw = stat.get("homeValue", {})
            away_raw = stat.get("awayValue", {})
            if isinstance(home_raw, dict):
                result["home"]["wins"] = home_raw.get("numerator", 0)
                result["home"]["played"] = home_raw.get("denominator", 0)
                result["home"]["losses"] = result["home"]["played"] - result["home"]["wins"]
            if isinstance(away_raw, dict):
                result["away"]["wins"] = away_raw.get("numerator", 0)
                result["away"]["played"] = away_raw.get("denominator", 0)
                result["away"]["losses"] = result["away"]["played"] - result["away"]["wins"]

    # Extract from team-level data too
    for side, key in [("home", "homeTeam"), ("away", "awayTeam")]:
        team = raw_data.get(key, {})
        if team.get("teamPosition"):
            result[side]["ladder_position"] = team["teamPosition"]
        if team.get("odds"):
            result[side]["odds"] = team["odds"]

    return result


def parse_scoring(raw_data: dict) -> dict:
    """
    Parse try scoring data from a completed match.
    Try summaries are strings like "Tom Trbojevic 14'"

    Returns:
        {
            "home_score": 18, "away_score": 52,
            "home_tries": [{"player": "...", "minute": "14'"}],
            "away_tries": [...],
            "home_try_scorers": ["Name", ...],  # unique names
            "away_try_scorers": ["Name", ...],
        }
    """
    result = {
        "home_score": None, "away_score": None,
        "home_tries": [], "away_tries": [],
        "home_try_scorers": [], "away_try_scorers": [],
    }

    home = raw_data.get("homeTeam", {})
    away = raw_data.get("awayTeam", {})

    result["home_score"] = home.get("score")
    result["away_score"] = away.get("score")

    for side, team in [("home", home), ("away", away)]:
        scoring = team.get("scoring", {})
        tries = scoring.get("tries", {})
        summaries = tries.get("summaries", [])

        parsed_tries = []
        scorer_names = []
        for s in summaries:
            if not isinstance(s, str):
                continue
            # Format: "Player Name 14'" or "Player Name 14' (pen)"
            # Split on last space-followed-by-digit pattern
            parts = s.rsplit(" ", 1)
            if len(parts) == 2:
                player = parts[0].strip()
                minute = parts[1].strip()
            else:
                player = s.strip()
                minute = ""
            parsed_tries.append({"player": player, "minute": minute})
            if player not in scorer_names:
                scorer_names.append(player)

        result[f"{side}_tries"] = parsed_tries
        result[f"{side}_try_scorers"] = scorer_names

    return result


def _extract_stat_value(val) -> float:
    """
    Extract numeric value from NRL stat objects.
    Stats come as: {"value": 67.0, "isLeader": false, ...}
    or as plain numbers/strings.
    """
    if isinstance(val, dict):
        return float(val.get("value", 0))
    return _parse_number(val)


def _parse_number(val) -> float:
    """Parse a number from various formats like '77%', '26', '3.53'."""
    if isinstance(val, (int, float)):
        return float(val)
    if isinstance(val, str):
        cleaned = val.replace("%", "").replace(",", "").strip()
        try:
            return float(cleaned)
        except ValueError:
            return 0.0
    return 0.0
