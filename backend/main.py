import os
import re
import logging
import asyncio
from contextlib import asynccontextmanager

import httpx
from starlette.middleware.base import BaseHTTPMiddleware
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, Response

from nrl_client import (
    fetch_round, fetch_match_detail,
    parse_fixtures, parse_team_list, parse_team_stats, parse_scoring,
    TOTAL_ROUNDS, SEASON,
)
from model import (
    generate_predictions, predict_win_probability,
    generate_multi_suggestion, find_value_picks, generate_team_summary,
    invalidate_cache,
)
from database import (
    init_db, get_total_match_count, get_total_try_count,
    get_player_game_log, get_db,
    upsert_prediction, get_accuracy_stats, get_unrecorded_completed_matches,
    search_players, search_teams, get_all_teams,
    get_team_roster, get_team_recent_results,
    get_team_attack_defence, get_home_away_win_rate,
    get_team_tries_conceded_by_edge, get_venue_stats,
    prefetch_round_data,
)
from odds_client import (
    add_implied_odds_to_players,
    fetch_bookmaker_odds,
    lookup_bookmaker_odds,
    has_api_key as has_odds_api_key,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup: init DB, warm cache, run prediction backfill in background."""
    init_db()
    existing = get_total_match_count()
    logger.info(f"Starting up. DB has {existing} matches.")

    pred_task = asyncio.create_task(_prediction_sync())
    warmup_task = asyncio.create_task(_warm_cache())
    yield
    pred_task.cancel()
    warmup_task.cancel()


async def _warm_cache():
    """Pre-load the cache for the current round so first page load is fast."""
    try:
        await asyncio.sleep(1)
        logger.info("Warming cache...")

        # Find the latest round with completed or upcoming matches
        # Search forward from round 1 to find the current active round
        target_round = None
        target_raw = None
        for r in range(1, TOTAL_ROUNDS + 1):
            raw = await fetch_round(r)
            if not raw:
                continue
            fixtures, _ = parse_fixtures(raw)
            if not fixtures:
                continue
            # Check if this round has any non-FullTime matches (upcoming/live)
            has_upcoming = any(
                (f.get("match_state") or "").lower() not in ("fulltime", "postmatch")
                for f in fixtures
            )
            if has_upcoming:
                target_round = r
                target_raw = raw
                break
            # Track the latest completed round as fallback
            target_round = r
            target_raw = raw

        if target_raw and target_round:
            fixtures, _ = parse_fixtures(target_raw)
            logger.info(f"Warming cache for round {target_round} ({len(fixtures)} matches)...")
            await asyncio.to_thread(_enrich_fixtures, fixtures, 3, target_round)
            logger.info(f"Cache warmed for round {target_round}.")
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.warning(f"Cache warmup failed (non-critical): {e}")


PREDICTION_SYNC_INTERVAL = 600  # 10 minutes


async def _record_prediction_for_match(match_url: str, model_version: int = 3):
    """Fetch a completed match from the NRL API, run predictions, and record accuracy."""
    import json

    raw = await fetch_match_detail(match_url)
    if raw is None:
        return False

    match_state = raw.get("matchState", "")
    if match_state not in ("FullTime", "PostMatch"):
        return False

    home_players = parse_team_list(raw, "homeTeam")
    away_players = parse_team_list(raw, "awayTeam")
    if not home_players and not away_players:
        return False

    season_m = re.search(r'/(\d{4})/', match_url)
    round_m = re.search(r'/round-(\d+)/', match_url)
    before_season = int(season_m.group(1)) if season_m else None
    before_round = int(round_m.group(1)) if round_m else None
    if not before_season or not before_round:
        return False

    stats = parse_team_stats(raw)
    home_team = raw.get("homeTeam", {})
    away_team = raw.get("awayTeam", {})
    home_nickname = home_team.get("nickName", "Home")
    away_nickname = away_team.get("nickName", "Away")
    match_venue = raw.get("venue", "")
    match_weather = raw.get("weather", "")
    match_ground = raw.get("groundConditions", "")

    predictions = generate_predictions(
        home_players, away_players,
        stats.get("home", {}), stats.get("away", {}),
        home_team_name=home_nickname, away_team_name=away_nickname,
        model_version=model_version,
        before_season=before_season, before_round=before_round,
        weather=match_weather, ground_conditions=match_ground,
    )

    win_prediction = predict_win_probability(
        home_nickname, away_nickname,
        stats.get("home", {}), stats.get("away", {}),
        model_version=model_version,
        before_season=before_season, before_round=before_round,
        venue=match_venue, weather=match_weather, ground_conditions=match_ground,
    )

    multi = generate_multi_suggestion(
        predictions["home"], predictions["away"],
        home_nickname, away_nickname,
    )

    top3_home = [
        {"name": p["name"], "number": p["number"], "position": p["position"], "try_percentage": p["try_percentage"]}
        for p in predictions["home"][:3]
    ]
    top3_away = [
        {"name": p["name"], "number": p["number"], "position": p["position"], "try_percentage": p["try_percentage"]}
        for p in predictions["away"][:3]
    ]

    scoring = parse_scoring(raw)
    if not scoring:
        return False

    home_actual = set(scoring["home_try_scorers"])
    away_actual = set(scoring["away_try_scorers"])
    for pick in top3_home:
        pick["scored"] = pick["name"] in home_actual
    for pick in top3_away:
        pick["scored"] = pick["name"] in away_actual

    all_actual = home_actual | away_actual
    multi_hits = sum(1 for p in multi["picks"] if p["name"] in all_actual)
    for p in multi["picks"]:
        p["scored"] = p["name"] in all_actual

    actual_winner = None
    win_correct = None
    if scoring["home_score"] is not None and scoring["away_score"] is not None:
        if scoring["home_score"] > scoring["away_score"]:
            actual_winner = home_nickname
        elif scoring["away_score"] > scoring["home_score"]:
            actual_winner = away_nickname
        else:
            actual_winner = "Draw"
        win_correct = 1 if win_prediction["predicted_winner"] == actual_winner else 0

    t3h_json = json.dumps([{"name": p["name"], "scored": p.get("scored")} for p in top3_home])
    t3a_json = json.dumps([{"name": p["name"], "scored": p.get("scored")} for p in top3_away])
    t3_hits = sum(1 for p in top3_home if p.get("scored")) + sum(1 for p in top3_away if p.get("scored"))
    m_json = json.dumps([{"name": p["name"], "team": p["team"], "scored": p.get("scored")} for p in multi["picks"]])

    upsert_prediction(
        match_url=match_url, season=before_season, round_number=before_round,
        model_version=model_version, home_team=home_nickname, away_team=away_nickname,
        predicted_winner=win_prediction["predicted_winner"],
        home_win_prob=win_prediction["home_win_prob"],
        predicted_home_score=win_prediction["predicted_home_score"],
        predicted_away_score=win_prediction["predicted_away_score"],
        actual_winner=actual_winner,
        actual_home_score=scoring.get("home_score"),
        actual_away_score=scoring.get("away_score"),
        win_correct=win_correct,
        top3_home_json=t3h_json, top3_away_json=t3a_json, top3_hits=t3_hits,
        multi_json=m_json, multi_hits=multi_hits,
        multi_all_scored=1 if multi_hits == len(multi["picks"]) else 0,
    )
    return True


async def _backfill_predictions():
    """Record predictions for all completed matches that haven't been recorded yet."""
    for mv in (1, 2, 3):
        unrecorded = get_unrecorded_completed_matches(model_version=mv)
        if not unrecorded:
            continue
        logger.info(f"Backfilling {len(unrecorded)} prediction(s) for V{mv}...")
        recorded = 0
        for match in unrecorded:
            try:
                ok = await _record_prediction_for_match(match["match_url"], model_version=mv)
                if ok:
                    recorded += 1
                await asyncio.sleep(0.5)  # rate limit NRL API
            except Exception as e:
                logger.warning(f"Failed to record prediction for {match['match_url']} V{mv}: {e}")
        logger.info(f"Backfilled {recorded}/{len(unrecorded)} predictions for V{mv}.")


async def _prediction_sync():
    """Background task: backfill on startup, then check for new completions periodically."""
    # Wait for scraper to finish populating the matches table
    await asyncio.sleep(30)
    while True:
        try:
            # Wait until scraper has data
            if get_total_match_count() > 0:
                break
        except Exception:
            pass
        await asyncio.sleep(10)

    # Initial backfill
    try:
        await _backfill_predictions()
    except Exception as e:
        logger.error(f"Prediction backfill error: {e}")

    # Periodic check for newly completed matches
    while True:
        await asyncio.sleep(PREDICTION_SYNC_INTERVAL)
        try:
            await _backfill_predictions()
        except asyncio.CancelledError:
            return
        except Exception as e:
            logger.error(f"Prediction sync error: {e}")


app = FastAPI(title="NRL Try Predictor", version="3.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


class CacheControlMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)
        path = request.url.path
        if path == "/api/health":
            response.headers["Cache-Control"] = "no-cache"
        elif path.startswith("/api/match"):
            # Match detail: short cache for upcoming, long for completed
            response.headers["Cache-Control"] = "public, max-age=120, stale-while-revalidate=300"
        elif path.startswith("/api/rounds/"):
            # Round page: 2 min cache, serve stale while revalidating
            response.headers["Cache-Control"] = "public, max-age=120, stale-while-revalidate=300"
        elif path.startswith("/api/rounds"):
            # Rounds list: changes rarely
            response.headers["Cache-Control"] = "public, max-age=600"
        elif path.startswith("/api/accuracy"):
            response.headers["Cache-Control"] = "public, max-age=300"
        elif path.startswith("/api/team") or path.startswith("/api/player"):
            response.headers["Cache-Control"] = "public, max-age=300"
        return response


app.add_middleware(CacheControlMiddleware)


@app.get("/api/health")
async def health_check():
    """Lightweight health check — keeps Render from sleeping."""
    return {"status": "ok"}


@app.get("/api/status")
async def get_status():
    """Return DB status - how much historical data is loaded."""
    return {
        "matches": get_total_match_count(),
        "tries": get_total_try_count(),
    }


@app.get("/api/img")
async def proxy_image(url: str):
    """Proxy external images to avoid hotlink blocking."""
    if not url.startswith("https://"):
        raise HTTPException(status_code=400, detail="Invalid URL")
    try:
        async with httpx.AsyncClient(
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "image/webp,image/apng,image/*,*/*;q=0.8",
            },
            timeout=10.0, follow_redirects=True
        ) as client:
            resp = await client.get(url)
            if resp.status_code != 200:
                raise HTTPException(status_code=404)
            content_type = resp.headers.get("content-type", "image/png")
            return Response(
                content=resp.content,
                media_type=content_type,
                headers={"Cache-Control": "public, max-age=86400"},
            )
    except (httpx.HTTPError, Exception):
        raise HTTPException(status_code=502, detail="Failed to fetch image")



@app.get("/api/rounds")
async def get_rounds():
    return {
        str(i): {"name": f"Round {i}", "round_number": i}
        for i in range(1, TOTAL_ROUNDS + 1)
    }


def _predict_single_fixture(f, model_version, round_number):
    """Predict win probability for a single fixture (runs in its own thread)."""
    home = f.get("home_team", "")
    away = f.get("away_team", "")
    if not home or not away:
        return f
    wp = predict_win_probability(home, away, {}, {}, model_version=model_version,
                                 before_season=SEASON, before_round=round_number,
                                 venue=f.get("venue", ""))
    f["predicted_winner"] = wp["predicted_winner"]
    f["home_win_prob"] = wp["home_win_prob"]
    f["away_win_prob"] = wp["away_win_prob"]
    f["predicted_home_score"] = wp["predicted_home_score"]
    f["predicted_away_score"] = wp["predicted_away_score"]
    home_odds_str = f.get("home_odds", "")
    away_odds_str = f.get("away_odds", "")
    if home_odds_str and away_odds_str:
        try:
            home_dec = float(home_odds_str)
            away_dec = float(away_odds_str)
            if home_dec > 0 and away_dec > 0:
                home_implied = 1.0 / home_dec
                away_implied = 1.0 / away_dec
                f["odds_comparison"] = {
                    "home_decimal": home_dec, "away_decimal": away_dec,
                    "home_implied_prob": round(home_implied, 4),
                    "away_implied_prob": round(away_implied, 4),
                    "home_model_prob": round(wp["home_win_prob"], 4),
                    "away_model_prob": round(wp["away_win_prob"], 4),
                    "home_value": wp["home_win_prob"] > home_implied,
                    "away_value": wp["away_win_prob"] > away_implied,
                    "home_edge": round(wp["home_win_prob"] - home_implied, 4),
                    "away_edge": round(wp["away_win_prob"] - away_implied, 4),
                }
        except (ValueError, ZeroDivisionError):
            pass
    state = (f.get("match_state") or "").lower()
    if state in ("fulltime", "postmatch") and f.get("home_score") is not None and f.get("away_score") is not None:
        if f["home_score"] > f["away_score"]:
            f["actual_winner"] = home
        elif f["away_score"] > f["home_score"]:
            f["actual_winner"] = away
        else:
            f["actual_winner"] = "Draw"
        f["prediction_correct"] = f["predicted_winner"] == f["actual_winner"]
    return f


def _enrich_fixtures(fixtures, model_version, round_number):
    """Add win predictions to all fixtures. Pre-fetches all team data in ONE query."""
    # Collect all teams and matchups (with venues for prefetch)
    team_names = set()
    matchups = []
    for f in fixtures:
        home = f.get("home_team", "")
        away = f.get("away_team", "")
        venue = f.get("venue", "")
        if home and away:
            team_names.add(home)
            team_names.add(away)
            matchups.append((home, away, venue))

    # Pre-fetch all team data in a single bulk query (1 query instead of ~40)
    if team_names:
        prefetch_round_data(
            list(team_names), matchups, last_n_games=10,
            before_season=SEASON, before_round=round_number,
        )

    # Now run predictions — all DB calls will hit the cache
    for f in fixtures:
        _predict_single_fixture(f, model_version, round_number)
    return fixtures


@app.get("/api/rounds/{round_number}")
async def get_round(round_number: int, version: int = 3):

    model_version = max(1, min(version, 3))
    if round_number < 1 or round_number > TOTAL_ROUNDS:
        raise HTTPException(status_code=404, detail="Invalid round number")

    raw = await fetch_round(round_number)
    if raw is None:
        raise HTTPException(status_code=502, detail="Could not fetch round data from NRL")

    fixtures, byes = parse_fixtures(raw)
    fixtures = await asyncio.to_thread(_enrich_fixtures, fixtures, model_version, round_number)

    return {
        "round": round_number,
        "name": f"Round {round_number}",
        "matches": fixtures,
        "byes": byes,
    }


@app.get("/api/player")
async def get_player(name: str):
    """Get a player's full game history."""
    if not name:
        raise HTTPException(status_code=400, detail="Player name is required")
    games = get_player_game_log(name)
    if not games:
        raise HTTPException(status_code=404, detail="No history found for this player")

    total_tries = sum(g["try_count"] for g in games)
    total_games = len(games)
    wins = sum(1 for g in games if g["won"])
    teams = list(dict.fromkeys(g["team"] for g in games))
    positions = list(dict.fromkeys(g["position"] for g in games if g["position"]))

    # Group by season
    seasons = {}
    for g in games:
        s = g["season"]
        if s not in seasons:
            seasons[s] = {"games": 0, "tries": 0}
        seasons[s]["games"] += 1
        seasons[s]["tries"] += g["try_count"]

    return {
        "name": name,
        "teams": teams,
        "positions": positions,
        "total_games": total_games,
        "total_tries": total_tries,
        "try_rate": round(total_tries / total_games, 3) if total_games > 0 else 0,
        "wins": wins,
        "win_rate": round(wins / total_games, 3) if total_games > 0 else 0,
        "seasons_summary": seasons,
        "games": games,
    }


def _compute_match_detail(url, raw, home_players, away_players,
                          model_version, bookmaker_data, is_completed, match_state):
    """Heavy sync computation for match detail — runs in a thread."""
    import json

    season_match = re.search(r'/(\d{4})/', url)
    round_match = re.search(r'/round-(\d+)/', url)
    before_season = int(season_match.group(1)) if season_match else None
    before_round = int(round_match.group(1)) if round_match else None

    stats = parse_team_stats(raw)
    home_team = raw.get("homeTeam", {})
    away_team = raw.get("awayTeam", {})
    home_nickname = home_team.get("nickName", "Home")
    away_nickname = away_team.get("nickName", "Away")
    match_venue = raw.get("venue", "")
    match_weather = raw.get("weather", "")
    match_ground = raw.get("groundConditions", "")

    # Pre-fetch all team data in one bulk query (venue stats, attack/defence, h2h)
    prefetch_round_data(
        [home_nickname, away_nickname],
        [(home_nickname, away_nickname, match_venue)],
        last_n_games=10,
        before_season=before_season,
        before_round=before_round,
    )

    predictions = generate_predictions(
        home_players, away_players,
        stats.get("home", {}), stats.get("away", {}),
        home_team_name=home_nickname,
        away_team_name=away_nickname,
        model_version=model_version,
        before_season=before_season,
        before_round=before_round,
        weather=match_weather,
        ground_conditions=match_ground,
    )

    add_implied_odds_to_players(predictions["home"])
    add_implied_odds_to_players(predictions["away"])

    if bookmaker_data:
        for side, team_name in [("home", home_nickname), ("away", away_nickname)]:
            opp_name = away_nickname if side == "home" else home_nickname
            for p in predictions[side]:
                bk_list = lookup_bookmaker_odds(bookmaker_data, team_name, opp_name, p["name"])
                if bk_list:
                    p["bookmaker_odds"] = bk_list

    win_prediction = predict_win_probability(
        home_nickname, away_nickname,
        stats.get("home", {}), stats.get("away", {}),
        model_version=model_version,
        before_season=before_season,
        before_round=before_round,
        venue=match_venue,
        weather=match_weather,
        ground_conditions=match_ground,
    )

    multi = generate_multi_suggestion(
        predictions["home"], predictions["away"],
        home_nickname, away_nickname,
    )

    home_summary = generate_team_summary(home_nickname, model_version,
                                          before_season=before_season, before_round=before_round)
    away_summary = generate_team_summary(away_nickname, model_version,
                                          before_season=before_season, before_round=before_round)

    value_picks_home = find_value_picks(predictions["home"], away_nickname, home_nickname,
                                         before_season=before_season, before_round=before_round)
    value_picks_away = find_value_picks(predictions["away"], home_nickname, away_nickname,
                                         before_season=before_season, before_round=before_round)

    top3_home = [
        {"name": p["name"], "number": p["number"], "position": p["position"], "try_percentage": p["try_percentage"]}
        for p in predictions["home"][:3]
    ]
    top3_away = [
        {"name": p["name"], "number": p["number"], "position": p["position"], "try_percentage": p["try_percentage"]}
        for p in predictions["away"][:3]
    ]

    scoring = parse_scoring(raw) if is_completed else None

    if scoring:
        home_actual = set(scoring["home_try_scorers"])
        away_actual = set(scoring["away_try_scorers"])
        for pick in top3_home:
            pick["scored"] = pick["name"] in home_actual
        for pick in top3_away:
            pick["scored"] = pick["name"] in away_actual
        all_actual = home_actual | away_actual
        multi_hits = sum(1 for p in multi["picks"] if p["name"] in all_actual)
        multi["hits"] = multi_hits
        multi["all_scored"] = multi_hits == len(multi["picks"])
        for p in multi["picks"]:
            p["scored"] = p["name"] in all_actual
        for vp in value_picks_home:
            vp["scored"] = vp["name"] in home_actual
        for vp in value_picks_away:
            vp["scored"] = vp["name"] in away_actual
        if scoring["home_score"] is not None and scoring["away_score"] is not None:
            actual_winner = home_nickname if scoring["home_score"] > scoring["away_score"] else away_nickname
            if scoring["home_score"] == scoring["away_score"]:
                actual_winner = "Draw"
            win_prediction["actual_winner"] = actual_winner
            win_prediction["correct"] = win_prediction["predicted_winner"] == actual_winner

    if is_completed and scoring and before_season and before_round:
        try:
            t3h_json = json.dumps([{"name": p["name"], "scored": p.get("scored")} for p in top3_home])
            t3a_json = json.dumps([{"name": p["name"], "scored": p.get("scored")} for p in top3_away])
            t3_hits = sum(1 for p in top3_home if p.get("scored")) + sum(1 for p in top3_away if p.get("scored"))
            m_json = json.dumps([{"name": p["name"], "team": p["team"], "scored": p.get("scored")} for p in multi["picks"]])
            upsert_prediction(
                match_url=url, season=before_season, round_number=before_round,
                model_version=model_version, home_team=home_nickname, away_team=away_nickname,
                predicted_winner=win_prediction["predicted_winner"],
                home_win_prob=win_prediction["home_win_prob"],
                predicted_home_score=win_prediction["predicted_home_score"],
                predicted_away_score=win_prediction["predicted_away_score"],
                actual_winner=win_prediction.get("actual_winner"),
                actual_home_score=scoring.get("home_score"),
                actual_away_score=scoring.get("away_score"),
                win_correct=1 if win_prediction.get("correct") else 0,
                top3_home_json=t3h_json, top3_away_json=t3a_json, top3_hits=t3_hits,
                multi_json=m_json, multi_hits=multi.get("hits", 0),
                multi_all_scored=1 if multi.get("all_scored") else 0,
            )
        except Exception as e:
            logger.warning(f"Failed to record prediction: {e}")

    home_theme = home_team.get("theme", {})
    away_theme = away_team.get("theme", {})

    return {
        "match_url": url,
        "match_state": match_state,
        "is_completed": is_completed,
        "home_team": home_team.get("name", home_nickname),
        "away_team": away_team.get("name", away_nickname),
        "home_nickname": home_nickname,
        "away_nickname": away_nickname,
        "home_colour": _theme_to_colour(home_theme),
        "away_colour": _theme_to_colour(away_theme),
        "home_theme_key": home_theme.get("key", "") if isinstance(home_theme, dict) else "",
        "away_theme_key": away_theme.get("key", "") if isinstance(away_theme, dict) else "",
        "home_position": home_team.get("teamPosition", ""),
        "away_position": away_team.get("teamPosition", ""),
        "home_odds": home_team.get("odds", ""),
        "away_odds": away_team.get("odds", ""),
        "odds_comparison": _build_odds_comparison(
            home_team.get("odds", ""), away_team.get("odds", ""),
            win_prediction["home_win_prob"], win_prediction["away_win_prob"],
        ),
        "venue": raw.get("venue", ""),
        "venue_city": raw.get("venueCity", ""),
        "kickoff": raw.get("startTime", ""),
        "weather": raw.get("weather", ""),
        "ground_conditions": raw.get("groundConditions", ""),
        "home_stats": stats.get("home", {}),
        "away_stats": stats.get("away", {}),
        "predictions": predictions,
        "top3_home": top3_home,
        "top3_away": top3_away,
        "scoring": scoring,
        "win_prediction": win_prediction,
        "multi": multi,
        "value_picks_home": value_picks_home,
        "value_picks_away": value_picks_away,
        "home_summary": home_summary,
        "away_summary": away_summary,
        "model_version": model_version,
        "db_status": {
            "matches": get_total_match_count(),
            "tries": get_total_try_count(),
        },
    }


@app.get("/api/match")
async def get_match_by_url(url: str, version: int = 3):

    if not url.startswith("/draw/"):
        raise HTTPException(status_code=400, detail="Invalid match URL path")

    model_version = max(1, min(version, 3))

    raw = await fetch_match_detail(url)
    if raw is None:
        raise HTTPException(status_code=502, detail="Could not fetch match data from NRL")

    home_players = parse_team_list(raw, "homeTeam")
    away_players = parse_team_list(raw, "awayTeam")

    if not home_players and not away_players:
        raise HTTPException(
            status_code=403,
            detail="Team lists have not been announced for this match yet"
        )

    # Fetch bookmaker odds (async) before running sync computation
    match_state = raw.get("matchState", "")
    is_completed = match_state in ("FullTime", "PostMatch")
    bookmaker_data = {}
    if has_odds_api_key() and not is_completed:
        try:
            bookmaker_data = await fetch_bookmaker_odds()
        except Exception as e:
            logger.warning(f"Failed to fetch bookmaker odds: {e}")

    # Run all heavy DB/model computation in a thread
    result = await asyncio.to_thread(
        _compute_match_detail, url, raw, home_players, away_players,
        model_version, bookmaker_data, is_completed, match_state
    )
    return result


def _build_odds_comparison(home_odds_str, away_odds_str, home_model_prob, away_model_prob):
    """Calculate implied probabilities from decimal odds and compare to model."""
    if not home_odds_str or not away_odds_str:
        return None
    try:
        home_dec = float(home_odds_str)
        away_dec = float(away_odds_str)
        if home_dec <= 0 or away_dec <= 0:
            return None
        home_implied = 1.0 / home_dec
        away_implied = 1.0 / away_dec
        return {
            "home_decimal": home_dec,
            "away_decimal": away_dec,
            "home_implied_prob": round(home_implied, 4),
            "away_implied_prob": round(away_implied, 4),
            "home_model_prob": round(home_model_prob, 4),
            "away_model_prob": round(away_model_prob, 4),
            "home_value": home_model_prob > home_implied,
            "away_value": away_model_prob > away_implied,
            "home_edge": round(home_model_prob - home_implied, 4),
            "away_edge": round(away_model_prob - away_implied, 4),
        }
    except (ValueError, ZeroDivisionError):
        return None


def _theme_to_colour(theme: dict) -> str:
    if not isinstance(theme, dict):
        return "#333333"
    key = theme.get("key", "")
    colour_map = {
        "broncos": "#6D2735", "raiders": "#56B947", "bulldogs": "#005DB5",
        "sharks": "#00A5DB", "titans": "#E8B825", "sea-eagles": "#6D2735",
        "storm": "#552D6D", "knights": "#005DB5", "cowboys": "#002B5C",
        "eels": "#005DB5", "panthers": "#2A2A2A", "rabbitohs": "#003B2F",
        "dragons": "#E2231A", "roosters": "#003B7B", "warriors": "#636466",
        "wests-tigers": "#F47920", "dolphins": "#C8102E",
    }
    return colour_map.get(key, "#333333")


@app.get("/api/accuracy")
async def get_accuracy(model_version: int = None, season: int = None):
    """Get prediction accuracy stats."""
    return get_accuracy_stats(model_version=model_version, season=season)


@app.get("/api/search")
async def search(q: str, limit: int = 15):
    """Search for players and teams."""
    if not q or len(q) < 2:
        return {"players": [], "teams": []}
    players = search_players(q, limit=limit)
    teams = search_teams(q)
    return {"players": players, "teams": teams}


@app.get("/api/team")
async def get_team(name: str, season: int = SEASON):
    """Get comprehensive team stats."""
    if not name:
        raise HTTPException(status_code=400, detail="Team name is required")

    # Check team exists
    all_teams = get_all_teams()
    if name not in all_teams:
        raise HTTPException(status_code=404, detail="Team not found")

    from model import generate_team_summary as _gen_summary

    form = get_team_attack_defence(name, last_n_games=10)
    ha = get_home_away_win_rate(name)
    edge_vuln = get_team_tries_conceded_by_edge(name, last_n_games=15)
    roster = get_team_roster(name, season=season)
    recent = get_team_recent_results(name, last_n=10)
    summary = _gen_summary(name, model_version=3)

    # Form string (W/L/D for last 10)
    form_str = [r["result"] for r in recent]

    # Find theme key from colour map
    theme_map = {
        "Broncos": "broncos", "Raiders": "raiders", "Bulldogs": "bulldogs",
        "Sharks": "sharks", "Titans": "titans", "Sea Eagles": "sea-eagles",
        "Storm": "storm", "Knights": "knights", "Cowboys": "cowboys",
        "Eels": "eels", "Panthers": "panthers", "Rabbitohs": "rabbitohs",
        "Dragons": "dragons", "Roosters": "roosters", "Warriors": "warriors",
        "Wests Tigers": "wests-tigers", "Dolphins": "dolphins",
    }
    theme_key = theme_map.get(name, "nrl")

    return {
        "name": name,
        "theme_key": theme_key,
        "colour": _theme_to_colour({"key": theme_key}),
        "season": season,
        "stats": {
            "avg_scored": round(form["avg_scored"], 1),
            "avg_conceded": round(form["avg_conceded"], 1),
            "wins": form["wins"],
            "played": form["played"],
            "home_win_rate": round(ha["home_win_rate"], 3),
            "away_win_rate": round(ha["away_win_rate"], 3),
            "home_played": ha["home_played"],
            "away_played": ha["away_played"],
        },
        "edge_vulnerability": {k: dict(v) for k, v in edge_vuln.items()} if edge_vuln else {},
        "summary": summary,
        "form": form_str,
        "recent_results": recent,
        "roster": roster,
    }


@app.get("/api/teams")
async def list_teams():
    """List all teams."""
    return get_all_teams()


# Serve frontend
static_dir = os.path.join(os.path.dirname(__file__), "static")
if os.path.isdir(static_dir):
    app.mount("/assets", StaticFiles(directory=os.path.join(static_dir, "assets")), name="assets")

    @app.get("/{full_path:path}")
    async def serve_frontend(full_path: str):
        file_path = os.path.join(static_dir, full_path)
        if os.path.isfile(file_path):
            return FileResponse(file_path)
        return FileResponse(os.path.join(static_dir, "index.html"))
