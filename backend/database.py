"""
PostgreSQL (Supabase) database for storing historical NRL match and try-scoring data.
Uses psycopg2 with connection pooling and in-memory TTL caching to reduce
round-trip latency to remote databases.
"""

import os
import logging
import atexit
import time
import threading

import decimal

import psycopg2
import psycopg2.pool
import psycopg2.extras
import psycopg2.extensions

logger = logging.getLogger(__name__)

# --- In-memory TTL cache to reduce round-trips to remote Supabase ---
_query_cache = {}
_cache_lock = threading.Lock()
CACHE_TTL = 1800  # 30 minutes — data only changes via cron job


def _cache_key(func_name, *args, **kwargs):
    """Build a hashable cache key from function name and arguments."""
    kw_tuple = tuple(sorted(kwargs.items()))
    return (func_name, args, kw_tuple)


def _cached_query(func_name, ttl=CACHE_TTL):
    """Decorator that caches DB query results for `ttl` seconds."""
    def decorator(func):
        def wrapper(*args, **kwargs):
            key = _cache_key(func_name, *args, **kwargs)
            now = time.monotonic()
            with _cache_lock:
                if key in _query_cache:
                    val, ts = _query_cache[key]
                    if now - ts < ttl:
                        return val
            result = func(*args, **kwargs)
            with _cache_lock:
                _query_cache[key] = (result, now)
            return result
        wrapper.__name__ = func.__name__
        return wrapper
    return decorator


def clear_query_cache():
    """Clear all cached query results (call after scraping)."""
    global _query_cache
    with _cache_lock:
        _query_cache.clear()


# Make psycopg2 return float instead of Decimal for numeric/real columns
DEC2FLOAT = psycopg2.extensions.new_type(
    psycopg2.extensions.DECIMAL.values,
    'DEC2FLOAT',
    lambda value, curs: float(value) if value is not None else None
)
psycopg2.extensions.register_type(DEC2FLOAT)

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/nrl"
)

# Connection pool (min 1, max 10 connections)
_pool = None


def _get_pool():
    """Lazily initialise and return the connection pool."""
    global _pool
    if _pool is None or _pool.closed:
        # Force sslmode and use options to prefer IPv4
        dsn = DATABASE_URL
        if "sslmode" not in dsn:
            sep = "&" if "?" in dsn else "?"
            dsn += f"{sep}sslmode=require"
        _pool = psycopg2.pool.ThreadedConnectionPool(
            2, 20, dsn,
            options="-c statement_timeout=30000",
        )
        atexit.register(_close_pool)
    return _pool


def _close_pool():
    global _pool
    if _pool is not None and not _pool.closed:
        _pool.closeall()
        _pool = None


SCHEMA = "nrltp"


class PooledConnection:
    """Wrapper around a psycopg2 connection that returns it to the pool on close()
    and exposes an execute/fetchone/fetchall interface compatible with the old code."""

    def __init__(self, conn, pool):
        self._conn = conn
        self._pool = pool
        # Always use RealDictCursor so rows behave like dicts
        self._conn.autocommit = False
        # Set search path so all queries use the nrltp schema
        cur = self._conn.cursor()
        cur.execute(f"SET search_path TO {SCHEMA}, public")
        cur.close()

    def execute(self, query, params=None):
        cur = self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(query, params)
        return cur

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    def close(self):
        """Return connection to the pool instead of truly closing it."""
        try:
            self._conn.rollback()  # discard any uncommitted work
        except Exception:
            pass
        self._pool.putconn(self._conn)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self.commit()
        else:
            self.rollback()
        self.close()
        return False


def get_db():
    """Get a database connection from the pool."""
    pool = _get_pool()
    conn = pool.getconn()
    return PooledConnection(conn, pool)


def init_db():
    """Create tables if they don't exist, and run migrations."""
    conn = get_db()

    # Create schema
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
    conn.commit()

    # Core tables
    conn.execute("""
        CREATE TABLE IF NOT EXISTS matches (
            id SERIAL PRIMARY KEY,
            season INTEGER NOT NULL,
            round_number INTEGER NOT NULL,
            round_title TEXT,
            match_url TEXT UNIQUE NOT NULL,
            match_state TEXT,
            home_team TEXT NOT NULL,
            away_team TEXT NOT NULL,
            home_score INTEGER,
            away_score INTEGER,
            venue TEXT,
            kickoff TEXT,
            venue_city TEXT DEFAULT '',
            weather TEXT DEFAULT '',
            ground_conditions TEXT DEFAULT '',
            UNIQUE(season, round_number, home_team, away_team)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS players (
            id SERIAL PRIMARY KEY,
            match_id INTEGER NOT NULL,
            team TEXT NOT NULL,
            side TEXT NOT NULL,
            name TEXT NOT NULL,
            jersey_number INTEGER,
            position TEXT,
            is_interchange INTEGER DEFAULT 0,
            FOREIGN KEY (match_id) REFERENCES matches(id)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS tries (
            id SERIAL PRIMARY KEY,
            match_id INTEGER NOT NULL,
            team TEXT NOT NULL,
            side TEXT NOT NULL,
            player_name TEXT NOT NULL,
            minute TEXT,
            field_side TEXT DEFAULT '',
            FOREIGN KEY (match_id) REFERENCES matches(id)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS scrape_progress (
            season INTEGER NOT NULL,
            round_number INTEGER NOT NULL,
            completed INTEGER DEFAULT 0,
            PRIMARY KEY (season, round_number)
        )
    """)

    conn.execute("CREATE INDEX IF NOT EXISTS idx_players_name ON players(name)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_players_position ON players(position)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_players_match ON players(match_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_tries_match ON tries(match_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_tries_player ON tries(player_name)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_matches_season ON matches(season)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_matches_teams ON matches(home_team, away_team)")
    # Composite indexes for the hot-path queries
    conn.execute("CREATE INDEX IF NOT EXISTS idx_matches_home_state ON matches(home_team, match_state, season DESC, round_number DESC)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_matches_away_state ON matches(away_team, match_state, season DESC, round_number DESC)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_matches_state_season ON matches(match_state, season DESC, round_number DESC)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_players_name_match ON players(name, match_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_tries_match_player_side ON tries(match_id, player_name, side)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_matches_venue_state ON matches(venue, match_state)")

    conn.commit()

    # Migration: add field_side column to tries (for DBs created before it was in CREATE TABLE)
    try:
        conn.execute("ALTER TABLE tries ADD COLUMN field_side TEXT DEFAULT ''")
        conn.commit()
        logger.info("Migration: added field_side column to tries table")
    except psycopg2.errors.DuplicateColumn:
        conn.rollback()

    # Index that depends on field_side column — must come after migration
    conn.execute("CREATE INDEX IF NOT EXISTS idx_tries_match_side_field ON tries(match_id, side, field_side)")
    conn.commit()

    # Interchanges table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS interchanges (
            id SERIAL PRIMARY KEY,
            match_id INTEGER NOT NULL,
            side TEXT NOT NULL,
            player_on TEXT NOT NULL,
            player_off TEXT NOT NULL,
            jersey_on INTEGER,
            jersey_off INTEGER,
            game_seconds INTEGER,
            FOREIGN KEY (match_id) REFERENCES matches(id)
        )
    """)

    conn.execute("CREATE INDEX IF NOT EXISTS idx_tries_field_side ON tries(field_side)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_interchanges_match ON interchanges(match_id)")

    conn.commit()

    # Predictions tracking table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS predictions (
            id SERIAL PRIMARY KEY,
            match_url TEXT NOT NULL,
            season INTEGER NOT NULL,
            round_number INTEGER NOT NULL,
            model_version INTEGER NOT NULL,
            home_team TEXT NOT NULL,
            away_team TEXT NOT NULL,
            predicted_winner TEXT,
            home_win_prob REAL,
            predicted_home_score INTEGER,
            predicted_away_score INTEGER,
            actual_winner TEXT,
            actual_home_score INTEGER,
            actual_away_score INTEGER,
            win_correct INTEGER,
            top3_home_json TEXT,
            top3_away_json TEXT,
            top3_hits INTEGER,
            multi_json TEXT,
            multi_hits INTEGER,
            multi_all_scored INTEGER,
            recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(match_url, model_version)
        )
    """)

    conn.execute("CREATE INDEX IF NOT EXISTS idx_predictions_season ON predictions(season, round_number)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_predictions_version ON predictions(model_version)")

    conn.commit()

    # Venue/weather columns on matches table
    for col, default in [("venue_city", "''"), ("weather", "''"), ("ground_conditions", "''")]:
        try:
            conn.execute(f"ALTER TABLE matches ADD COLUMN {col} TEXT DEFAULT {default}")
            conn.commit()
            logger.info(f"Migration: added {col} column to matches table")
        except psycopg2.errors.DuplicateColumn:
            conn.rollback()

    conn.close()
    logger.info("Database initialised")


def is_round_scraped(season: int, round_number: int) -> bool:
    conn = get_db()
    row = conn.execute(
        "SELECT completed FROM scrape_progress WHERE season=%s AND round_number=%s",
        (season, round_number)
    ).fetchone()
    conn.close()
    return bool(row and row["completed"])


def reset_scrape_progress():
    """Clear all scrape progress to force a full re-scrape."""
    conn = get_db()
    conn.execute("DELETE FROM scrape_progress")
    conn.commit()
    conn.close()
    logger.info("Scrape progress reset — full re-scrape will occur on next startup")


def mark_round_scraped(season: int, round_number: int):
    conn = get_db()
    conn.execute(
        """INSERT INTO scrape_progress (season, round_number, completed) VALUES (%s, %s, 1)
           ON CONFLICT (season, round_number) DO UPDATE SET completed = 1""",
        (season, round_number)
    )
    conn.commit()
    conn.close()


def insert_match(season, round_number, round_title, match_url, match_state,
                 home_team, away_team, home_score, away_score, venue, kickoff,
                 venue_city="") -> int:
    conn = get_db()
    try:
        cur = conn.execute(
            """INSERT INTO matches
               (season, round_number, round_title, match_url, match_state,
                home_team, away_team, home_score, away_score, venue, kickoff, venue_city)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
               ON CONFLICT DO NOTHING
               RETURNING id""",
            (season, round_number, round_title, match_url, match_state,
             home_team, away_team, home_score, away_score, venue, kickoff, venue_city)
        )
        row = cur.fetchone()
        conn.commit()
        if row:
            return row["id"]
        # If ignored (duplicate), fetch existing ID
        row = conn.execute(
            "SELECT id FROM matches WHERE match_url=%s", (match_url,)
        ).fetchone()
        return row["id"] if row else 0
    finally:
        conn.close()


def insert_player(match_id, team, side, name, jersey_number, position, is_interchange):
    conn = get_db()
    conn.execute(
        """INSERT INTO players (match_id, team, side, name, jersey_number, position, is_interchange)
           VALUES (%s, %s, %s, %s, %s, %s, %s)""",
        (match_id, team, side, name, jersey_number, position, int(is_interchange))
    )
    conn.commit()
    conn.close()


def insert_try(match_id, team, side, player_name, minute):
    conn = get_db()
    conn.execute(
        """INSERT INTO tries (match_id, team, side, player_name, minute)
           VALUES (%s, %s, %s, %s, %s)""",
        (match_id, team, side, player_name, minute)
    )
    conn.commit()
    conn.close()


def bulk_insert_match_data(match_id, players_data, tries_data, interchanges_data=None):
    """Insert all players, tries, and interchanges for a match in one transaction.
    Clears existing data first to avoid duplicates on re-scrape."""
    conn = get_db()
    try:
        # Clear any existing data for this match (safe for re-scrapes)
        conn.execute("DELETE FROM players WHERE match_id=%s", (match_id,))
        conn.execute("DELETE FROM tries WHERE match_id=%s", (match_id,))
        conn.execute("DELETE FROM interchanges WHERE match_id=%s", (match_id,))
        for p in players_data:
            conn.execute(
                """INSERT INTO players (match_id, team, side, name, jersey_number, position, is_interchange)
                   VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                (match_id, p["team"], p["side"], p["name"], p["jersey_number"],
                 p["position"], int(p["is_interchange"]))
            )
        for t in tries_data:
            conn.execute(
                """INSERT INTO tries (match_id, team, side, player_name, minute, field_side)
                   VALUES (%s, %s, %s, %s, %s, %s)""",
                (match_id, t["team"], t["side"], t["player_name"], t["minute"],
                 t.get("field_side", ""))
            )
        if interchanges_data:
            for ic in interchanges_data:
                conn.execute(
                    """INSERT INTO interchanges
                       (match_id, side, player_on, player_off, jersey_on, jersey_off, game_seconds)
                       VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                    (match_id, ic["side"], ic["player_on"], ic["player_off"],
                     ic.get("jersey_on", 0), ic.get("jersey_off", 0), ic.get("game_seconds", 0))
                )
        conn.commit()
    finally:
        conn.close()


# ---- Query functions for the prediction model ----


def _temporal_filter(alias="m", before_season=None, before_round=None):
    """Build a SQL WHERE clause fragment and params to filter matches before a given round."""
    if before_season is None:
        return "", []
    return (
        f" AND ({alias}.season < %s OR ({alias}.season = %s AND {alias}.round_number < %s))",
        [before_season, before_season, before_round or 1],
    )


@_cached_query("player_try_history")
def get_player_try_history(player_name: str, before_season=None, before_round=None) -> list:
    """Get all matches where a player was listed and whether they scored."""
    tf, tp = _temporal_filter("m", before_season, before_round)
    conn = get_db()
    rows = conn.execute(f"""
        SELECT p.name, p.position, p.team, p.side, p.jersey_number,
               m.season, m.round_number, m.home_team, m.away_team,
               m.home_score, m.away_score,
               (SELECT COUNT(*) FROM tries t
                WHERE t.match_id = m.id AND t.player_name = p.name AND t.side = p.side) as tries_scored
        FROM players p
        JOIN matches m ON p.match_id = m.id
        WHERE p.name = %s AND m.match_state = 'FullTime'{tf}
        ORDER BY m.season, m.round_number
    """, (player_name, *tp)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_players_try_histories_batch(player_names: list, before_season=None, before_round=None) -> dict:
    """Batch fetch try histories for multiple players in a single query.
    Returns: {player_name: [history_rows]}"""
    if not player_names:
        return {}

    # Check cache first — return cached ones, query only uncached
    uncached = []
    result = {}
    now = time.monotonic()
    for name in player_names:
        key = _cache_key("player_try_history", name, before_season, before_round)
        with _cache_lock:
            if key in _query_cache:
                val, ts = _query_cache[key]
                if now - ts < CACHE_TTL:
                    result[name] = val
                    continue
        uncached.append(name)

    if not uncached:
        return result

    tf, tp = _temporal_filter("m", before_season, before_round)
    placeholders = ", ".join(["%s"] * len(uncached))
    conn = get_db()
    rows = conn.execute(f"""
        SELECT p.name, p.position, p.team, p.side, p.jersey_number,
               m.season, m.round_number, m.home_team, m.away_team,
               m.home_score, m.away_score,
               (SELECT COUNT(*) FROM tries t
                WHERE t.match_id = m.id AND t.player_name = p.name AND t.side = p.side) as tries_scored
        FROM players p
        JOIN matches m ON p.match_id = m.id
        WHERE p.name IN ({placeholders}) AND m.match_state = 'FullTime'{tf}
        ORDER BY p.name, m.season, m.round_number
    """, (*uncached, *tp)).fetchall()
    conn.close()

    # Group by player name
    batch = {}
    for r in rows:
        name = r["name"]
        if name not in batch:
            batch[name] = []
        batch[name].append(dict(r))

    # Cache individual results
    now = time.monotonic()
    with _cache_lock:
        for name in uncached:
            hist = batch.get(name, [])
            key = _cache_key("player_try_history", name, before_season, before_round)
            _query_cache[key] = (hist, now)
            result[name] = hist

    return result


def get_players_recent_form_batch(player_names: list, last_n_games: int = 5,
                                   before_season=None, before_round=None) -> dict:
    """Batch fetch recent form for multiple players in a single query.
    Returns: {player_name: {games, tries, rate, streak}}"""
    if not player_names:
        return {}

    # Check cache first
    uncached = []
    result = {}
    now = time.monotonic()
    for name in player_names:
        key = _cache_key("player_recent_form", name, last_n_games, before_season, before_round)
        with _cache_lock:
            if key in _query_cache:
                val, ts = _query_cache[key]
                if now - ts < CACHE_TTL:
                    result[name] = val
                    continue
        uncached.append(name)

    if not uncached:
        return result

    tf, tp = _temporal_filter("m", before_season, before_round)
    placeholders = ", ".join(["%s"] * len(uncached))
    conn = get_db()
    # Use a window function to rank games per player and limit to last N
    rows = conn.execute(f"""
        SELECT sub.name, sub.tries_scored FROM (
            SELECT p.name,
                   (SELECT COUNT(*) FROM tries t
                    WHERE t.match_id = m.id AND t.player_name = p.name AND t.side = p.side) as tries_scored,
                   ROW_NUMBER() OVER (PARTITION BY p.name ORDER BY m.season DESC, m.round_number DESC) as rn
            FROM players p
            JOIN matches m ON p.match_id = m.id
            WHERE p.name IN ({placeholders}) AND m.match_state = 'FullTime'{tf}
        ) sub WHERE sub.rn <= %s
        ORDER BY sub.name, sub.rn
    """, (*uncached, *tp, last_n_games)).fetchall()
    conn.close()

    # Group by player
    batch = {}
    for r in rows:
        name = r["name"]
        if name not in batch:
            batch[name] = []
        batch[name].append(r["tries_scored"])

    # Compute form stats and cache
    default = {"games": 0, "tries": 0, "rate": 0, "streak": 0}
    now = time.monotonic()
    with _cache_lock:
        for name in uncached:
            scores = batch.get(name, [])
            if not scores:
                form = default.copy()
            else:
                games = len(scores)
                tries = sum(scores)
                streak = 0
                for s in scores:
                    if s > 0:
                        streak += 1
                    else:
                        break
                form = {
                    "games": games,
                    "tries": tries,
                    "rate": round(tries / games, 3) if games > 0 else 0,
                    "streak": streak,
                }
            key = _cache_key("player_recent_form", name, last_n_games, before_season, before_round)
            _query_cache[key] = (form, now)
            result[name] = form

    return result


@_cached_query("position_try_rates")
def get_position_try_rates() -> dict:
    """
    Calculate historical try-scoring rate by position.
    Returns: {position: {appearances: N, total_tries: N, rate: float}}
    """
    conn = get_db()
    rows = conn.execute("""
        SELECT p.position,
               COUNT(*) as appearances,
               COALESCE(SUM(
                   (SELECT COUNT(*) FROM tries t
                    WHERE t.match_id = p.match_id AND t.player_name = p.name AND t.side = p.side)
               ), 0) as total_tries
        FROM players p
        JOIN matches m ON p.match_id = m.id
        WHERE m.match_state = 'FullTime'
          AND p.position IS NOT NULL
          AND p.position != ''
        GROUP BY p.position
    """).fetchall()
    conn.close()

    result = {}
    for r in rows:
        pos = r["position"]
        apps = r["appearances"]
        tries = r["total_tries"]
        result[pos] = {
            "appearances": apps,
            "total_tries": tries,
            "rate": tries / apps if apps > 0 else 0,
        }
    return result


def prefetch_round_data(team_names: list, matchups: list, last_n_games: int = 10,
                        before_season=None, before_round=None):
    """Pre-fetch all team stats needed for a round in bulk queries.
    Populates individual caches so subsequent calls are instant.
    team_names: list of team names, matchups: list of (home, away) tuples."""
    tf, tp = _temporal_filter("m", before_season, before_round)
    conn = get_db()

    # 1. Batch fetch recent matches — limit to ~200 rows (enough for 16 teams × 10 games each)
    placeholders = ", ".join(["%s"] * len(team_names))
    all_matches = conn.execute(f"""
        SELECT home_team, away_team, home_score, away_score, season, round_number, venue
        FROM matches m
        WHERE (home_team IN ({placeholders}) OR away_team IN ({placeholders}))
          AND match_state = 'FullTime'
          AND home_score IS NOT NULL{tf}
        ORDER BY season DESC, round_number DESC
        LIMIT 200
    """, (*team_names, *team_names, *tp)).fetchall()

    # 2. Fetch venue stats for all venues in the matchups (separate query, no temporal filter)
    # matchups can be (home, away) or (home, away, venue) tuples
    venue_names = list(set(
        m[2] for m in matchups if len(m) > 2 and m[2]
    ) | set(
        r["venue"] for r in all_matches if r.get("venue")
    ))
    venue_matches = {}
    if venue_names:
        vn_placeholders = ", ".join(["%s"] * len(venue_names))
        venue_rows = conn.execute(f"""
            SELECT venue, home_team, away_team, home_score, away_score
            FROM matches
            WHERE venue IN ({vn_placeholders})
              AND match_state = 'FullTime' AND home_score IS NOT NULL
        """, tuple(venue_names)).fetchall()
        for vr in venue_rows:
            v = vr["venue"]
            if v not in venue_matches:
                venue_matches[v] = []
            venue_matches[v].append(vr)

    conn.close()

    now = time.monotonic()

    # Process attack/defence stats per team
    for team in team_names:
        team_rows = [r for r in all_matches if r["home_team"] == team or r["away_team"] == team][:last_n_games]
        stats = _compute_attack_defence(team, team_rows)
        key = _cache_key("team_attack_defence", team, last_n_games, before_season, before_round)
        with _cache_lock:
            _query_cache[key] = (stats, now)

    # Process home/away win rates per team
    for team in team_names:
        team_home = [r for r in all_matches if r["home_team"] == team]
        team_away = [r for r in all_matches if r["away_team"] == team]
        ha_stats = _compute_home_away_rate(team, team_home, team_away)
        key = _cache_key("home_away_win_rate", team, before_season, before_round)
        with _cache_lock:
            _query_cache[key] = (ha_stats, now)

    # Process h2h for each matchup
    for matchup in matchups:
        home, away = matchup[0], matchup[1]
        h2h_rows = [r for r in all_matches
                     if (r["home_team"] == home and r["away_team"] == away) or
                        (r["home_team"] == away and r["away_team"] == home)]
        h2h_stats = _compute_h2h(home, away, h2h_rows)
        key = _cache_key("h2h_record", home, away, before_season, before_round)
        with _cache_lock:
            _query_cache[key] = (h2h_stats, now)

    # Process margin-weighted form per team (V3)
    import math
    for team in team_names:
        team_rows = [r for r in all_matches if r["home_team"] == team or r["away_team"] == team][:last_n_games]
        if team_rows:
            total_quality = 0
            margins = []
            blowout_wins = 0
            close_losses = 0
            for r in team_rows:
                margin = (r["home_score"] - r["away_score"]) if r["home_team"] == team else (r["away_score"] - r["home_score"])
                margins.append(margin)
                total_quality += 1.0 / (1.0 + math.exp(-margin / 8.0))
                if margin >= 18:
                    blowout_wins += 1
                if -6 <= margin < 0:
                    close_losses += 1
            mwf = {
                "quality_score": round(total_quality / len(team_rows), 3),
                "avg_margin": round(sum(margins) / len(margins), 1),
                "blowout_wins": blowout_wins,
                "close_losses": close_losses,
            }
        else:
            mwf = {"quality_score": 0.5, "avg_margin": 0, "blowout_wins": 0, "close_losses": 0}
        key = _cache_key("team_margin_weighted_form", team, last_n_games, before_season, before_round)
        with _cache_lock:
            _query_cache[key] = (mwf, now)

    # Process bye-week detection per team (V3)
    if before_season and before_round and before_round > 1:
        prev_round = before_round - 1
        for team in team_names:
            had_bye = not any(
                (r["home_team"] == team or r["away_team"] == team) and r["round_number"] == prev_round and r["season"] == before_season
                for r in all_matches
            )
            key = _cache_key("team_had_bye", team, before_season, before_round)
            with _cache_lock:
                _query_cache[key] = (had_bye, now)

    # Process venue stats for each matchup
    for matchup in matchups:
        home = matchup[0]
        venue = matchup[2] if len(matchup) > 2 else ""
        if not venue or venue not in venue_matches:
            continue
        vrows = venue_matches[venue]
        # Cache venue stats without team filter
        vstats_all = _compute_venue_stats(vrows, None)
        key_all = _cache_key("venue_stats", venue, None)
        with _cache_lock:
            _query_cache[key_all] = (vstats_all, now)
        # Cache venue stats for the home team
        vstats_home = _compute_venue_stats(vrows, home)
        key_home = _cache_key("venue_stats", venue, home)
        with _cache_lock:
            _query_cache[key_home] = (vstats_home, now)


def _compute_attack_defence(team_name, rows):
    """Compute attack/defence stats from pre-fetched match rows."""
    if not rows:
        return {
            "avg_scored": 22.0, "avg_conceded": 22.0,
            "avg_scored_recent": 22.0, "avg_conceded_recent": 22.0,
            "wins": 0, "wins_recent": 0, "played": 0,
        }
    total_scored = total_conceded = wins = 0
    recent_scored = recent_conceded = recent_wins = 0
    recent_n = min(5, len(rows))
    for i, r in enumerate(rows):
        if r["home_team"] == team_name:
            scored, conceded = r["home_score"], r["away_score"]
            won = r["home_score"] > r["away_score"]
        else:
            scored, conceded = r["away_score"], r["home_score"]
            won = r["away_score"] > r["home_score"]
        total_scored += scored
        total_conceded += conceded
        if won:
            wins += 1
        if i < recent_n:
            recent_scored += scored
            recent_conceded += conceded
            if won:
                recent_wins += 1
    n = len(rows)
    return {
        "avg_scored": total_scored / n, "avg_conceded": total_conceded / n,
        "avg_scored_recent": recent_scored / recent_n if recent_n > 0 else 22.0,
        "avg_conceded_recent": recent_conceded / recent_n if recent_n > 0 else 22.0,
        "wins": wins, "wins_recent": recent_wins, "played": n,
    }


def _compute_home_away_rate(team_name, home_rows, away_rows):
    """Compute home/away win rate from pre-fetched rows."""
    home_played = len(home_rows)
    home_wins = sum(1 for r in home_rows if r["home_score"] > r["away_score"])
    away_played = len(away_rows)
    away_wins = sum(1 for r in away_rows if r["away_score"] > r["home_score"])
    return {
        "home_played": home_played, "home_wins": home_wins,
        "home_win_rate": home_wins / home_played if home_played > 0 else 0.5,
        "away_played": away_played, "away_wins": away_wins,
        "away_win_rate": away_wins / away_played if away_played > 0 else 0.5,
    }


def _compute_h2h(team_a, team_b, rows):
    """Compute H2H record from pre-fetched rows."""
    a_wins = b_wins = 0
    for r in rows:
        if r["home_team"] == team_a:
            if r["home_score"] > r["away_score"]:
                a_wins += 1
            elif r["away_score"] > r["home_score"]:
                b_wins += 1
        else:
            if r["away_score"] > r["home_score"]:
                a_wins += 1
            elif r["home_score"] > r["away_score"]:
                b_wins += 1
    return {"team_a_wins": a_wins, "team_b_wins": b_wins, "played": len(rows)}


def _compute_venue_stats(rows, team_name=None):
    """Compute venue stats from pre-fetched rows. Mirrors get_venue_stats() logic."""
    if team_name:
        # Filter to only matches involving this team (matches original SQL filter)
        rows = [r for r in rows if r["home_team"] == team_name or r["away_team"] == team_name]
    if not rows:
        return {"games": 0, "home_win_rate": 0.5, "avg_total_score": 44.0}
    games = len(rows)
    home_wins = 0
    total_score = 0
    team_wins = 0
    team_games = 0
    for r in rows:
        total_score += (r["home_score"] or 0) + (r["away_score"] or 0)
        if r["home_score"] > r["away_score"]:
            home_wins += 1
        if team_name:
            team_games += 1
            is_home = r["home_team"] == team_name
            if is_home and r["home_score"] > r["away_score"]:
                team_wins += 1
            elif not is_home and r["away_score"] > r["home_score"]:
                team_wins += 1
    result = {
        "games": games,
        "home_win_rate": round(home_wins / games, 3) if games > 0 else 0.5,
        "avg_total_score": round(total_score / games, 1) if games > 0 else 44.0,
    }
    if team_name:
        result["team_win_rate"] = round(team_wins / team_games, 3) if team_games > 0 else 0.5
        result["team_games"] = team_games
    return result


@_cached_query("team_attack_defence")
def get_team_attack_defence(team_name: str, last_n_games: int = 10, before_season=None, before_round=None) -> dict:
    """Get a team's recent attack/defence stats.
    Returns both full window (last_n_games) and recent window (last 5)
    for recency-weighted blending in the model."""
    tf, tp = _temporal_filter("matches", before_season, before_round)
    conn = get_db()
    rows = conn.execute(f"""
        SELECT home_team, away_team, home_score, away_score
        FROM matches
        WHERE (home_team = %s OR away_team = %s)
          AND match_state = 'FullTime'
          AND home_score IS NOT NULL{tf}
        ORDER BY season DESC, round_number DESC
        LIMIT %s
    """, (team_name, team_name, *tp, last_n_games)).fetchall()
    conn.close()

    if not rows:
        return {
            "avg_scored": 22.0, "avg_conceded": 22.0,
            "avg_scored_recent": 22.0, "avg_conceded_recent": 22.0,
            "wins": 0, "wins_recent": 0, "played": 0,
        }

    total_scored = 0
    total_conceded = 0
    wins = 0
    recent_scored = 0
    recent_conceded = 0
    recent_wins = 0
    recent_n = min(5, len(rows))

    for i, r in enumerate(rows):
        if r["home_team"] == team_name:
            scored = r["home_score"]
            conceded = r["away_score"]
            won = r["home_score"] > r["away_score"]
        else:
            scored = r["away_score"]
            conceded = r["home_score"]
            won = r["away_score"] > r["home_score"]

        total_scored += scored
        total_conceded += conceded
        if won:
            wins += 1

        # Last 5 games (most recent)
        if i < recent_n:
            recent_scored += scored
            recent_conceded += conceded
            if won:
                recent_wins += 1

    n = len(rows)
    return {
        "avg_scored": total_scored / n,
        "avg_conceded": total_conceded / n,
        "avg_scored_recent": recent_scored / recent_n if recent_n > 0 else 22.0,
        "avg_conceded_recent": recent_conceded / recent_n if recent_n > 0 else 22.0,
        "wins": wins,
        "wins_recent": recent_wins,
        "played": n,
    }


@_cached_query("h2h_record")
def get_h2h_record(team_a: str, team_b: str, before_season=None, before_round=None) -> dict:
    """Get head-to-head record between two teams."""
    tf, tp = _temporal_filter("matches", before_season, before_round)
    conn = get_db()
    rows = conn.execute(f"""
        SELECT home_team, away_team, home_score, away_score
        FROM matches
        WHERE ((home_team = %s AND away_team = %s) OR (home_team = %s AND away_team = %s))
          AND match_state = 'FullTime'
          AND home_score IS NOT NULL{tf}
        ORDER BY season DESC, round_number DESC
    """, (team_a, team_b, team_b, team_a, *tp)).fetchall()
    conn.close()

    a_wins = 0
    b_wins = 0
    for r in rows:
        if r["home_team"] == team_a:
            if r["home_score"] > r["away_score"]:
                a_wins += 1
            elif r["away_score"] > r["home_score"]:
                b_wins += 1
        else:
            if r["away_score"] > r["home_score"]:
                a_wins += 1
            elif r["home_score"] > r["away_score"]:
                b_wins += 1

    return {"team_a_wins": a_wins, "team_b_wins": b_wins, "played": len(rows)}


@_cached_query("home_away_win_rate")
def get_home_away_win_rate(team_name: str, before_season=None, before_round=None) -> dict:
    """Get a team's home and away win rates."""
    tf, tp = _temporal_filter("matches", before_season, before_round)
    conn = get_db()

    home_rows = conn.execute(f"""
        SELECT COUNT(*) as played,
               SUM(CASE WHEN home_score > away_score THEN 1 ELSE 0 END) as wins
        FROM matches
        WHERE home_team = %s AND match_state = 'FullTime' AND home_score IS NOT NULL{tf}
    """, (team_name, *tp)).fetchone()

    away_rows = conn.execute(f"""
        SELECT COUNT(*) as played,
               SUM(CASE WHEN away_score > home_score THEN 1 ELSE 0 END) as wins
        FROM matches
        WHERE away_team = %s AND match_state = 'FullTime' AND away_score IS NOT NULL{tf}
    """, (team_name, *tp)).fetchone()

    conn.close()

    return {
        "home_played": home_rows["played"],
        "home_wins": home_rows["wins"] or 0,
        "home_win_rate": (home_rows["wins"] or 0) / home_rows["played"] if home_rows["played"] > 0 else 0.5,
        "away_played": away_rows["played"],
        "away_wins": away_rows["wins"] or 0,
        "away_win_rate": (away_rows["wins"] or 0) / away_rows["played"] if away_rows["played"] > 0 else 0.5,
    }


@_cached_query("team_tries_conceded_by_position")
def get_team_tries_conceded_by_position(team_name: str, last_n_games: int = 15, before_season=None, before_round=None) -> dict:
    """
    Get how many tries a team concedes to each position.
    Uses a single query instead of N+1.
    """
    tf, tp = _temporal_filter("m", before_season, before_round)
    conn = get_db()

    # Single query: get matches + tries conceded in one go
    rows = conn.execute(f"""
        WITH recent_matches AS (
            SELECT id, home_team, away_team
            FROM matches m
            WHERE (m.home_team = %s OR m.away_team = %s)
              AND m.match_state = 'FullTime'{tf}
            ORDER BY m.season DESC, m.round_number DESC
            LIMIT %s
        )
        SELECT p.position, COUNT(*) as cnt,
               (SELECT COUNT(*) FROM recent_matches) as games
        FROM recent_matches rm
        JOIN tries t ON t.match_id = rm.id
            AND t.side = CASE WHEN rm.home_team = %s THEN 'away' ELSE 'home' END
        LEFT JOIN players p ON p.match_id = t.match_id AND p.name = t.player_name AND p.side = t.side
        WHERE p.position IS NOT NULL AND p.position != ''
        GROUP BY p.position
    """, (team_name, team_name, *tp, last_n_games, team_name)).fetchall()
    conn.close()

    if not rows:
        return {}

    games = rows[0]["games"] if rows else 0
    league_rates = get_position_try_rates()

    from collections import Counter
    result = {}
    for r in rows:
        pos = r["position"]
        count = r["cnt"]
        conceded_rate = count / games if games > 0 else 0
        league_rate = league_rates.get(pos, {}).get("rate", 0.1)
        vulnerability = conceded_rate / league_rate if league_rate > 0 else 1.0
        result[pos] = {
            "conceded": count,
            "games": games,
            "rate_per_game": round(conceded_rate, 3),
            "league_avg_rate": round(league_rate, 3),
            "vulnerability": round(vulnerability, 2),
        }

    return result


@_cached_query("team_tries_conceded_by_edge")
def get_team_tries_conceded_by_edge(team_name: str, last_n_games: int = 15, before_season=None, before_round=None) -> dict:
    """
    Get how many tries a team concedes on each edge (left / right / middle).
    Uses a single query instead of N+1.
    """
    tf, tp = _temporal_filter("m", before_season, before_round)
    conn = get_db()

    # Single query: matches + edge counts
    rows = conn.execute(f"""
        WITH recent_matches AS (
            SELECT id, home_team
            FROM matches m
            WHERE (m.home_team = %s OR m.away_team = %s)
              AND m.match_state = 'FullTime'{tf}
            ORDER BY m.season DESC, m.round_number DESC
            LIMIT %s
        )
        SELECT t.field_side, COUNT(*) as cnt,
               (SELECT COUNT(*) FROM recent_matches) as games
        FROM recent_matches rm
        JOIN tries t ON t.match_id = rm.id
            AND t.side = CASE WHEN rm.home_team = %s THEN 'away' ELSE 'home' END
            AND t.field_side != ''
        GROUP BY t.field_side
    """, (team_name, team_name, *tp, last_n_games, team_name)).fetchall()

    # League-wide rates (single query)
    league_rows = conn.execute("""
        SELECT field_side, COUNT(*) as cnt,
               (SELECT COUNT(*) FROM matches WHERE match_state = 'FullTime') as total_games
        FROM tries
        WHERE field_side != ''
        GROUP BY field_side
    """).fetchall()
    conn.close()

    if not rows:
        return {}

    games = rows[0]["games"] if rows else 0
    edge_counts = {"left": 0, "right": 0, "middle": 0, "fullback": 0}
    for r in rows:
        fs = r["field_side"]
        if fs in edge_counts:
            edge_counts[fs] = r["cnt"]

    league_games = league_rows[0]["total_games"] if league_rows else 1
    league_edge_rates = {}
    for r in league_rows:
        league_edge_rates[r["field_side"]] = r["cnt"] / (league_games * 2) if league_games > 0 else 0

    result = {}
    for edge, count in edge_counts.items():
        rate = count / games if games > 0 else 0
        league_rate = league_edge_rates.get(edge, 0.5)
        vulnerability = rate / league_rate if league_rate > 0 else 1.0
        result[edge] = {
            "conceded": count,
            "games": games,
            "rate_per_game": round(rate, 3),
            "league_avg_rate": round(league_rate, 3),
            "vulnerability": round(vulnerability, 2),
        }

    return result


@_cached_query("player_recent_form")
def get_player_recent_form(player_name: str, last_n_games: int = 5, before_season=None, before_round=None) -> dict:
    """
    Get a player's recent try-scoring form.
    Returns: {games: N, tries: N, rate: float, streak: int (consecutive games scoring)}
    """
    tf, tp = _temporal_filter("m", before_season, before_round)
    conn = get_db()
    rows = conn.execute(f"""
        SELECT m.season, m.round_number,
               (SELECT COUNT(*) FROM tries t
                WHERE t.match_id = m.id AND t.player_name = p.name AND t.side = p.side) as tries_scored
        FROM players p
        JOIN matches m ON p.match_id = m.id
        WHERE p.name = %s AND m.match_state = 'FullTime'{tf}
        ORDER BY m.season DESC, m.round_number DESC
        LIMIT %s
    """, (player_name, *tp, last_n_games)).fetchall()
    conn.close()

    if not rows:
        return {"games": 0, "tries": 0, "rate": 0, "streak": 0}

    games = len(rows)
    tries = sum(r["tries_scored"] for r in rows)

    # Calculate current scoring streak (consecutive games with a try, most recent first)
    streak = 0
    for r in rows:
        if r["tries_scored"] > 0:
            streak += 1
        else:
            break

    return {
        "games": games,
        "tries": tries,
        "rate": round(tries / games, 3) if games > 0 else 0,
        "streak": streak,
    }


def get_player_game_log(player_name: str) -> list:
    """
    Get full game log for a player: every match they played, their team,
    position, opponent, score, and tries scored with minutes.
    """
    conn = get_db()
    rows = conn.execute("""
        SELECT p.name, p.team, p.position, p.jersey_number, p.side,
               m.id as match_id, m.season, m.round_number, m.home_team, m.away_team,
               m.home_score, m.away_score, m.venue, m.kickoff, m.match_url
        FROM players p
        JOIN matches m ON p.match_id = m.id
        WHERE p.name = %s AND m.match_state = 'FullTime'
        ORDER BY m.season DESC, m.round_number DESC
    """, (player_name,)).fetchall()

    games = []
    for r in rows:
        match_id = r["match_id"]
        side = r["side"]
        # Get tries this player scored in this match
        tries = conn.execute("""
            SELECT minute FROM tries
            WHERE match_id = %s AND player_name = %s AND side = %s
        """, (match_id, player_name, side)).fetchall()

        opponent = r["away_team"] if r["home_team"] == r["team"] else r["home_team"]
        is_home = r["home_team"] == r["team"]
        team_score = r["home_score"] if is_home else r["away_score"]
        opp_score = r["away_score"] if is_home else r["home_score"]
        won = team_score > opp_score if team_score is not None and opp_score is not None else None

        games.append({
            "season": r["season"],
            "round": r["round_number"],
            "team": r["team"],
            "position": r["position"],
            "jersey_number": r["jersey_number"],
            "opponent": opponent,
            "is_home": is_home,
            "team_score": team_score,
            "opp_score": opp_score,
            "won": won,
            "venue": r["venue"],
            "tries": [t["minute"] for t in tries],
            "try_count": len(tries),
        })

    conn.close()
    return games


def unmark_round(season: int, round_number: int):
    """Mark a round as not scraped so it gets re-scraped."""
    conn = get_db()
    conn.execute(
        "DELETE FROM scrape_progress WHERE season=%s AND round_number=%s",
        (season, round_number)
    )
    conn.commit()
    conn.close()


def delete_match_data(match_url: str):
    """Delete a match and its associated players/tries so it can be re-inserted."""
    conn = get_db()
    row = conn.execute("SELECT id FROM matches WHERE match_url=%s", (match_url,)).fetchone()
    if row:
        mid = row["id"]
        conn.execute("DELETE FROM tries WHERE match_id=%s", (mid,))
        conn.execute("DELETE FROM players WHERE match_id=%s", (mid,))
        conn.execute("DELETE FROM interchanges WHERE match_id=%s", (mid,))
        conn.execute("DELETE FROM matches WHERE id=%s", (mid,))
        conn.commit()
    conn.close()


def get_incomplete_match_urls_for_round(season: int, round_number: int) -> list:
    """
    Get match URLs from a round that we DON'T have as completed yet.
    Used to find games that have finished since we last checked.
    """
    conn = get_db()
    rows = conn.execute("""
        SELECT match_url FROM matches
        WHERE season=%s AND round_number=%s
    """, (season, round_number)).fetchall()
    conn.close()
    return [r["match_url"] for r in rows]


@_cached_query("total_match_count", ttl=60)
def get_total_match_count() -> int:
    conn = get_db()
    row = conn.execute("SELECT COUNT(*) as cnt FROM matches WHERE match_state='FullTime'").fetchone()
    conn.close()
    return row["cnt"]


@_cached_query("total_try_count", ttl=60)
def get_total_try_count() -> int:
    conn = get_db()
    row = conn.execute("SELECT COUNT(*) as cnt FROM tries").fetchone()
    conn.close()
    return row["cnt"]


def get_team_scoring_breakdown(team_name: str, last_n_games: int = 10, before_season=None, before_round=None) -> dict:
    """
    Get a team's scoring breakdown: tries scored, tries conceded,
    and kicking points (conversions + penalties + field goals) per game.

    Derives kicking points from: total_score - (tries x 4).
    """
    tf, tp = _temporal_filter("matches", before_season, before_round)
    conn = get_db()

    match_rows = conn.execute(f"""
        SELECT id, home_team, away_team, home_score, away_score
        FROM matches
        WHERE (home_team = %s OR away_team = %s)
          AND match_state = 'FullTime'
          AND home_score IS NOT NULL{tf}
        ORDER BY season DESC, round_number DESC
        LIMIT %s
    """, (team_name, team_name, *tp, last_n_games)).fetchall()

    if not match_rows:
        conn.close()
        return {
            "games": 0,
            "tries_scored_pg": 3.5, "tries_conceded_pg": 3.5,
            "kick_pts_scored_pg": 8.0, "kick_pts_conceded_pg": 8.0,
            "conversion_rate": 0.75,
        }

    total_tries_scored = 0
    total_tries_conceded = 0
    total_kick_pts_scored = 0
    total_kick_pts_conceded = 0
    recent_tries_scored = 0
    recent_tries_conceded = 0
    recent_kick_pts_scored = 0
    recent_kick_pts_conceded = 0
    games = len(match_rows)
    recent_n = min(5, games)

    for i, mr in enumerate(match_rows):
        is_home = mr["home_team"] == team_name
        scoring_side = "home" if is_home else "away"
        conceding_side = "away" if is_home else "home"
        team_score = mr["home_score"] if is_home else mr["away_score"]
        opp_score = mr["away_score"] if is_home else mr["home_score"]

        tries_for = conn.execute(
            "SELECT COUNT(*) as cnt FROM tries WHERE match_id = %s AND side = %s",
            (mr["id"], scoring_side)
        ).fetchone()["cnt"]

        tries_against = conn.execute(
            "SELECT COUNT(*) as cnt FROM tries WHERE match_id = %s AND side = %s",
            (mr["id"], conceding_side)
        ).fetchone()["cnt"]

        kick_pts_for = max(0, team_score - tries_for * 4)
        kick_pts_against = max(0, opp_score - tries_against * 4)

        total_tries_scored += tries_for
        total_tries_conceded += tries_against
        total_kick_pts_scored += kick_pts_for
        total_kick_pts_conceded += kick_pts_against

        if i < recent_n:
            recent_tries_scored += tries_for
            recent_tries_conceded += tries_against
            recent_kick_pts_scored += kick_pts_for
            recent_kick_pts_conceded += kick_pts_against

    conn.close()

    # Conversion rate estimate: kick_pts / (tries * 2) — capped at 1.0
    total_max_conv = total_tries_scored * 2 if total_tries_scored > 0 else 1
    conv_rate = min(total_kick_pts_scored / total_max_conv, 1.0)

    return {
        "games": games,
        "tries_scored_pg": total_tries_scored / games,
        "tries_conceded_pg": total_tries_conceded / games,
        "kick_pts_scored_pg": total_kick_pts_scored / games,
        "kick_pts_conceded_pg": total_kick_pts_conceded / games,
        "conversion_rate": round(conv_rate, 3),
        # Recent window (last 5)
        "tries_scored_pg_recent": recent_tries_scored / recent_n if recent_n > 0 else 3.5,
        "tries_conceded_pg_recent": recent_tries_conceded / recent_n if recent_n > 0 else 3.5,
        "kick_pts_scored_pg_recent": recent_kick_pts_scored / recent_n if recent_n > 0 else 8.0,
        "kick_pts_conceded_pg_recent": recent_kick_pts_conceded / recent_n if recent_n > 0 else 8.0,
    }


# ---- Prediction tracking ----


def get_unrecorded_completed_matches(model_version: int = 2) -> list:
    """Get completed matches that don't have a prediction recorded yet for this model version."""
    conn = get_db()
    rows = conn.execute("""
        SELECT m.match_url, m.season, m.round_number, m.home_team, m.away_team,
               m.home_score, m.away_score, m.venue, m.kickoff
        FROM matches m
        WHERE m.match_state = 'FullTime'
          AND m.home_score IS NOT NULL
          AND m.season = 2026
          AND NOT EXISTS (
              SELECT 1 FROM predictions p
              WHERE p.match_url = m.match_url AND p.model_version = %s
          )
        ORDER BY m.season, m.round_number
    """, (model_version,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def upsert_prediction(match_url: str, season: int, round_number: int, model_version: int,
                      home_team: str, away_team: str, predicted_winner: str,
                      home_win_prob: float, predicted_home_score: int, predicted_away_score: int,
                      actual_winner: str = None, actual_home_score: int = None,
                      actual_away_score: int = None, win_correct: int = None,
                      top3_home_json: str = None, top3_away_json: str = None,
                      top3_hits: int = None, multi_json: str = None,
                      multi_hits: int = None, multi_all_scored: int = None):
    """Insert or update a prediction record."""
    conn = get_db()
    conn.execute("""
        INSERT INTO predictions
            (match_url, season, round_number, model_version, home_team, away_team,
             predicted_winner, home_win_prob, predicted_home_score, predicted_away_score,
             actual_winner, actual_home_score, actual_away_score, win_correct,
             top3_home_json, top3_away_json, top3_hits,
             multi_json, multi_hits, multi_all_scored)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT(match_url, model_version) DO UPDATE SET
            actual_winner=EXCLUDED.actual_winner,
            actual_home_score=EXCLUDED.actual_home_score,
            actual_away_score=EXCLUDED.actual_away_score,
            win_correct=EXCLUDED.win_correct,
            top3_home_json=EXCLUDED.top3_home_json,
            top3_away_json=EXCLUDED.top3_away_json,
            top3_hits=EXCLUDED.top3_hits,
            multi_json=EXCLUDED.multi_json,
            multi_hits=EXCLUDED.multi_hits,
            multi_all_scored=EXCLUDED.multi_all_scored
    """, (match_url, season, round_number, model_version, home_team, away_team,
          predicted_winner, home_win_prob, predicted_home_score, predicted_away_score,
          actual_winner, actual_home_score, actual_away_score, win_correct,
          top3_home_json, top3_away_json, top3_hits,
          multi_json, multi_hits, multi_all_scored))
    conn.commit()
    conn.close()


def get_accuracy_stats(model_version: int = None, season: int = None) -> dict:
    """Get aggregate accuracy stats, optionally filtered by model version and season."""
    conn = get_db()
    where = "WHERE actual_winner IS NOT NULL"
    params = []
    if model_version is not None:
        where += " AND model_version = %s"
        params.append(model_version)
    if season is not None:
        where += " AND season = %s"
        params.append(season)

    # Overall win prediction accuracy
    row = conn.execute(f"""
        SELECT COUNT(*) as total,
               SUM(CASE WHEN win_correct = 1 THEN 1 ELSE 0 END) as correct
        FROM predictions {where}
    """, params).fetchone()
    win_total = row["total"]
    win_correct = row["correct"] or 0

    # Try pick accuracy (top 3 per team = 6 picks per match)
    try_row = conn.execute(f"""
        SELECT SUM(top3_hits) as hits,
               COUNT(*) * 6 as total_picks
        FROM predictions {where} AND top3_hits IS NOT NULL
    """, params).fetchone()
    try_hits = try_row["hits"] or 0
    try_total = try_row["total_picks"] or 0

    # Multi accuracy
    multi_row = conn.execute(f"""
        SELECT COUNT(*) as total,
               SUM(CASE WHEN multi_all_scored = 1 THEN 1 ELSE 0 END) as all_hit,
               SUM(multi_hits) as total_hits,
               COUNT(*) * 3 as total_picks
        FROM predictions {where} AND multi_hits IS NOT NULL
    """, params).fetchone()

    # By round
    by_round = conn.execute(f"""
        SELECT round_number,
               COUNT(*) as total,
               SUM(CASE WHEN win_correct = 1 THEN 1 ELSE 0 END) as win_correct,
               SUM(top3_hits) as try_hits,
               COUNT(*) * 6 as try_total,
               SUM(CASE WHEN multi_all_scored = 1 THEN 1 ELSE 0 END) as multi_all_hit,
               SUM(multi_hits) as multi_hits
        FROM predictions {where}
        GROUP BY round_number ORDER BY round_number
    """, params).fetchall()

    # By model version
    by_model = conn.execute(f"""
        SELECT model_version,
               COUNT(*) as total,
               SUM(CASE WHEN win_correct = 1 THEN 1 ELSE 0 END) as win_correct,
               SUM(top3_hits) as try_hits,
               SUM(multi_hits) as multi_hits,
               SUM(CASE WHEN multi_all_scored = 1 THEN 1 ELSE 0 END) as multi_all_hit
        FROM predictions WHERE actual_winner IS NOT NULL
        {"AND season = %s" if season is not None else ""}
        GROUP BY model_version ORDER BY model_version
    """, [season] if season is not None else []).fetchall()

    conn.close()

    return {
        "win_prediction": {
            "total": win_total,
            "correct": win_correct,
            "accuracy": round(win_correct / win_total, 3) if win_total > 0 else 0,
        },
        "try_picks": {
            "total_picks": try_total,
            "hits": try_hits,
            "hit_rate": round(try_hits / try_total, 3) if try_total > 0 else 0,
        },
        "multi": {
            "total": multi_row["total"] or 0,
            "all_scored": multi_row["all_hit"] or 0,
            "hit_rate": round((multi_row["all_hit"] or 0) / multi_row["total"], 3) if multi_row["total"] else 0,
            "total_hits": multi_row["total_hits"] or 0,
            "avg_hits": round((multi_row["total_hits"] or 0) / multi_row["total"], 1) if multi_row["total"] else 0,
        },
        "by_round": [dict(r) for r in by_round],
        "by_model": [dict(r) for r in by_model],
    }


# ---- Venue/weather stats ----


@_cached_query("venue_stats")
def get_venue_stats(venue_name: str, team_name: str = None) -> dict:
    """Get win rate and scoring stats at a specific venue, optionally for a specific team."""
    conn = get_db()

    if team_name:
        rows = conn.execute("""
            SELECT home_team, away_team, home_score, away_score
            FROM matches
            WHERE venue = %s AND (home_team = %s OR away_team = %s)
              AND match_state = 'FullTime' AND home_score IS NOT NULL
            ORDER BY season DESC, round_number DESC
        """, (venue_name, team_name, team_name)).fetchall()
    else:
        rows = conn.execute("""
            SELECT home_team, away_team, home_score, away_score
            FROM matches
            WHERE venue = %s AND match_state = 'FullTime' AND home_score IS NOT NULL
            ORDER BY season DESC, round_number DESC
        """, (venue_name,)).fetchall()

    conn.close()

    if not rows:
        return {"games": 0, "home_win_rate": 0.5, "avg_total_score": 44.0}

    games = len(rows)
    home_wins = 0
    total_score = 0
    team_wins = 0
    team_games = 0

    for r in rows:
        total_score += (r["home_score"] or 0) + (r["away_score"] or 0)
        if r["home_score"] > r["away_score"]:
            home_wins += 1
        if team_name:
            team_games += 1
            is_home = r["home_team"] == team_name
            if is_home and r["home_score"] > r["away_score"]:
                team_wins += 1
            elif not is_home and r["away_score"] > r["home_score"]:
                team_wins += 1

    result = {
        "games": games,
        "home_win_rate": round(home_wins / games, 3) if games > 0 else 0.5,
        "avg_total_score": round(total_score / games, 1) if games > 0 else 44.0,
    }
    if team_name:
        result["team_win_rate"] = round(team_wins / team_games, 3) if team_games > 0 else 0.5
        result["team_games"] = team_games

    return result


# ---- V3 model queries ----


@_cached_query("team_rest_days")
def get_team_rest_days(team_name: str, season: int, round_number: int) -> int:
    """Get days since team's last match. Returns -1 if unknown."""
    conn = get_db()
    row = conn.execute("""
        SELECT kickoff FROM matches
        WHERE (home_team = %s OR away_team = %s)
          AND match_state = 'FullTime'
          AND (season < %s OR (season = %s AND round_number < %s))
        ORDER BY season DESC, round_number DESC
        LIMIT 1
    """, (team_name, team_name, season, season, round_number)).fetchone()
    if not row or not row["kickoff"]:
        conn.close()
        return -1
    # Also get current match kickoff
    cur_row = conn.execute("""
        SELECT kickoff FROM matches
        WHERE (home_team = %s OR away_team = %s)
          AND season = %s AND round_number = %s
        LIMIT 1
    """, (team_name, team_name, season, round_number)).fetchone()
    conn.close()
    if not cur_row or not cur_row["kickoff"]:
        return -1
    try:
        from datetime import datetime
        # Parse kickoff strings — format varies, try common patterns
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S%z",
                    "%A %d %B %Y %I:%M%p", "%A, %B %d, %Y %I:%M %p"):
            try:
                prev = datetime.strptime(row["kickoff"][:19], fmt[:min(len(fmt), 19)])
                curr = datetime.strptime(cur_row["kickoff"][:19], fmt[:min(len(fmt), 19)])
                return max(0, (curr - prev).days)
            except (ValueError, TypeError):
                continue
        return -1
    except Exception:
        return -1


@_cached_query("team_margin_weighted_form")
def get_team_margin_weighted_form(team_name: str, last_n_games: int = 10,
                                   before_season=None, before_round=None) -> dict:
    """Get margin-of-victory weighted form stats.
    Returns quality_score: wins by large margins count more, losses by large margins count less."""
    tf, tp = _temporal_filter("m", before_season, before_round)
    conn = get_db()
    rows = conn.execute(f"""
        SELECT home_team, away_team, home_score, away_score
        FROM matches m
        WHERE (m.home_team = %s OR m.away_team = %s)
          AND m.match_state = 'FullTime' AND m.home_score IS NOT NULL{tf}
        ORDER BY m.season DESC, m.round_number DESC
        LIMIT %s
    """, (team_name, team_name, *tp, last_n_games)).fetchall()
    conn.close()

    if not rows:
        return {"quality_score": 0.5, "avg_margin": 0, "blowout_wins": 0, "close_losses": 0}

    import math
    total_quality = 0
    margins = []
    blowout_wins = 0
    close_losses = 0
    for r in rows:
        if r["home_team"] == team_name:
            margin = r["home_score"] - r["away_score"]
        else:
            margin = r["away_score"] - r["home_score"]
        margins.append(margin)
        # Sigmoid-like quality mapping: big wins → ~1.0, big losses → ~0.0
        quality = 1.0 / (1.0 + math.exp(-margin / 8.0))
        total_quality += quality
        if margin >= 18:
            blowout_wins += 1
        if -6 <= margin < 0:
            close_losses += 1

    return {
        "quality_score": round(total_quality / len(rows), 3),
        "avg_margin": round(sum(margins) / len(margins), 1),
        "blowout_wins": blowout_wins,
        "close_losses": close_losses,
    }


@_cached_query("team_had_bye")
def get_team_had_bye(team_name: str, season: int, round_number: int) -> bool:
    """Check if the team had a bye in the previous round."""
    if round_number <= 1:
        return False
    prev_round = round_number - 1
    conn = get_db()
    row = conn.execute("""
        SELECT 1 FROM matches
        WHERE (home_team = %s OR away_team = %s)
          AND season = %s AND round_number = %s
    """, (team_name, team_name, season, prev_round)).fetchone()
    conn.close()
    return row is None


@_cached_query("player_try_minutes")
def get_player_try_minute_profile(player_name: str, before_season=None, before_round=None) -> dict:
    """Get a player's try-scoring distribution by match period (first/second half)."""
    tf, tp = _temporal_filter("m", before_season, before_round)
    conn = get_db()
    rows = conn.execute(f"""
        SELECT t.minute
        FROM tries t
        JOIN matches m ON m.id = t.match_id
        WHERE t.player_name = %s AND m.match_state = 'FullTime'{tf}
    """, (player_name, *tp)).fetchall()
    conn.close()

    if not rows:
        return {"total": 0, "first_half": 0, "second_half": 0, "first_half_rate": 0.5}

    first_half = 0
    second_half = 0
    for r in rows:
        try:
            minute = int(str(r["minute"]).replace("'", "").strip())
            if minute <= 40:
                first_half += 1
            else:
                second_half += 1
        except (ValueError, TypeError):
            second_half += 1  # default to second half if unparseable

    total = first_half + second_half
    return {
        "total": total,
        "first_half": first_half,
        "second_half": second_half,
        "first_half_rate": round(first_half / total, 3) if total > 0 else 0.5,
    }


def get_player_try_minutes_batch(player_names: list, before_season=None, before_round=None) -> dict:
    """Batch version of get_player_try_minute_profile."""
    if not player_names:
        return {}
    tf, tp = _temporal_filter("m", before_season, before_round)
    conn = get_db()
    placeholders = ", ".join(["%s"] * len(player_names))
    rows = conn.execute(f"""
        SELECT t.player_name, t.minute
        FROM tries t
        JOIN matches m ON m.id = t.match_id
        WHERE t.player_name IN ({placeholders}) AND m.match_state = 'FullTime'{tf}
    """, (*player_names, *tp)).fetchall()
    conn.close()

    profiles = {}
    for r in rows:
        name = r["player_name"]
        if name not in profiles:
            profiles[name] = {"first_half": 0, "second_half": 0}
        try:
            minute = int(str(r["minute"]).replace("'", "").strip())
            if minute <= 40:
                profiles[name]["first_half"] += 1
            else:
                profiles[name]["second_half"] += 1
        except (ValueError, TypeError):
            profiles[name]["second_half"] += 1

    result = {}
    for name in player_names:
        p = profiles.get(name, {"first_half": 0, "second_half": 0})
        total = p["first_half"] + p["second_half"]
        result[name] = {
            "total": total,
            "first_half": p["first_half"],
            "second_half": p["second_half"],
            "first_half_rate": round(p["first_half"] / total, 3) if total > 0 else 0.5,
        }
    return result


@_cached_query("player_avg_interchange_minutes")
def get_player_avg_minutes(player_name: str, before_season=None, before_round=None) -> float:
    """Estimate average minutes played for a bench player from interchange data.
    Returns estimated minutes (0-80). Returns 80 for starters, ~30 for unknown bench."""
    tf, tp = _temporal_filter("m", before_season, before_round)
    conn = get_db()
    # Find games where this player came on as interchange
    rows = conn.execute(f"""
        SELECT i.game_seconds
        FROM interchanges i
        JOIN matches m ON m.id = i.match_id
        WHERE i.player_on = %s AND m.match_state = 'FullTime'{tf}
        ORDER BY m.season DESC, m.round_number DESC
        LIMIT 10
    """, (player_name, *tp)).fetchall()
    conn.close()

    if not rows:
        return 30.0  # Default for unknown bench players

    # Average entry time in minutes, then estimate minutes played = 80 - entry_time
    entry_minutes = [r["game_seconds"] / 60.0 for r in rows if r["game_seconds"] is not None]
    if not entry_minutes:
        return 30.0
    avg_entry = sum(entry_minutes) / len(entry_minutes)
    return round(max(5, 80.0 - avg_entry), 1)


def get_bench_minutes_batch(player_names: list, before_season=None, before_round=None) -> dict:
    """Batch version: get estimated minutes for bench players."""
    if not player_names:
        return {}
    tf, tp = _temporal_filter("m", before_season, before_round)
    conn = get_db()
    placeholders = ", ".join(["%s"] * len(player_names))
    rows = conn.execute(f"""
        SELECT i.player_on, i.game_seconds
        FROM interchanges i
        JOIN matches m ON m.id = i.match_id
        WHERE i.player_on IN ({placeholders}) AND m.match_state = 'FullTime'{tf}
        ORDER BY m.season DESC, m.round_number DESC
    """, (*player_names, *tp)).fetchall()
    conn.close()

    entries = {}
    for r in rows:
        name = r["player_on"]
        if r["game_seconds"] is not None:
            entries.setdefault(name, []).append(r["game_seconds"] / 60.0)

    result = {}
    for name in player_names:
        if name in entries and entries[name]:
            # Only use last 10 entries
            recent = entries[name][:10]
            avg_entry = sum(recent) / len(recent)
            result[name] = round(max(5, 80.0 - avg_entry), 1)
        else:
            result[name] = 30.0
    return result


@_cached_query("calibration_curve")
def get_calibration_data() -> dict:
    """Get calibration data from historical predictions.
    Groups predicted try percentages into buckets and returns actual hit rates."""
    conn = get_db()
    # Get all predictions with top3 data
    rows = conn.execute("""
        SELECT top3_home_json, top3_away_json, top3_hits
        FROM predictions
        WHERE actual_winner IS NOT NULL AND top3_home_json IS NOT NULL
    """).fetchall()
    conn.close()

    if len(rows) < 10:
        return {}  # Not enough data to calibrate

    import json
    # Collect (predicted_prob, did_score) pairs
    pairs = []
    for r in rows:
        try:
            home_picks = json.loads(r["top3_home_json"]) if r["top3_home_json"] else []
            away_picks = json.loads(r["top3_away_json"]) if r["top3_away_json"] else []
            for pick in home_picks + away_picks:
                if "try_percentage" in pick and "scored" in pick:
                    pairs.append((pick["try_percentage"] / 100.0, 1 if pick["scored"] else 0))
        except (json.JSONDecodeError, TypeError):
            continue

    if len(pairs) < 30:
        return {}

    # Create calibration buckets (0-10%, 10-20%, etc.)
    buckets = {}
    for pred, actual in pairs:
        bucket = min(int(pred * 10), 5)  # 0-5 (0-10%, 10-20%, ..., 50%+)
        if bucket not in buckets:
            buckets[bucket] = {"predicted_sum": 0, "actual_sum": 0, "count": 0}
        buckets[bucket]["predicted_sum"] += pred
        buckets[bucket]["actual_sum"] += actual
        buckets[bucket]["count"] += 1

    calibration = {}
    for bucket, data in buckets.items():
        if data["count"] >= 5:
            avg_predicted = data["predicted_sum"] / data["count"]
            avg_actual = data["actual_sum"] / data["count"]
            calibration[bucket] = {
                "avg_predicted": round(avg_predicted, 3),
                "avg_actual": round(avg_actual, 3),
                "ratio": round(avg_actual / avg_predicted, 3) if avg_predicted > 0 else 1.0,
                "count": data["count"],
            }
    return calibration


@_cached_query("opponent_quality_tries")
def get_player_quality_adjusted_tries(player_name: str, before_season=None, before_round=None) -> float:
    """Get a player's try rate adjusted for opponent defensive quality.
    Tries against weak defences are discounted, tries against strong defences are boosted."""
    tf, tp = _temporal_filter("m", before_season, before_round)
    conn = get_db()
    # Get all matches this player appeared in with their try counts and opponent
    rows = conn.execute(f"""
        SELECT p.match_id, p.side,
               m.home_team, m.away_team, m.home_score, m.away_score,
               (SELECT COUNT(*) FROM tries t WHERE t.match_id = p.match_id AND t.player_name = p.name) as tries_scored
        FROM players p
        JOIN matches m ON m.id = p.match_id
        WHERE p.name = %s AND m.match_state = 'FullTime'{tf}
        ORDER BY m.season DESC, m.round_number DESC
        LIMIT 20
    """, (player_name, *tp)).fetchall()
    conn.close()

    if len(rows) < 3:
        return -1.0  # Not enough data

    # Calculate league average conceding
    total_weighted_rate = 0
    total_weight = 0
    for r in rows:
        opponent = r["away_team"] if r["side"] == "home" else r["home_team"]
        # Opponent's points conceded in this match as a proxy for defensive quality
        opp_conceded = r["home_score"] if r["side"] == "away" else r["away_score"]
        # Weight: strong defence (low conceding) → weight > 1, weak defence → weight < 1
        defence_quality = max(0.5, min(22.0 / max(opp_conceded, 4), 2.0))
        tries = r["tries_scored"] or 0
        total_weighted_rate += tries * defence_quality
        total_weight += defence_quality

    if total_weight == 0:
        return -1.0
    return round(total_weighted_rate / total_weight, 3)


def get_quality_adjusted_tries_batch(player_names: list, before_season=None, before_round=None) -> dict:
    """Batch version of get_player_quality_adjusted_tries.
    Returns {player_name: quality_adjusted_rate} for all players with enough data."""
    if not player_names:
        return {}
    tf, tp = _temporal_filter("m", before_season, before_round)
    conn = get_db()
    placeholders = ", ".join(["%s"] * len(player_names))
    rows = conn.execute(f"""
        SELECT p.name, p.side, p.match_id,
               m.home_team, m.away_team, m.home_score, m.away_score,
               (SELECT COUNT(*) FROM tries t WHERE t.match_id = p.match_id AND t.player_name = p.name) as tries_scored
        FROM players p
        JOIN matches m ON m.id = p.match_id
        WHERE p.name IN ({placeholders}) AND m.match_state = 'FullTime'{tf}
        ORDER BY m.season DESC, m.round_number DESC
    """, (*player_names, *tp)).fetchall()
    conn.close()

    # Group by player, keep last 20 per player
    player_rows = {}
    for r in rows:
        name = r["name"]
        if name not in player_rows:
            player_rows[name] = []
        if len(player_rows[name]) < 20:
            player_rows[name].append(r)

    result = {}
    for name, prows in player_rows.items():
        if len(prows) < 3:
            continue
        total_weighted_rate = 0
        total_weight = 0
        for r in prows:
            opp_conceded = r["home_score"] if r["side"] == "away" else r["away_score"]
            defence_quality = max(0.5, min(22.0 / max(opp_conceded, 4), 2.0))
            tries = r["tries_scored"] or 0
            total_weighted_rate += tries * defence_quality
            total_weight += defence_quality
        if total_weight > 0:
            result[name] = round(total_weighted_rate / total_weight, 3)
    return result


def get_weather_scoring_impact() -> dict:
    """Get average tries per game under different weather/ground conditions vs overall."""
    conn = get_db()

    # Overall average tries per game
    overall = conn.execute("""
        SELECT COUNT(DISTINCT m.id) as games,
               COUNT(t.id) as tries
        FROM matches m
        LEFT JOIN tries t ON t.match_id = m.id
        WHERE m.match_state = 'FullTime'
    """).fetchone()
    overall_tpg = (overall["tries"] / (overall["games"] * 2)) if overall["games"] > 0 else 3.5

    # By weather condition
    weather_rows = conn.execute("""
        SELECT m.weather,
               COUNT(DISTINCT m.id) as games,
               COUNT(t.id) as tries
        FROM matches m
        LEFT JOIN tries t ON t.match_id = m.id
        WHERE m.match_state = 'FullTime' AND m.weather != '' AND m.weather IS NOT NULL
        GROUP BY m.weather
    """).fetchall()

    # By ground condition
    ground_rows = conn.execute("""
        SELECT m.ground_conditions,
               COUNT(DISTINCT m.id) as games,
               COUNT(t.id) as tries
        FROM matches m
        LEFT JOIN tries t ON t.match_id = m.id
        WHERE m.match_state = 'FullTime' AND m.ground_conditions != '' AND m.ground_conditions IS NOT NULL
        GROUP BY m.ground_conditions
    """).fetchall()

    conn.close()

    weather = {}
    for r in weather_rows:
        if r["games"] >= 3:
            tpg = r["tries"] / (r["games"] * 2) if r["games"] > 0 else overall_tpg
            weather[r["weather"]] = {
                "games": r["games"],
                "tries_per_game": round(tpg, 2),
                "vs_average": round(tpg / overall_tpg, 3) if overall_tpg > 0 else 1.0,
            }

    ground = {}
    for r in ground_rows:
        if r["games"] >= 3:
            tpg = r["tries"] / (r["games"] * 2) if r["games"] > 0 else overall_tpg
            ground[r["ground_conditions"]] = {
                "games": r["games"],
                "tries_per_game": round(tpg, 2),
                "vs_average": round(tpg / overall_tpg, 3) if overall_tpg > 0 else 1.0,
            }

    return {
        "overall_tries_per_game": round(overall_tpg, 2),
        "by_weather": weather,
        "by_ground": ground,
    }


# ---- Search ----


def search_players(query: str, limit: int = 20) -> list:
    """Search for players by name. Returns one row per player with their most
    recent team and position, plus career totals. Uses ILIKE for
    case-insensitive matching."""
    conn = get_db()
    rows = conn.execute("""
        WITH latest AS (
            SELECT DISTINCT ON (p.name)
                   p.name,
                   p.team,
                   p.position,
                   p.jersey_number,
                   (m.season * 100 + m.round_number) AS latest_round
            FROM players p
            JOIN matches m ON p.match_id = m.id
            WHERE p.name ILIKE %s AND m.match_state = 'FullTime'
            ORDER BY p.name, m.season DESC, m.round_number DESC
        )
        SELECT l.name,
               l.team,
               l.position,
               l.jersey_number,
               l.latest_round,
               (SELECT COUNT(DISTINCT p2.match_id)
                  FROM players p2
                  JOIN matches m2 ON p2.match_id = m2.id
                  WHERE p2.name = l.name AND m2.match_state = 'FullTime') AS total_games,
               (SELECT COUNT(*) FROM tries t WHERE t.player_name = l.name) AS total_tries
        FROM latest l
        ORDER BY l.latest_round DESC
        LIMIT %s
    """, (f"%{query}%", limit)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def search_teams(query: str) -> list:
    """Search for team names matching the query (case-insensitive)."""
    conn = get_db()
    rows = conn.execute("""
        SELECT DISTINCT home_team as name FROM matches
        WHERE home_team ILIKE %s
        UNION
        SELECT DISTINCT away_team as name FROM matches
        WHERE away_team ILIKE %s
        ORDER BY name
    """, (f"%{query}%", f"%{query}%")).fetchall()
    conn.close()
    return [r["name"] for r in rows]


def get_all_teams() -> list:
    """Get all unique team names."""
    conn = get_db()
    rows = conn.execute("""
        SELECT DISTINCT home_team as name FROM matches
        UNION
        SELECT DISTINCT away_team as name FROM matches
        ORDER BY name
    """).fetchall()
    conn.close()
    return [r["name"] for r in rows]


def get_team_roster(team_name: str, season: int = None) -> list:
    """Get all players who played for a team, optionally in a specific season."""
    conn = get_db()
    if season:
        rows = conn.execute("""
            SELECT p.name, p.position, p.jersey_number,
                   COUNT(DISTINCT m.id) as games,
                   (SELECT COUNT(*) FROM tries t
                    JOIN matches m2 ON t.match_id = m2.id
                    WHERE t.player_name = p.name AND t.team = %s
                      AND m2.season = %s) as tries
            FROM players p
            JOIN matches m ON p.match_id = m.id
            WHERE p.team = %s AND m.season = %s AND m.match_state = 'FullTime'
            GROUP BY p.name, p.position, p.jersey_number
            ORDER BY MIN(p.jersey_number), p.name
        """, (team_name, season, team_name, season)).fetchall()
    else:
        rows = conn.execute("""
            SELECT p.name, p.position, p.jersey_number,
                   COUNT(DISTINCT m.id) as games,
                   (SELECT COUNT(*) FROM tries t WHERE t.player_name = p.name AND t.team = %s) as tries
            FROM players p
            JOIN matches m ON p.match_id = m.id
            WHERE p.team = %s AND m.match_state = 'FullTime'
            GROUP BY p.name, p.position, p.jersey_number
            ORDER BY MIN(p.jersey_number), p.name
        """, (team_name, team_name)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_team_recent_results(team_name: str, last_n: int = 10) -> list:
    """Get a team's most recent results."""
    conn = get_db()
    rows = conn.execute("""
        SELECT season, round_number, home_team, away_team, home_score, away_score, venue
        FROM matches
        WHERE (home_team = %s OR away_team = %s) AND match_state = 'FullTime'
          AND home_score IS NOT NULL
        ORDER BY season DESC, round_number DESC
        LIMIT %s
    """, (team_name, team_name, last_n)).fetchall()
    conn.close()

    results = []
    for r in rows:
        is_home = r["home_team"] == team_name
        team_score = r["home_score"] if is_home else r["away_score"]
        opp_score = r["away_score"] if is_home else r["home_score"]
        opponent = r["away_team"] if is_home else r["home_team"]
        results.append({
            "season": r["season"],
            "round": r["round_number"],
            "opponent": opponent,
            "is_home": is_home,
            "team_score": team_score,
            "opp_score": opp_score,
            "result": "W" if team_score > opp_score else ("L" if opp_score > team_score else "D"),
            "venue": r["venue"],
        })
    return results
