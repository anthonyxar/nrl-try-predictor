"""
PostgreSQL (Supabase) database for storing historical NRL match and try-scoring data.
Uses psycopg2 with connection pooling.
"""

import os
import logging
import atexit

import psycopg2
import psycopg2.pool
import psycopg2.extras

logger = logging.getLogger(__name__)

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
            1, 10, dsn,
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

    conn.commit()

    # Migration: add field_side column to tries
    try:
        conn.execute("ALTER TABLE tries ADD COLUMN field_side TEXT DEFAULT ''")
        conn.commit()
        logger.info("Migration: added field_side column to tries table")
    except psycopg2.errors.DuplicateColumn:
        conn.rollback()  # must rollback the failed transaction in PostgreSQL

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


def get_team_tries_conceded_by_position(team_name: str, last_n_games: int = 15, before_season=None, before_round=None) -> dict:
    """
    Get how many tries a team concedes to each position.
    Returns: {position: {conceded: N, games: N, rate: float, league_avg: float, vulnerability: float}}
    vulnerability > 1.0 means they concede MORE than league average to that position.
    """
    tf, tp = _temporal_filter("matches", before_season, before_round)
    conn = get_db()

    # Find the last N matches for this team
    match_rows = conn.execute(f"""
        SELECT id, home_team, away_team
        FROM matches
        WHERE (home_team = %s OR away_team = %s)
          AND match_state = 'FullTime'{tf}
        ORDER BY season DESC, round_number DESC
        LIMIT %s
    """, (team_name, team_name, *tp, last_n_games)).fetchall()

    if not match_rows:
        conn.close()
        return {}

    match_ids = [r["id"] for r in match_rows]
    games = len(match_ids)

    # For each match, the "opposing" side scored the tries against this team
    conceded_tries = []
    for mr in match_rows:
        conceding_side = "away" if mr["home_team"] == team_name else "home"
        scoring_side = "home" if conceding_side == "away" else "away"
        tries = conn.execute("""
            SELECT t.player_name, p.position
            FROM tries t
            LEFT JOIN players p ON p.match_id = t.match_id AND p.name = t.player_name AND p.side = t.side
            WHERE t.match_id = %s AND t.side = %s
        """, (mr["id"], scoring_side)).fetchall()
        for t in tries:
            pos = t["position"] or "Unknown"
            conceded_tries.append(pos)

    conn.close()

    # Count by position
    from collections import Counter
    pos_counts = Counter(conceded_tries)

    # Get league-wide position try rates for comparison
    league_rates = get_position_try_rates()

    result = {}
    for pos, count in pos_counts.items():
        if not pos or pos == "Unknown":
            continue
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


def get_team_tries_conceded_by_edge(team_name: str, last_n_games: int = 15, before_season=None, before_round=None) -> dict:
    """
    Get how many tries a team concedes on each edge (left / right / middle).
    Returns: {edge: {conceded: N, games: N, rate_per_game: float}}
    'left'/'right' from the SCORING team's perspective, so a team's left-edge
    weakness means they leak tries to the opponent's left-side attackers.
    """
    tf, tp = _temporal_filter("matches", before_season, before_round)
    conn = get_db()

    match_rows = conn.execute(f"""
        SELECT id, home_team, away_team
        FROM matches
        WHERE (home_team = %s OR away_team = %s)
          AND match_state = 'FullTime'{tf}
        ORDER BY season DESC, round_number DESC
        LIMIT %s
    """, (team_name, team_name, *tp, last_n_games)).fetchall()

    if not match_rows:
        conn.close()
        return {}

    games = len(match_rows)
    edge_counts = {"left": 0, "right": 0, "middle": 0, "fullback": 0}

    for mr in match_rows:
        scoring_side = "away" if mr["home_team"] == team_name else "home"
        tries = conn.execute("""
            SELECT field_side FROM tries
            WHERE match_id = %s AND side = %s AND field_side != ''
        """, (mr["id"], scoring_side)).fetchall()
        for t in tries:
            fs = t["field_side"]
            if fs in edge_counts:
                edge_counts[fs] += 1

    conn.close()

    # Calculate league-wide edge rates for comparison
    total_conn = get_db()
    league_totals = total_conn.execute("""
        SELECT field_side, COUNT(*) as cnt
        FROM tries
        WHERE field_side != ''
        GROUP BY field_side
    """).fetchall()
    league_games_row = total_conn.execute("""
        SELECT COUNT(*) as cnt FROM matches WHERE match_state = 'FullTime'
    """).fetchone()
    total_conn.close()

    league_games = league_games_row["cnt"] if league_games_row else 1
    league_edge_rates = {}
    for r in league_totals:
        # Each match has 2 teams, so per-team rate = total / (games * 2)
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


def get_total_match_count() -> int:
    conn = get_db()
    row = conn.execute("SELECT COUNT(*) as cnt FROM matches WHERE match_state='FullTime'").fetchone()
    conn.close()
    return row["cnt"]


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
    """Search for players by name. Returns unique player entries with recent team/position."""
    conn = get_db()
    rows = conn.execute("""
        SELECT p.name, p.team, p.position, p.jersey_number,
               MAX(m.season * 100 + m.round_number) as latest_round,
               COUNT(DISTINCT m.id) as total_games,
               (SELECT COUNT(*) FROM tries t WHERE t.player_name = p.name) as total_tries
        FROM players p
        JOIN matches m ON p.match_id = m.id
        WHERE p.name LIKE %s AND m.match_state = 'FullTime'
        GROUP BY p.name, p.team, p.position, p.jersey_number
        ORDER BY latest_round DESC
        LIMIT %s
    """, (f"%{query}%", limit)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def search_teams(query: str) -> list:
    """Search for team names matching the query."""
    conn = get_db()
    rows = conn.execute("""
        SELECT DISTINCT home_team as name FROM matches
        WHERE home_team LIKE %s
        UNION
        SELECT DISTINCT away_team as name FROM matches
        WHERE away_team LIKE %s
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
