# AGENTS.md

This file provides guidance to AI coding agents (Claude Code, etc.) when working with code in this repository.

## What this is

A prediction site for who will score tries in NRL (rugby league) matches. FastAPI backend (Python) + React/Vite frontend, backed by a Postgres (Supabase) database of historical match data scraped from the public NRL website API. Deployed on Render (`render.yaml`); local dev uses Docker Compose.

## Commands

### Backend (from `backend/`)
```
pip install -r requirements.txt
uvicorn main:app --reload --port 8000        # dev server
python scrape_job.py                         # one-shot: full historical scrape if DB empty, else incremental sync
python -c "from scraper import run_scraper; run_scraper()"          # force full historical scrape (2020-2026)
python -c "from scraper import run_sync_current_season; run_sync_current_season()"  # sync current season only
```
There is no test suite or linter configured for the backend.

### Frontend (from `frontend/`)
```
npm install
npm run dev        # Vite dev server, proxies /api -> http://backend:8000 (see vite.config.js)
npm run build       # production build to frontend/dist
npm run preview
```
No test suite or linter configured for the frontend.

### Docker Compose (full stack, from repo root)
```
docker compose up --build
```
Backend on :8000, frontend (nginx) on :3000. Requires `ODDS_API_KEY` and `DATABASE_URL` env vars (both optional — app degrades gracefully without them).

### Data sync cadence
The backend runs `_scrape_sync()` (`main.py`) every 30 minutes — a background task, started at startup, that calls `scraper.py`'s `sync_current_season()` directly in-process and invalidates the query cache if it found anything new. This is what keeps the production DB current; see `docs/adr/0005-in-process-scrape-sync.md` for why it isn't an external GitHub Actions cron (short version: that cron proved unreliable — GitHub silently auto-disables a `schedule:` trigger after 60 days of repo inactivity, with no self-recovery, and setting that aside its schedule trigger is best-effort with no SLA anyway). `.github/workflows/scrape.yml` still exists but is `workflow_dispatch:`-only now — manual use for bootstrapping an empty DB or forcing a re-scrape, not ongoing sync. The backend also runs a separate background prediction-sync loop (not a scrape) every 10 minutes to backfill accuracy tracking for newly-completed matches.

## Architecture

### Data flow
1. **Historical data** lives in Postgres (`nrltp` schema — see `database.py`): `matches`, `players`, `tries`, `interchanges`, `predictions`, `cache_store`, `scrape_progress`. Populated by `scraper.py` (via the cron job), scraping `https://www.nrl.com/draw/data?...` and per-match `.../data` endpoints for 2020-2026.
2. **Live fixture/team-list data** is fetched on-demand from the same NRL endpoints via `nrl_client.py` (not persisted beyond a 60s in-process cache) — this is what powers "who's playing this round" and team lists before a match starts. NRL fronts these endpoints with an anonymous SSO probe cookie that a persistent client must retry once to get past — see `docs/adr/0004-nrl-sso-cookie-persistent-client.md` before touching `nrl_client.py` or `scraper.py`'s HTTP client setup.
   - **Round count**: `TOTAL_ROUNDS` in `nrl_client.py` (31 = 27 regular-season + 4 finals weeks), `CURRENT_SEASON_ROUNDS` in `scraper.py`, and `TOTAL_ROUNDS` in `frontend/src/components/Draw.jsx` must all stay in sync — none of them are derived from the others. NRL's API doesn't error past the real end of season, it just keeps echoing the latest available round.
   - **Undrawn rounds** (e.g. Finals Weeks 2-4 before NRL has published who's playing) come back from NRL as a byte-for-byte echo of the last real round, not an error. `main.py`'s `_is_undrawn_echo()` detects this (same `match_url`s as `round_number - 1`'s cached fixtures) and caches an empty placeholder instead of showing duplicated matches under the wrong round. That placeholder is treated as "live" for cache-TTL purposes (2 min, not 30) specifically so it gets rechecked often and picks up the real draw promptly once NRL publishes it.
3. **The prediction model** (`model.py`) combines historical DB stats with the live team list to produce a try probability per player, and a win probability per team. It queries `database.py` for team form, H2H, venue stats, edge vulnerability, player try history, etc.
4. **`main.py`** (FastAPI) wires it together per route: fetch live fixture/match data -> pull team form from DB -> run model -> return JSON. It also opportunistically writes completed-match predictions back to the `predictions` table for accuracy tracking, and backfills player headshots it discovers.
5. The frontend (`frontend/src`) is a thin client that calls `/api/*` and renders — `App.jsx` has the route table, one component per page under `src/components/`.

### `backend/data/` (fixtures.py, players.py, teams.py)
Static/legacy mock data (hardcoded 2026 squads and fixtures). **Not imported by any live code path** — the app now sources all fixture/roster data live from the NRL API (`nrl_client.py`) and history from Postgres. Leave alone unless explicitly asked to clean it up.

### Model: V3 only
`model.py` runs a single model (formerly "V3" of three side-by-side versions — V1/V2 were deleted, see `docs/adr/0001-single-model-version.md`). There is no `model_version` parameter anywhere in `model.py`'s public functions; predictions always use the full factor set (recency-weighted form, edge vulnerability, venue-specific home advantage, weather, margin-of-victory weighting, rest/bye-week adjustments, season progression, opponent-quality-adjusted try rates, calibration). The `version` query param on `/api/rounds/{n}` and `/api/match` is still accepted (for old bookmarked links) but ignored. `predictions.model_version` is still written as `3` on every insert — the column stayed to avoid a migration and because old rows with `model_version` 1/2 remain queryable history; don't reintroduce per-request version branching without re-reading that ADR. The full factor list (with weights) used to be a `/models` in-app page; it's now `docs/model-factors.md` — update that doc, not a component, when factors change.

### Caching layers (three distinct ones — know which one you're touching)
- `database.py`: in-process TTL cache (`_query_cache`, 30 min) in front of every DB query function, keyed by function name + args. `prefetch_round_data()` bulk-loads/primes this cache for a whole round in ~2 queries instead of N+1 per-team queries — always prefer extending this bulk path over adding new per-team queries in a loop. `invalidate_cache()` / `clear_query_cache()` must be called after any scrape. The connection pool sets `connect_timeout=10` so a slow/unreachable DB can't block the app from starting to serve requests indefinitely.
- `nrl_client.py`: 60s in-process cache on raw NRL API responses (`_nrl_api_cache`), to survive bursts of requests for the same round.
- `main.py` `_round_cache`: the assembled `/api/rounds/{n}` response (fixtures + predictions), keyed by `round_number` alone, TTL 2 min while a round has live/upcoming matches or 30 min once fully completed, refreshed asynchronously (stale-while-revalidate) and persisted to the `cache_store` table so it survives process restarts. `_restore_cache_from_db()` reloads it on startup.

### Prediction/accuracy tracking
`predictions` table records what the model predicted vs. what actually happened, per `(match_url, model_version)` — `model_version` is always `3` for new rows now. Written from two places: `_record_prediction_for_match` (background backfill loop, `_prediction_sync`) and `_compute_match_detail` (when a user views a completed match). Both must stay in sync in what they compute/store if one changes — `/api/accuracy` reads this table directly.

### Frontend fetch timeout
Every frontend data fetch goes through `frontend/src/api.js`'s `fetchJson()`, which aborts after 45s and throws a descriptive error instead of hanging forever. Plain `fetch()` has no built-in timeout — a component that bypasses `fetchJson` and calls `fetch()` directly reintroduces the "infinite spinner" failure mode (see `docs/adr/0002-frontend-fetch-timeout.md`). Always use `fetchJson` for new API calls, and give the component an error state with a retry action.

### Unhandled exceptions must go through the global handler
`main.py` registers `@app.exception_handler(Exception)`. Without it, an unhandled exception in a route bypasses `CORSMiddleware`'s header injection entirely, and the browser reports a misleading "CORS blocked" error instead of the real 500 — this is a known FastAPI/Starlette gotcha, not a CORS config problem, and it's easy to burn time debugging the wrong thing if you don't know it. The handler logs the real exception (`exc_info=True`, mirrored to `app_logs`) and returns a normal JSON 500 that keeps its CORS headers. Don't remove it, and don't add other bare `except Exception: pass` swallows in route handlers that would prevent it from firing.

### Render free-tier cold starts
The backend (`render.yaml`) runs on Render's free plan, which sleeps after 15 min of inactivity; the first request after sleep can take 30-60s+. This is mitigated by an external UptimeRobot ping hitting `/api/health` — if the app seems to hang for a long time on first load, check that the UptimeRobot monitor is still active and pointed at `/api/health` specifically (Render intercepts `/` and `/robots.txt` for a sleeping service without waking it, so pinging those does nothing).

### DB init doesn't block startup
`init_db()` runs as a background task (`_startup_sequence()` in `main.py`) that retries every 30s if it fails, rather than blocking `lifespan()` before the app binds its port. This matters because Supabase's free tier can reject connections outright (e.g. `FATAL: tenant/user ... not found` when the project is paused) — see `docs/adr/0003-nonfatal-db-init-at-startup.md`. If the site is fully down (no port bound, Render deploy shows "No open ports detected"), check Supabase's dashboard for a paused project or a stale `DATABASE_URL` in Render's env vars before assuming it's a code issue — the app is designed to degrade (DB-dependent endpoints error) rather than crash-loop when the DB is unreachable, so a total outage points at infra/credentials, not the app itself.

### Logs mirrored to Supabase
`log_handler.py`'s `SupabaseLogHandler` attaches to the root logger and writes every INFO+ log record into the `app_logs` table, so logs are browsable directly in Supabase (Table Editor / SQL Editor) rather than depending on Render's log retention. Attached once per process, after `init_db()` (the table must exist first): in `main.py`'s `lifespan()` with `source="backend"`, and in `scrape_job.py`'s `main()` with `source="scraper"` — filter on that column to tell the two apart. `prune_old_logs()` runs at the same point in both, deleting rows older than 14 days to bound table growth. The insert runs synchronously wherever the log call happens (deliberate — log volume is low, see the docstring); never add a `logger.info`/`logger.warning` call inside a genuinely hot per-request path without considering that it now costs a DB write.

### Field-side / edge attribution
Jersey numbers 1-13 map to a field side (`left`/`right`/`middle`/`fullback`) via `JERSEY_FIELD_SIDE` (duplicated in `model.py` and `scraper.py` — keep in sync). For bench players (14+), `scraper.py`'s `_determine_field_side` walks interchange timeline data to infer which starter's edge they inherited. This underpins the "edge vulnerability" factor in the model and the value-picks logic.

## Agent skills

### Issue tracker

Issues live as GitHub issues in anthonyxar/nrl-try-predictor. See `docs/agents/issue-tracker.md`.

### Domain docs

Single-context layout (root CONTEXT.md + docs/adr/). See `docs/agents/domain.md`.
