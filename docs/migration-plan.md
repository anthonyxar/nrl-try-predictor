# Migration plan — single-box Sydney deployment

Status: **agreed, not started.** Design settled 2026-09-12. Cutover deliberately deferred
until after the 2026 Grand Final.

Replaces the current two-vendor free-tier setup (Render free web service + static site,
Supabase free Postgres) with one paid DigitalOcean droplet in Sydney running the whole
stack under Docker Compose.

To start the work, see `docs/migration-kickoff-prompt.md`.

---

## 1. Why

The free tier is paid for in latency and reliability rather than money:

- **Cold starts.** Render's free plan sleeps after 15 minutes; the first request after
  sleep takes 30-60s. This is currently papered over with an external UptimeRobot ping
  whose only job is to stop the process sleeping.
- **A pausing database.** Supabase's free tier can refuse connections outright
  (`FATAL: tenant/user ... not found`). ADR-0003 exists solely because that happened.
- **No Oceania region, on either vendor.** Render offers Oregon, Ohio, Virginia,
  Frankfurt and Singapore — there is no OCE option at any price tier, so "upgrade Render"
  cannot satisfy the latency goal. Every request from an Australian user currently crosses
  the Pacific, then the backend crosses the network again to reach Postgres.
- **Two vendors, two dashboards, two failure modes** for one small site.

## 2. Target architecture

One DigitalOcean droplet, **SYD1, 2 GB / 1 vCPU, ~US$12/mo** (verify current pricing at
purchase). Docker Compose runs:

| Container | Role | Exposure |
|---|---|---|
| `backend` | FastAPI / uvicorn | internal only |
| `db` | `postgres:16-alpine` | **no published port at all** |
| `web` | nginx serving `frontend/dist`, proxying `/api/` | 80/443, Cloudflare-only |
| — | no pgweb/Studio — DBeaver over SSH tunnel | — |

Cloudflare sits in front of the droplet for free TLS and CDN caching of static assets.
The origin firewall allows 80/443 **only from Cloudflare's published IP ranges**, plus 22
for SSH with keys. The droplet's public IP is therefore not directly reachable.

Total running cost: **~US$12/mo** plus a domain (~US$12/yr). Cloudflare and R2 stay within
their free tiers at this scale.

### What this fixes for free

`frontend/nginx.conf` already proxies `location /api/` to `http://backend:8000`, so the
frontend and API become **same-origin** with no new code. Consequences:

- `VITE_API_BASE` and its hardcoded `onrender.com` value are deleted.
- `allow_origins=["*"]` in `main.py` can be tightened or dropped.
- App-to-DB latency drops from a cross-network hop to the Docker bridge. This matters more
  than user latency: a single match-detail response fans out to several sequential DB
  calls, so DB round-trip cost is multiplied per request.

## 3. Decisions, and what was rejected

### Vercel — rejected

The backend is a **stateful long-lived process**, which Vercel Functions are not.
`main.py` starts three background loops at startup (`_scrape_sync` every 30 min,
`_prediction_sync` every 10 min, `_warm_cache`) and depends on three in-process caches
(`database.py:_query_cache` 30 min, `nrl_client.py:_nrl_api_cache` 60 s,
`main.py:_round_cache` with stale-while-revalidate). Porting to Vercel means rewriting the
loops as Vercel Cron, relocating all three caches to the DB or Redis, and accepting a cold
start on every idle invocation — i.e. paying money to reacquire the exact problem this
migration exists to remove. Vercel also no longer sells first-party Postgres; it resells
Neon via its marketplace, so the single-vendor goal would not hold either.

### Fly.io (app Machine + Fly Managed Postgres, one bill) — rejected

Genuinely viable, and has a `syd` region. Rejected because it is two priced resources
rather than one, Fly Machines have auto-stop semantics that must be explicitly disabled
(the thing we are migrating away from), and it buys a platform abstraction this app does
not need — there is no disk requirement and no horizontal scaling.

### Managed Postgres in Sydney — rejected on price

The cheapest managed Postgres in SYD1 is ~US$15/mo (DO), Neon's Sydney paid tier ~US$19.
Either one **costs more than the entire droplet budget** and reintroduces a second vendor
and bill. The escape hatch stays open: DO offers managed Postgres in-region, so if the
self-hosted DB ever becomes the bottleneck, moving it is a DSN change — provided the DB is
addressed by **hostname, never `localhost`** (task B3).

### Self-hosting Supabase — rejected

Supabase's self-host stack is ~13 containers (`db`, `studio`, `kong`, `auth`, `rest`,
`realtime`, `storage`, `imgproxy`, `meta`, `functions`, `analytics`, `vector`,
`supavisor`). This app uses exactly one of them: Postgres. There is no `supabase` SDK in
the codebase — `database.py` talks to a plain `DATABASE_URL` via `psycopg2`, so the only
Supabase-specific thing lost is browsing `app_logs` in the Table Editor.

Running twelve extra containers to keep a web table browser needs ~4 GB to be stable (the
`analytics`/Logflare container is the usual OOM culprit; `vector` wants the Docker socket),
pushing the droplet to ~US$24/mo — **doubling the budget for one feature** — while adding
published-by-default JWT/anon/service keys that must be rotated and a Kong-exposed Studio
to patch. DBeaver over an SSH tunnel covers the same need with zero containers and zero
internet exposure.

### Oracle Cloud Always Free (Sydney/Melbourne) — rejected

Meets every stated requirement on paper: OCE region, never sleeps, free forever, 4 ARM
cores. Rejected because ARM capacity in `ap-sydney-1` is frequently unavailable to free
accounts, Oracle reclaims idle free instances, free accounts are terminated without appeal,
and there is no SLA. The current pain is *caused* by free-tier pathologies; swapping to a
different free tier buys a different and less predictable set of them. US$12/mo is the
price of the problem actually going away.

### Staging environment — rejected in favour of real local dev

A `develop`-branch staging stack on the same droplet would put a second `_scrape_sync` loop
alongside production — by design, the exact double-scrape hazard task B4 exists to prevent.
A second droplet doubles cost for a site with one maintainer. Instead the local Docker stack
becomes the staging environment (Phase A), seeded from the real nightly backup so it is
representative rather than a toy.

### Horizontal scaling — explicitly out of scope

**The app cannot currently run more than one replica**, and this is an application
constraint, not an infrastructure one. Two instances would run two `_scrape_sync` and two
`_prediction_sync` loops concurrently against the same upserts, and the three in-process
caches would diverge per replica so identical requests return different data depending on
which instance is hit, with `cache_store` restore racing between them.

Scaling is therefore **vertical**: DO resize is one click and ~60s of downtime, reversible
for CPU/RAM. Behind `_round_cache` and Cloudflare most requests never reach Postgres at
all, so a 2 GB box has substantial headroom. Two cheap hedges are taken now because they
are painful to retrofit (tasks B3, B4); the rest of multi-instance support is not built
until traffic demands it.

### ADR-0005's in-process scrape loops — kept, unchanged

Moving the scrape to host cron would be tidier on a box that never sleeps, but
`_scrape_sync` calls `invalidate_cache()` **in the same process** whose `_query_cache`
needs invalidating the moment new data lands. Host cron would need a new mechanism to tell
the running app its cache is stale. ADR-0005's premise only improves here: the loops no
longer compete with cold starts or a sleeping dyno. Amend the ADR with a note; don't change
the design.

### ADR-0003's degrade-don't-die startup — kept; UptimeRobot repurposed

The background-retry `init_db()` stays. Its original trigger (a paused Supabase project)
disappears, but `depends_on` only orders startup — it does not prevent the `db` container
restarting mid-life, so the retry loop remains cheap insurance.

The UptimeRobot monitor's **purpose inverts**: it stops being a keep-warm hack and becomes
genuine alerting. Point it at `/api/health` with notifications enabled, and treat a missed
ping as a real incident rather than normal background noise.

## 4. Work breakdown

Each task below is scoped to be delegated to **one sub-agent**, verified, then committed
before the next starts. Do not bundle phases into a single change — the point of the
breakdown is that a mistake in Phase C cannot contaminate Phase A.

Phases A-G carry **zero production risk**: the existing Render/Supabase stack keeps serving
throughout. Only Phase H touches production.

### Phase A — Local development stack (do first; highest value, no risk)

This is the phase that stops you testing in production, which was the stated motivation.

- **A1 — Add Postgres to Compose.** `postgres:16-alpine` service in `docker-compose.yml`,
  named volume, healthcheck, `depends_on` from `backend`. **No `ports:` mapping** — the DB
  is reachable only on the Docker network. Note `docker-compose.yml` currently has no
  database service at all, so local dev today runs against remote Supabase or degraded with
  no DB.
- **A2 — `DB_SSLMODE` carve-out.** `database.py:84-86` unconditionally appends
  `sslmode=require`, which fails against a container with no certs. Make it an env var
  defaulting to `require` so production behaviour is unchanged and local can set `disable`.
- **A3 — `ENABLE_SYNC_LOOPS` gate.** `_scrape_sync` and `_prediction_sync` must not start
  unless explicitly enabled. Today, running the backend locally against a production
  `DATABASE_URL` writes to production from your laptop. Default **off**; production sets it
  true.
- **A4 — Dev override file.** `docker-compose.override.yml` (git-tracked, dev-only): vite
  dev server with HMR, backend on `--reload` with source bind-mounted, Postgres from A1.
  `docker-compose.yml` stays prod-shaped.
- **A5 — Fix the vite proxy inconsistency.** `vite.config.js` proxies to
  `http://backend:8000`, a *container* hostname, so the `npm run dev` instruction in
  AGENTS.md only works inside Compose. Make it work in both contexts, or correct the docs.
- **A6 — Seed script.** One command that fetches the latest nightly dump and `pg_restore`s
  it into local Postgres. Depends on Phase E existing; until then, accept a manually-placed
  dump file.

### Phase B — Production topology

- **B1 — Prod Compose file.** nginx `web` container serving `frontend/dist` and proxying
  `/api/`; `backend` **not** publicly published. Restart policies (`unless-stopped`) and
  Docker enabled at boot so a reboot self-heals.
- **B2 — Same-origin cleanup.** Delete `VITE_API_BASE` and its `onrender.com` value;
  tighten `allow_origins` in `main.py` from `["*"]`.
- **B3 — DB addressed by hostname.** `DATABASE_URL` must name the Compose service, never
  `localhost`, so relocating Postgres later is a DSN change.
- **B4 — Advisory lock on the sync loops.** Wrap `_scrape_sync` and `_prediction_sync` in a
  `pg_advisory_lock` so a future second replica cannot double-scrape. ~10 lines, and the
  one multi-instance concern that genuinely hurts to retrofit after being bitten.

### Phase C — Provisioning and hardening

- **C1 — Idempotent `provision.sh` in the repo.** Non-root sudo user; SSH keys only with
  password auth disabled; `ufw` allowing 22 plus 80/443 **restricted to Cloudflare's IP
  ranges**; `unattended-upgrades` for security patches, configured **not** to auto-reboot
  during the day; Docker from the official repo; `fail2ban`.

  This script is load-bearing, not a convenience: §6 chose dumps over droplet snapshots, so
  "I broke the OS" recovery means re-provisioning from scratch. If it isn't scripted, it
  isn't recoverable.

### Phase D — CI/CD

- **D1 — Deploy workflow.** On push to `main`: build images in CI (not on the box — a 2 GB
  droplet will OOM on `npm install`), push to a registry, SSH in, `docker compose pull &&
  up -d`. CI holds **one** secret: the SSH deploy key.
- **D2 — Branch protection.** `main` protected; work happens on feature branches via PR.
- **D3 — Rewrite `scrape.yml`.** It currently runs `scrape_job.py` on a GitHub-hosted
  runner using `secrets.DATABASE_URL`, which **cannot reach a Postgres with no published
  port**. Convert it to SSH in and run `docker compose exec backend python scrape_job.py`.
  Keep the `workflow_dispatch` trigger — this is the bootstrap path for an empty DB, which
  is exactly the state the new box starts in.

### Phase E — Backups and secrets

Secrets live in a **root-owned `chmod 600` `.env` on the box**, never in git and never in
GitHub Actions secrets. Keep a copy in a password manager — §6 means no snapshot holds one
for you.

- **E1 — Nightly `pg_dump -Fc` to Cloudflare R2.** 14-day retention, via cron or a systemd
  timer.
- **E2 — Post-round dump.** An extra dump once a round's matches have locked in.
  `edge_picks` rows are written **once, pre-kickoff, and never again**
  (`database.py:316-320`), so a nightly-only RPO of up to 24h can permanently lose
  bookmaker odds that cannot be repurchased at any price.
- **E3 — Rehearsed restore.** Restore into the local stack and confirm it works. An
  untested backup is not a backup, and it doubles as A6's seed path.

### Phase F — Parity harness

- **F1 — Scripted old-vs-new comparison**, run before the flip:
  - per-table row counts against the source DB;
  - `/api/accuracy` returning identical numbers from both stacks;
  - a JSON diff of `/api/rounds/{n}` between old and new for **a regular round, a finals
    week, and an undrawn round** — finals URL parsing (`finals-week-N` vs `round-N`) and
    `_is_undrawn_echo()` are known-fragile per AGENTS.md, and the finals regex has broken
    the nav bar once already;
  - run with caches both cold and warm.

### Phase G — Documentation

- **G1 — ADR-0008**, recording this decision and the rejected alternatives in §3. §3 is
  written to be the source for it — copy the reasoning rather than re-deriving it.
- **G2 — Amend ADR-0003 and ADR-0005.** 0003: original trigger gone, design stands,
  UptimeRobot repurposed to alerting. 0005: premise improved, design unchanged.
- **G3 — Rewrite `AGENTS.md` and `README.md`.** The Render free-tier cold-start section, the
  Supabase log-browsing section, the UptimeRobot note and the deployment description all
  become wrong. Delete `render.yaml` only at the end of Phase H.
- **G4 — `docs/runbook.md`**: provision, deploy, restore, rollback, and the §5 cutover
  sequence. Must exist *before* cutover, not after.

### Phase H — Cutover (after the Grand Final only)

See §5. Manual, gated, and not started until Phases A-G are complete and rehearsed.

## 5. Cutover sequence

**Timing is the largest single risk in this migration.** `edge_picks` rows are written
once, pre-kickoff, and never again. Any finals match kicking off mid-migration whose write
lands in the database being abandoned is a permanent, unrepairable hole in the Dashboard's
P/L history. Finals is also peak traffic for the year.

So: build everything during the season, flip **after the Grand Final**, into the quietest
months of the year, with a fully rehearsed box.

Migration is **two-phase**:

1. **Bulk restore during the season (rehearsal).** `pg_dump -Fc` of the `nrltp` schema from
   Supabase, restored onto the droplet, so Phases A-F are tested against real data. Exclude
   `app_logs` and `cache_store` — both are disposable (pruned at 14 days / rebuilt on
   startup) and only inflate the dump.
2. **Final delta restore at flip**, below.

The flip, in order — **the first step is the one that is easy to get wrong**:

1. **Stop the Render backend.** While it is alive, `_scrape_sync` and `_prediction_sync`
   keep writing, so any dump taken with it running is stale before it finishes.
2. Final `pg_dump` from Supabase.
3. Restore onto the droplet.
4. Run the Phase F parity harness. Do not proceed on a failure.
5. Point DNS at the droplet via Cloudflare.
6. Verify live, with `/api/health` alerting armed.

### Rollback, and its sharp edge

Rollback is a DNS change plus un-suspending Render — **but only for about a day.** Once the
new box has been live and scraping, the *old* database is the stale one, so rolling back
later is a data merge, not a revert. Another reason to flip in the off-season, when days of
staleness cost nothing.

### Decommissioning

Park both old services for **~2 weeks** after the flip: Render services suspended but not
deleted, Supabase project alive and read-only, `render.yaml` still in the repo. That window
covers a bug surfacing under real traffic that the parity harness missed. Then delete all
three and remove `render.yaml`.

## 6. Data inventory

The nine tables in `nrltp` fall into three tiers with very different stakes:

| Tier | Tables | Why |
|---|---|---|
| **Irreplaceable** | `predictions`, `edge_picks` | Point-in-time records. Cannot be regenerated at any price. |
| **Re-scrapable** | `matches`, `players`, `tries`, `interchanges`, `scrape_progress` | Free from the NRL API — just slow (2020-2026). |
| **Disposable** | `cache_store`, `app_logs` | Rebuilt on startup / pruned at 14 days. |

`edge_picks` is the highest-stakes table in the database and the easiest to overlook. It
stores `bookmaker_decimal_odds` and `bookmaker_name` captured pre-kickoff specifically so
the Dashboard's P/L simulation is free of look-ahead bias. Historical odds are not
purchasable after the fact. Every backup and verification decision in this plan is shaped by
that table, not by the much larger re-scrapable ones.

**Backups: nightly `pg_dump -Fc` to Cloudflare R2, plus a post-round dump. No droplet
snapshots.** Accepted consequence: whole-box recovery means re-provisioning from the Phase C
script rather than restoring an image. PITR/WAL archiving was considered and rejected as
over-engineering for a database whose bulk is free to re-fetch.

## 7. Open items requiring your action (not an agent's)

- Register a domain and add it to Cloudflare.
- Create the DigitalOcean droplet in SYD1 and add your SSH key.
- Create a Cloudflare R2 bucket and API token for backups.
- Reconfigure the UptimeRobot monitor for alerting (notifications on, `/api/health`).
- Confirm current pricing — figures here are approximate and were not verified live.
