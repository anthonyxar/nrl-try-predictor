# Store history back to 2010, but model on a rolling recent window

The database is being reseeded from NRL's API back to **2010** (previously 2020), and
at the same time the model is being restricted to estimating every rate, form figure
and probability from a **rolling recent window** of roughly the last 36 months. These
sound contradictory and are not: they answer different questions. "How experienced is
this player?" wants every season a player has ever played. "How do players score tries
now?" wants only seasons resembling the present game — the six-again rule alone
reshaped scoring in 2020, and 2012 tries carry almost no signal about 2027.

**How far back we store and how far back we model on are two separate, independently
tunable decisions.** Anyone who "fixes" the model to use all the data we now hold will
make it worse.

## Status

Accepted.

## The one exception

Career games is the single model input drawn from the full retention span. A 2010
appearance therefore does still reach the model — as a count, never as a rate. This
means old-season *appearance* accuracy matters (including `isOnField`, below) even
though old-season scoring data does not.

## What the reseed changes structurally

**`player_id` becomes the join key.** Everything currently joins on `p.name`, which is
fragile across six seasons of name-format drift and outright collisions. NRL's
`playerId` was already read by the scraper and discarded. It is verified stable across
clubs and seasons — Latrell Mitchell is `502502` in both a 2018 Roosters match and a
2026 Rabbitohs match — and is present on every player object back to 2010.

**A player dimension table is introduced**, because none existed. The `players` table
is one row per player-per-match; there was nowhere to record a player's identity. It is
renamed `player_appearances` to say what it holds, and a `player` table keyed on
`player_id` holds identity. See `CONTEXT.md` for the Player / Appearance / Selection
distinction this enforces.

**`isOnField` is persisted.** NRL returns 19–20 players per squad, of whom some never
take the field (in 2026 round 27, Roosters #18 and #20 were `isOnField: false`). The
scraper ignored this, so every named reserve counted as a game played — inflating
career games, and quietly depressing try rates by adding non-playing "appearances" to
every rate's denominator. Stored because it cannot be recovered later without another
full re-scrape.

## Considered options

**Seed career totals by scraping NRL player profile pages.** Their "Career By Season"
table gives club, year, games and tries back to debut. Rejected: it is HTML rather than
JSON, so it breaks on any site redesign, it exposes no `playerId` to join on, and a
full API reseed to 2010 gets round-level granularity for every current player instead
of season-level totals — using proven tooling and no new failure mode.

**Accept the 2020 truncation** and treat pre-2020 career games as censored. Rejected
once it was established that the existing draw and match-detail endpoints serve data
back to 2010 (and list seasons to 1908) with `playerId` intact.

**Backfill `player_id` via a full re-scrape** rather than a targeted pass. Rejected:
same network cost, larger blast radius — a re-scrape rewrites tries, interchanges and
field-side attribution that are already correct and validated.

## Consequences

- Old seasons are materially thinner. 2010 match payloads carry **no `Interchange`
  timeline events** and **zeroed try minutes**. Bench minutes and bench field-side
  attribution therefore have no input for early seasons. A per-season coverage marker
  records which feature families are populated, so the backtest harness can pick a
  valid evaluation span rather than silently scoring 2012 with fabricated bench
  minutes. The first season carrying `Interchange` events must be established during
  the reseed.
- NRL rewrites venue names to current branding even for old matches (a 2010 Parramatta
  home game returns "CommBank Stadium"), so venue-keyed statistics pool a ground with
  its predecessors.
- The reseed is safe for prediction history: the re-scrape path deletes only by
  `match_id`, `matches` upserts with `ON CONFLICT DO NOTHING RETURNING id` so match ids
  are stable, and `predictions` / `edge_picks` key on `match_url` with no foreign key.
  All 2026 predictions survive.
- Roughly 2.5x the current row count. Well inside Supabase's free-tier limit, but worth
  measuring rather than assuming.
