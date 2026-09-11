# Model review — 2026 season

A review of how the prediction model performed across the 2026 season, and the
enhancements to make before 2027 kicks off.

**Status: plan written, numbers pending.** The metrics and their thresholds are fixed
here *before* the harness runs, deliberately — so that a result cannot be chosen after
seeing which one flatters the change. Result tables below are empty and get filled in
by the backtest harness (§4).

Vocabulary in this document follows [`CONTEXT.md`](../CONTEXT.md). Related decisions:
[ADR-0006](adr/0006-parallel-model-versions.md) (parallel model versions),
[ADR-0007](adr/0007-full-history-retention-rolling-model-window.md) (retention vs
modelling window).

---

## 1. How the model performed in 2026

### 1.1 What is being measured

Four outputs, weighted by how much they matter:

| Rank | Output | What it is | 2026 sample size |
|---|---|---|---|
| 1 | **Try probability** | Per player, per match — the product | ~6,800 player-matches |
| 2 | **Betting edge picks** | `edge_picks` vs bookmaker odds, with real P/L | ~600 picks |
| 3 | **Top-3 / multi** | The ranking the site actually shows | ~1,200 picks |
| 4 | **Win probability** | Predicted winner and score | ~200 matches |

Note the sample sizes; they decide what may be tuned (§4.4). ~6,800 try predictions
comfortably supports fitting a handful of parameters. ~600 edge picks does not support
fitting anything — a few longshots landing swings ROI wildly.

### 1.2 Decision metrics

| Role | Metric | Rule |
|---|---|---|
| **Primary** | Log loss over every player in every match | Must improve |
| **Secondary** | Expected calibration error by probability bucket | Must improve or hold |
| **Guardrail** | Top-3 hit rate | Must not regress |
| **Guardrail** | Simulated ROI from `edge_picks` | Must not regress materially |

Log loss is primary because it punishes confident wrong calls, which is precisely the
failure mode an unshrunk low-evidence player creates. ROI is the outcome that actually
matters but is far too noisy over one season to *select* on — it vetoes a change, it
never picks one.

### 1.3 Results — 2026 headline

_To be filled by the harness._

| Metric | Champion (v3) | Challenger (v4) | Delta |
|---|---|---|---|
| Log loss (all players) | | | |
| ECE | | | |
| Top-3 hit rate | | | |
| Multi all-scored rate | | | |
| Win accuracy | | | |
| Simulated ROI | | | |

### 1.4 Results — stability check, 2020–2025

_To be filled by the harness._ A change must improve 2026 **and** not degrade earlier
seasons. This exists to catch a change that improved 2026 by luck — which matters here,
because the headline enhancement was motivated by a single observed match.

| Season | v3 log loss | v4 log loss | Delta | Matches |
|---|---|---|---|---|
| 2020 | | | | |
| 2021 | | | | |
| 2022 | | | | |
| 2023 | | | | |
| 2024 | | | | |
| 2025 | | | | |
| **2026** | | | | |

### 1.5 Calibration by probability bucket — 2026

_To be filled by the harness._ Unlike the current in-model calibration (§2.4), this
covers **all** players, not just the top-3 picks.

| Bucket | Predicted avg | Actual rate | n | Over/under |
|---|---|---|---|---|
| 0–10% | | | | |
| 10–20% | | | | |
| 20–30% | | | | |
| 30–40% | | | | |
| 40–50% | | | | |
| 50%+ | | | | |

---

## 2. Findings

### 2.1 No sample-size handling in the player try factor — **critical**

`_get_player_try_factor_from_history` (`backend/model.py:170`) returns `1.0` below 3
games, then computes a raw 60/40 recent/career blend with **no shrinkage toward the
position baseline**, clamped only to `[0.4, 2.5]`.

A player with 3 games and 2 tries is treated with nearly the same confidence as a
player with 120 games and 80 tries. The games count is sitting right there — `len(history)` —
and is simply unused.

Worse, the fallback for a low-evidence player is `1.0`, i.e. *exactly positional
average*. Since `_match_position` returns a rate computed across all players including
established stars, this quietly asserts that a debutant scores like an average NRL
player. That is not a neutral prior; it is a generous one.

**Observed consequence.** 2026 round 27, Rabbitohs v Roosters — the Roosters rested
their first-graders and named a largely inexperienced squad. Salesi Foketi was the
model's **#2 pick at 48.6%**, roughly 5x his position's ~10% base rate. He did not
score.

### 2.2 Probability ceiling saturation — **high**

`_predict_try_with_history` ends with `min(max(rate, 0.01), 0.60)` (`model.py:336`).
In that same round-27 match, **four of the six top-3 picks sat at exactly 60.0%** —
Johnston, Mitchell, Graham and Tupou. The model's most confident picks are pinned at
the clamp and **cannot be ranked against one another**.

This lands directly on betting edge picks, the second-ranked output: an edge is a gap
between model probability and implied probability, and a saturated probability makes
that gap meaningless at the top of the book.

The clamp is a symptom. The cause is that a chain of independent multipliers — base x
player x attack x defence x field-edge x home x weather x margin x rest x season — has
no business producing a probability. Each factor is individually clamped; their product
is not. The structural fix is combining factors in **log-odds space**, where a
probability cannot run off the end. Raising the clamp is a legitimate interim step and
will probably improve log loss on its own.

### 2.3 Look-ahead leakage in three query functions — **high**

Every query in the prediction path takes `before_season` / `before_round`, except three:

| Function | Location | Leaks |
|---|---|---|
| `get_position_try_rates()` | `database.py:726` | The starting value of every try probability |
| `get_venue_stats()` | `database.py:1777` | Venue win rates used in win probability |
| `get_calibration_data()` | `database.py:2081` | The calibration correction applied to every player |

`get_calibration_data` is a genuine **production** bug, not merely a backtest obstacle:
`_compute_match_detail` recomputes completed-match predictions when a user views them,
so a match viewed in December is scored using calibration that learned from that very
match. This one gets a real temporal filter. The other two are handled by injection in
the harness until the size of their effect is measured.

### 2.4 Calibration is censored and self-referential — **medium**

`get_calibration_data` builds its buckets **only from top-3 picks** — the
high-probability tail — then applies the resulting correction to *every* player,
including low-probability ones whose bucket has no supporting data. It also learns from
predictions that were themselves already calibrated, so the correction feeds back into
itself.

### 2.5 `isOnField` ignored — appearances overcounted — **medium**

NRL returns 19–20 players per squad and marks non-playing reserves `isOnField: false`
(in round 27, Roosters #18 Jake Elliott and #20 Lui Lee). The scraper drops the flag,
so every named reserve counts as a game played.

Two consequences: career games (§3) is inflated for exactly the fringe players this
work is about, and every try rate's **denominator** includes players who never took the
field — counted as "played and didn't score", quietly depressing rates.

Fixed by ADR-0007.

### 2.6 Name-based joins with no `player_id` — **medium**

Everything joins on `p.name`. NRL's `playerId` is read by the scraper (`scraper.py:242`)
and discarded. Verified stable across clubs and seasons (Latrell Mitchell is `502502`
in both 2018 and 2026) and present back to 2010. Fixed by ADR-0007.

### 2.7 Team form applied to a squad that did not generate it — **out of scope, recorded**

The loudest wrong signal in the round-27 match was not any player factor. It was team
form: the Roosters carried *"averaging 29 pts/game (last 5) — elite attack"* and Souths
*"conceding 26 pts/game — leaky defence"* into a match Souths won **50–20**. Those are
the top-listed positive factors on every Roosters pick. The model had the Roosters as
**52.2% favourites**, predicted 26–22.

`predict_win_probability` (`model.py:339`) takes team names and team stats only. **It
never looks at a player.** No per-player multiplier, however well calibrated, can move
that 52.2% by a single point.

**This is deliberately out of scope for 2027.** The consequence is stated plainly so it
is not rediscovered as a surprise: after this work ships, a team resting its
first-graders will still be priced on the form of the squad that did not play. The fix,
if taken up later, is a single derived *squad strength index* computed from the named
17, consumed by both the team form factor and win probability.

**A second consequence to keep in mind while reading §5:** because per-player is the
only lever in scope, the experience multiplier will partly absorb team-level error. Its
fitted values will read slightly high, and should not be interpreted as a pure estimate
of the effect of inexperience.

---

## 3. Worked design — the career-games experience multiplier

### 3.1 Definition

**Career games**: a player's count of Appearances (not Selections) up to, but not
including, the match being predicted. Across all clubs, not just the current one.
Point-in-time always.

The club split was considered and rejected: a 150-game veteran who signs elsewhere has
not become a rookie, and resetting the count on transfer would misprice every off-season
signing in round 1 — the exact moment the model most needs to be right.

### 3.2 Mechanism

Both mechanisms below are in scope, because §2.1 shows the shrinkage target itself is
biased:

1. **A confidence weight** — career games decides how far a player's observed try rate
   is pulled toward their position's base rate.
2. **A standalone multiplier** — because the shrinkage target (positional average) is
   itself too generous for an inexperienced player.

### 3.3 Where the numbers come from

**Measured, not guessed.** Query the retention span for actual try rate by career-games
bucket, expressed as a ratio to the position base rate:

```sql
-- Observed try rate by career-games bucket, relative to position base rate.
-- Run over the modelling window for rates; career games spans all history.
WITH appearance_seq AS (
  SELECT
    pa.player_id,
    pa.match_id,
    pa.position,
    m.season,
    m.round_number,
    ROW_NUMBER() OVER (
      PARTITION BY pa.player_id
      ORDER BY m.season, m.round_number
    ) - 1                                             AS career_games_before,
    (SELECT COUNT(*) FROM tries t
      WHERE t.match_id = pa.match_id
        AND t.player_id = pa.player_id)               AS tries_scored
  FROM player_appearances pa
  JOIN matches m ON m.id = pa.match_id
  WHERE m.match_state = 'FullTime'
    AND pa.is_on_field = TRUE
),
bucketed AS (
  SELECT
    position,
    CASE
      WHEN career_games_before < 1   THEN '0 (debut)'
      WHEN career_games_before < 6   THEN '1-5'
      WHEN career_games_before < 11  THEN '6-10'
      WHEN career_games_before < 21  THEN '11-20'
      WHEN career_games_before < 51  THEN '21-50'
      WHEN career_games_before < 101 THEN '51-100'
      ELSE '100+'
    END AS games_bucket,
    CASE WHEN tries_scored > 0 THEN 1.0 ELSE 0.0 END AS scored
  FROM appearance_seq
  WHERE season >= :modelling_window_start_season
)
SELECT
  games_bucket,
  COUNT(*)                AS appearances,
  AVG(scored)             AS observed_try_rate,
  AVG(scored) / NULLIF(
    (SELECT AVG(scored) FROM bucketed), 0)  AS ratio_to_overall
FROM bucketed
GROUP BY games_bucket
ORDER BY MIN(CASE games_bucket
  WHEN '0 (debut)' THEN 0 WHEN '1-5' THEN 1 WHEN '6-10' THEN 2
  WHEN '11-20' THEN 3 WHEN '21-50' THEN 4 WHEN '51-100' THEN 5 ELSE 6 END);
```

Run it per position as well as pooled — the effect is likely to differ between wingers
and props.

### 3.4 Results — measured curve

_To be filled._ The shape matters as much as the values: a cliff at debut implies a
different mechanism from a smooth curve out to ~30 games.

| Career games | Appearances | Observed try rate | Ratio to position base | Fitted multiplier |
|---|---|---|---|---|
| 0 (debut) | | | | |
| 1–5 | | | | |
| 6–10 | | | | |
| 11–20 | | | | |
| 21–50 | | | | |
| 51–100 | | | | |
| 100+ | | | | |

### 3.5 Fitting protocol

Fit on **2020–2025**, confirm on **2026** as an untouched test set. This is the one
parameter set worth fitting strictly out-of-sample: it originated in noticing a single
match, so it carries the highest risk of being a story rather than an effect.

### 3.6 The 2010 truncation, resolved

Career games was originally going to be truncated at 2020, making a 2017 debutant
indistinguishable from a 2020 one. ADR-0007's reseed to 2010 removes this: every player
active in 2026 debuted after 2010, so the count is complete for everyone the model
prices.

---

## 4. Backtest harness

### 4.1 Shape

Walk-forward replay. For each match, call the model with `before_season` /
`before_round` set to that match's round, so it sees only what it could have seen.
Every model and database function already accepts these parameters, which is what makes
this feasible without restructuring the model.

### 4.2 Leakage handling

Per §2.3:

- `get_calibration_data` — add a real temporal filter. Ships to production; it is a bug.
- `get_position_try_rates`, `get_venue_stats` — inject temporal variants in the harness
  only. Measure the size of the leakage before deciding whether to pay for a production
  change.

### 4.3 Evaluation span

2026 is the headline benchmark. 2020–2025 runs as a stability check. Pre-2020 is
excluded from all scoring — see ADR-0007 — and the per-season coverage marker decides
which seasons have the feature families a given run depends on.

### 4.4 What may be tuned, and against what

| Parameter | Tuned on | Rationale |
|---|---|---|
| Rolling window length (~36 months) | 2026 try log loss | ~6,800 observations supports it |
| Window decay | 2026 try log loss | Same |
| Shrinkage strength | 2026 try log loss | Same |
| Probability ceiling / log-odds params | 2026 try log loss | Same |
| Experience multiplier bands | **2020–2025, confirmed on 2026** | Originated from one match |
| Win probability weights | **Not tuned** | ~200 matches; judgement only |
| ROI / staking parameters | **Not tuned** | ~600 picks; guardrail only |

Sweeping ROI over 600 picks will reliably produce a beautiful, meaningless number. It
is a veto, never an objective.

---

## 5. Data layer changes

All specified in [ADR-0007](adr/0007-full-history-retention-rolling-model-window.md).
Summary of the work:

1. **Verify `playerId` stability** — done. `502502` for Latrell Mitchell across a 2018
   Roosters match and a 2026 Rabbitohs match; present on all 17 players in 2010
   payloads.
2. **Add `player_id`**, backfill via a targeted pass over match-detail endpoints.
3. **Introduce a `player` dimension table**; rename `players` to `player_appearances`.
4. **Persist `isOnField`**; count career games from Appearances only.
5. **Reseed to 2010** via the existing API scraper.
6. **Add a per-season coverage marker** recording which feature families are populated.

Open questions for the reseed to answer:

- The first season carrying `Interchange` timeline events (2010 has none).
- Whether `isOnField` is reliable on older seasons, or absent/always-true — if
  unreliable, a per-season fallback to the 1–17 jersey rule is needed.
- Actual database size after the reseed (est. ~2.5x current; free tier is 500MB).

---

## 6. Parallel models in 2027

Per [ADR-0006](adr/0006-parallel-model-versions.md):

- Champion `model_version = 3` is the only model users see.
- Challenger `model_version = 4` runs in the background prediction-sync loop and writes
  its own prediction rows.
- Comparison via `get_accuracy_stats`'s existing `by_model` grouping.
- Frontend: an **unlisted route** with no navigation link. `/api/accuracy` already
  serves `by_model` publicly, so this is tidiness, not access control.
- Promotion happens only when the challenger beats the champion on §1.2's metrics over
  a full season.

---

## 7. Backlog

Ordered. Tracking issue: **#2**.

| Issue | Item | Why | Effort |
|---|---|---|---|
| #3 | `player_id` + player dimension table | Blocks trustworthy career-games counts | M |
| #4 | Persist `isOnField` | Career games and every rate denominator are wrong without it | S |
| #5 | Reseed to 2010 | Complete career history | M |
| #6 | Backtest harness + leakage fixes | Nothing below can be validated without it | L |
| #7 | Career-games experience multiplier | The headline enhancement | M |
| #8 | Log-odds factor combination (ceiling fix) | Restores ranking among top picks | M |
| #9 | Rolling modelling window | Removes stale-era data from rate estimation | S |
| #10 | Rebuild calibration on all players | Currently censored and self-referential | M |
| #11 | Parallel models + unlisted compare route | Confirms 2027 is an improvement | M |
| #12 | 2010+ stats browsing on the site | Unlocked by the reseed; scope separately | M |

### Sequence

```
verify playerId ✅ → provisional fit on unambiguous names → player_id backfill
   → isOnField → 2010 reseed → harness + leakage fixes → final multiplier fit
   → log-odds restructure → walk-forward validation → 2027 shadow run
```

The provisional fit runs early and deliberately on dirty data, restricted to players
with unambiguous names. It answers the one question that determines whether the rest of
the work is worth doing: is there a real experience effect, and is it a debut cliff or a
smooth curve?

---

## 8. Deliberately out of scope

- **Squad-aware team form and win probability** (§2.7). The round-27 Roosters match
  stays mispriced at the team level after this work. Recorded so it is a known
  limitation, not a future surprise.
- **Separate club-tenure factor.** Considered; rejected as heavily correlated with
  career games and likely to double-count it.
- **Scraping NRL player profile pages** for career totals. Superseded by the API reseed
  — see ADR-0007's considered options.
- **Backfilling seasons before 2010.** The API lists seasons to 1908, but no player
  active in 2026 debuted before 2010.
