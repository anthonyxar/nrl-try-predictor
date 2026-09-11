# NRL Try Predictor

A prediction site for who will score tries in NRL matches. This glossary fixes the
vocabulary the codebase and its docs use, so the same word means the same thing in
`model.py`, in the database schema, and in every issue and ADR.

## Language

### People

**Player**:
One human being who has played NRL, identified for all time by NRL's `playerId`.
Stable across clubs and seasons — Latrell Mitchell is `502502` whether he is at the
Roosters or the Rabbitohs.
_Avoid_: Person, athlete. Never use "player" to mean a single game's team-list entry.

**Appearance**:
One player taking the field in one match. The unit of history: a player has hundreds,
a match has ~34. This is what the `players` table has always actually stored, despite
its name.
_Avoid_: Game (ambiguous — see below), team-list entry, selection.

**Selection**:
A player being named in a match's team list. Not the same as an Appearance — a named
reserve who never took the field was selected but did not appear. NRL's `isOnField`
flag distinguishes them.
_Avoid_: Naming, squad inclusion.

**Squad**:
The set of players named for one team in one match, typically 17 starters and bench
plus 1–3 reserves. Distinct from a club's full roster.
_Avoid_: Team (a Team is the club; a Squad is who it named this week), lineup, roster.

**Career games**:
A player's count of Appearances up to, but not including, a given match. Always
point-in-time — "career games" without a reference match is meaningless. Counts
Appearances at every club, not just the current one, and excludes Selections that
were not Appearances.
_Avoid_: Games played, caps, experience (reserve "experience" for the model factor
derived from this count).

### Prediction

**Try probability**:
The model's estimate that a given player scores at least one try in a given match.
Per player, per match. Not a count of tries.
_Avoid_: Try chance, try odds (reserve "odds" for bookmaker prices).

**Position base rate**:
How often a position scores league-wide, before anything about the individual player
is considered. The starting value of every Try probability, and the target that a
low-evidence player's rate is pulled toward.
_Avoid_: League average, baseline.

**Player try factor**:
A multiplier on the Position base rate expressing how much better or worse than their
position this specific player finishes. 1.0 means exactly positional average.
_Avoid_: Player rating, form factor (Form means something narrower — see below).

**Form**:
A team's or player's results over a recent window only. Never career-spanning; when a
statement is about all of history, say "career".
_Avoid_: Recent performance, momentum.

**Squad strength index**:
How much experience a team's named on-field Squad carries, relative to what that club
normally fields over the Modelling window. Centred on 1.0. Exists because Form belongs
to the Squad that generated it, and this week's Squad may not be that one.
_Avoid_: Team strength (that's Form), squad rating, experience score.

**Edge**:
Two unrelated meanings, both load-bearing, so always qualify which:
*Field edge* — the left/right/middle/fullback side of the field a player attacks or
defends, derived from jersey number.
*Betting edge* — the gap between the model's Try probability and a bookmaker's
implied probability.
_Avoid_: Using bare "edge" for either.

**Model version**:
One complete implementation of the prediction model, identified by the integer
written to `predictions.model_version`. Two versions may run concurrently, scored
against the same matches. A version is a whole module, never a branch inside a
shared function.
_Avoid_: Model variant, algorithm version, V1/V2/V3 as a naming scheme for anything
other than the historical versions already recorded in the database.

**Champion / Challenger**:
The Model version currently served to users (Champion) and one running alongside it
purely to be measured (Challenger). A Challenger's predictions are recorded but never
shown.
_Avoid_: Production/staging model, A/B (there is no split of users).

### Data

**Modelling window**:
The span of recent seasons any rate, form or probability may be estimated from.
Deliberately narrower than what the database stores.
_Avoid_: Lookback, training window.

**Retention span**:
The full range of seasons the database holds (2010 onward). Most of it exists for
Career games and for browsing history, and is explicitly out of bounds for estimating
rates — see ADR-0007.
_Avoid_: History, dataset, archive.
