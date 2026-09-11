# Model factors

Reference list of every factor the prediction model (`backend/model.py`) weighs, for
whoever is next tuning it. This used to be rendered as an in-app "How the Model Works"
page (`/models`); it was moved here since it's documentation, not a user-facing feature,
and doesn't need to ship in the frontend bundle or take up a nav slot.

See `AGENTS.md`'s "Model: V3 only" section for how this fits into the codebase (single
model version, no `model_version` branching).

## Try probability

What decides how likely a player is to score:

- **Position base rate** — how often each position scores tries league-wide (e.g. wingers ~35%, props ~6%).
- **Player try factor** — blends the player's recent 5-game form (60%) with career rate (40%), relative to their position's expected rate.
- **Team attack / opponent defence** — blends last-5 and last-10 game averages for both sides.
- **Edge vulnerability** — the opponent's left/right/middle defensive weakness, weighted by how much each position actually attacks that edge.
- **Home advantage** — a 6% try-rate boost when playing at home.
- **Weather & ground conditions** — wet weather and heavy grounds suppress try rates, backs more than forwards.
- **Margin-of-victory weighted form** — a 30-point win counts more than a scrappy 2-point win, via a sigmoid quality score.
- **Rest days & bye week** — short turnarounds (≤5 days) penalise try rate, a bye week or 9+ days rest boosts it.
- **Season progression** — early-season rounds (1–8) are discounted, since there's less reliable data yet.
- **Opponent-quality adjusted try rate** — tries scored against strong defences count for more than tries against weak ones.
- **Interchange timing** — bench players are scaled by their actual average minutes on field, not a flat assumption.
- **Historical calibration** — predictions are pulled toward the observed hit rate for their probability bucket, correcting for any systematic over/under-confidence.

## Win probability

What decides the predicted winner and score:

- **Recency-weighted form** — last-5 form (20%) and last-10 form (10%), so recent results matter more than older ones.
- **Points differential** — last-5 scoring margin (15%).
- **Margin-weighted quality score** — the same sigmoid-of-margins signal used for try rates (20%).
- **Head-to-head record** (10%).
- **Venue-specific home advantage** — the home team's actual win rate at that ground, where there's enough history; a flat +8% otherwise (15%).
- **Season win rate** (10%).
- **Rest days, bye week, and early-season adjustments** layered on top of the weighted blend.

Every match is scored by this same model, and every prediction is tracked in the
accuracy dashboard (`/accuracy`) against what actually happened.
