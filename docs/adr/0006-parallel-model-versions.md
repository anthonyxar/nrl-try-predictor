# Run two model versions in parallel (supersedes ADR-0001)

ADR-0001 collapsed three side-by-side model versions into one, on the grounds that
"nobody was actually comparing versions in practice". For 2027 we are reintroducing a
second model version, because we now intend to do exactly that comparison: the 2027
model changes (career-games experience multiplier, rolling modelling window, log-odds
factor combination) are significant enough that shipping them blind is a worse risk
than carrying a second implementation for a season. The champion (`model_version` 3)
stays the only model users see; the challenger (`model_version` 4) runs in shadow and
is only promoted once it beats the champion over a full season of real matches.

## Status

Accepted. Supersedes ADR-0001, whose reasoning is preserved below where it still holds.

## What this does not reintroduce

ADR-0001's other two objections were about *branch rot*, not about comparison, and they
still stand:

- version-gating "touched ~7 backend functions and 3 frontend pages"
- V1's `predict_win_probability` path had no `else` branch and would have raised
  `NameError` if invoked — a divergent branch nobody exercised

So there is **no `model_version` parameter and no version conditional inside any
prediction function**. A model version is a whole module satisfying a shared interface,
registered by version number. Two implementations cannot develop the kind of dead
divergent branch that produced that `NameError`, because neither has a branch.

## Considered options

**Revive per-request `model_version` branching** — rejected; this is precisely what
ADR-0001 removed, for reasons that remain valid.

**Visible A/B with a version selector in the UI** — rejected for now. An unproven
challenger showing users worse picks costs more than it teaches, and the comparison
only needs an audience of one. The comparison view is an unlisted frontend route.

**Ship the 2027 changes directly to the single model** — rejected. The changes are
motivated partly by a single observed match (2026 round 27, Rabbitohs v Roosters), and
a season of shadow scoring is the cheapest available protection against having
generalised from one game.

## Consequences

- `predictions.model_version` — retained by ADR-0001 and never removed — becomes
  load-bearing again, and `get_accuracy_stats`'s `by_model` grouping is the comparison
  surface.
- `/api/accuracy` already returns `by_model` publicly, so shadow-model numbers are
  visible to anyone calling the API. The unlisted comparison route is tidiness, not
  access control. Accepted deliberately.
- Promotion of a challenger to champion is a decision made against the metrics in
  `docs/model-review-2026.md`, not an implicit consequence of merging code.
