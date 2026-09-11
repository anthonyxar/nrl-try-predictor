# Collapse the prediction model to a single (V3) version

> **Superseded by [ADR-0006](0006-parallel-model-versions.md)** for the comparison
> question only. Its primary reason — "nobody was actually comparing versions in
> practice" — no longer holds for 2027. Its other two reasons, both about divergent
> branches rotting inside shared functions, still stand and constrain how ADR-0006 is
> implemented: a model version is a whole module, never a conditional.

`model.py` used to run three model versions side by side (V1 baseline, V2 enhanced, V3 full), selected per-request via a `version` query param and a UI selector on the Draw/MatchDetail/Accuracy pages. We deleted V1 and V2 entirely — `model.py`'s public functions no longer take a `model_version` argument and always run what was V3's factor set. The `predictions.model_version` column and the `version` query param stay (as `3` / accepted-but-ignored respectively) so old links and historical accuracy rows for model_version 1/2 keep working, but there is no way to select or generate a non-V3 prediction any more.

We did this because nobody was actually comparing versions in practice — V3 was always the default and the only one that mattered for the accuracy dashboard's headline numbers — and maintaining three parallel branches through every prediction function had real cost (the version-gating touched ~7 backend functions and 3 frontend pages). It also surfaced a latent bug: `predict_win_probability`'s V1 code path had no `else` branch and would have raised `NameError` if ever actually invoked with `model_version=1`, meaning V1 was silently broken for win predictions.

## Considered options

Keep all three versions and just fix the reported bug — rejected because the version machinery was pure surface area for bugs like the one above, for a comparison nobody was using.
