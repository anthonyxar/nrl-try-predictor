# Kickoff prompt — 2027 model upgrade

Saved for whenever you're ready to start the work. Paste the block below into a fresh
Claude Code session in this repo.

The design is already settled — it lives in `docs/model-review-2026.md`, `CONTEXT.md`,
ADR-0006 and ADR-0007. This prompt deliberately does **not** restate the decisions; it
points at them, so there is one source of truth and no chance of the prompt drifting
out of sync with the docs.

**Before pasting**, put a read-only `DATABASE_URL` in `backend/.env` (gitignored). Step
0 needs it and several steps after that do too.

---

```
Read docs/model-review-2026.md in full, then CONTEXT.md, docs/adr/0006-parallel-model-versions.md
and docs/adr/0007-full-history-retention-rolling-model-window.md. Together they are the
spec for this work: the design decisions in them are settled, so implement them rather
than re-deriving or relitigating them. If you find something in them that is actually
wrong, say so before writing code.

Then check the GitHub issues (gh issue list) — there's a tracking issue with children
covering this work, with blocking relationships already encoded. Work them in the order
§7 of the review sets out, respecting the blockers.

Start with step 0 below, which isn't yet an issue because it's a measurement, not a
change. Report what you find and stop for my sign-off before starting issue 1.

STEP 0 — size the problem before changing anything. I've put a read-only DATABASE_URL
in backend/.env. Using it, answer:

  a) How many rows in players / matches / tries, and what's the total DB size? ADR-0007
     estimates the 2010 reseed at ~2.5x current against Supabase's 500MB free tier —
     confirm there's headroom.
  b) How bad is the name-join problem actually? Count distinct player names, find
     near-duplicates (differing only by punctuation, initials, accents, or whitespace),
     and count names whose recorded club history looks implausible. This sizes finding
     2.6 and tells us whether the provisional fit in §7 can trust a name-keyed subset.
  c) How many rows in `players` are non-playing reserves — jersey >= 18, or bench
     players with no interchange record? This sizes finding 2.5. Note that isOnField
     was never persisted, so this is an estimate from jersey numbers.
  d) Run the §3.3 query against the CURRENT schema (adapted — no player_id, no
     is_on_field yet, so key on name and accept the noise) restricted to players with
     unambiguous names. This is the provisional fit from §7. I want to know: is there a
     real experience effect, is it a debut cliff or a smooth curve, and how big is it?
     Fill §3.4's table with what you get and mark it clearly as provisional.
  e) Confirm the 2026 accuracy baseline from the predictions table so we have a
     before-picture: win accuracy, top-3 hit rate, multi all-scored rate, and edge-pick
     ROI. Fill what you can of §1.3's Champion column.

Commit the updated doc with the provisional numbers. Then stop and tell me whether
finding (d) justifies the rest of the plan — if the experience effect isn't there, I'd
rather know before backfilling six seasons of player IDs.
```

---

## Why step 0 comes first

The entire plan rests on an effect inferred from **one match** (2026 round 27,
Rabbitohs v Roosters). Step 0 costs an afternoon and tests that inference against six
seasons before anyone backfills a database. If the curve in §3.4 comes back flat, the
right move is to drop the multiplier and spend the off-season on findings §2.2 (ceiling
saturation) and §2.3 (leakage) instead — both of which are defects regardless of
whether experience predicts anything.

## After step 0

The issues carry the rest. Rough shape:

1. `player_id` + player dimension table → unblocks everything else
2. `isOnField` → small, do it alongside 1
3. Reseed to 2010
4. Backtest harness + leakage fixes → unblocks all validation
5. Experience multiplier, fitted properly this time
6. Log-odds restructure
7. Rolling modelling window
8. Calibration rebuild
9. Parallel models + unlisted compare route

Steps 5–8 are the challenger model (`model_version = 4`) and should land together
behind ADR-0006's module boundary, not be merged piecemeal into the champion.
