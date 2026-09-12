# Kickoff prompt — single-box Sydney migration

Saved for whenever you're ready to start the work. Paste the block below into a fresh
Claude Code session in this repo.

The design is already settled — it lives in `docs/migration-plan.md`. This prompt
deliberately does **not** restate the decisions; it points at them, so there is one source
of truth and no chance of the prompt drifting out of sync with the plan.

**Safe to start any time.** Phases A-G carry no production risk — the Render/Supabase stack
keeps serving throughout. Only Phase H (cutover) touches production, and it must not happen
until after the 2026 Grand Final.

**Before pasting:** nothing is required for Phase A. Phases C-E need the droplet, domain and
R2 bucket from §7 of the plan — but don't provision them yet; the prompt below stops for
sign-off well before then.

---

```
Read docs/migration-plan.md in full. It is the spec for this work: the decisions in §3 are
settled, so implement them rather than re-deriving or relitigating them. If you find
something in it that is actually wrong, say so before writing code.

Then read AGENTS.md, and the ADRs it points at for anything you are about to touch —
especially 0003 (non-fatal DB init) and 0005 (in-process scrape sync), both of which this
migration amends rather than reverses.

HOW I WANT THIS DONE

Delegate each task in §4 to its own sub-agent. One task, one agent, one commit. Do not
bundle a phase into a single sweeping change, and do not let one agent carry multiple
tasks — a mistake in Phase C must not be able to contaminate Phase A, and I want to be
able to review each step on its own.

Between tasks: you verify the work yourself before committing, and you report what
changed. Do not take a sub-agent's "done" at face value — check the diff, and for Phase A
actually run the stack.

Work on a feature branch per phase and open a PR. Do not push to main.

Tasks within a phase that don't depend on each other can run in parallel; tasks across
phases mostly cannot, because later phases build on earlier ones. A2, A3 and A5 are
independent of each other and of A1. A4 needs A1. A6 needs Phase E.

START HERE — PHASE A ONLY

Do Phase A (A1-A5; skip A6 until Phase E exists) and then stop for my sign-off before
touching Phase B.

Phase A first because it is the thing I most immediately want — a local dev stack I can
actually work in, so I stop testing in production — and because it has zero production
risk.

Two things in Phase A matter more than they look:

  - A3 (ENABLE_SYNC_LOOPS) is a safety gate, not a convenience. Right now, running the
    backend locally against a production DATABASE_URL will write to production from my
    laptop via _scrape_sync and _prediction_sync. Default it OFF, and make the failure
    mode loud: if the loops are disabled, log it clearly at startup so I don't spend an
    hour wondering why nothing is syncing.

  - A2 (DB_SSLMODE) must not change production behaviour. Default it to `require` so the
    current Supabase connection is byte-identical; only local sets `disable`.

When Phase A is done I want to be able to run `docker compose up`, get a seeded local
Postgres, a hot-reloading frontend, an auto-reloading backend, and no writes leaving my
machine. Show me that working — don't just tell me it should.

Then stop and tell me: did anything in §4's Phase A turn out to be wrong or
underspecified once you were in the code?
```

---

## Why Phase A comes first

The stated motivation for the whole migration was twofold: a server that never sleeps, and
*not constantly deploying to prod to test things.* Only the second one is solvable without
spending money, and it's solvable this week.

Phase A also de-risks everything after it. Phases B-F all assume a working local Compose
stack with a real Postgres in it — provisioning, backups and the parity harness are all
much harder to develop against a remote database you're afraid to break.

## Phase order and why

| Phase | Depends on | Production risk |
|---|---|---|
| A — local dev stack | — | none |
| B — prod topology | A1 | none (not deployed yet) |
| C — provisioning script | droplet exists | none |
| D — CI/CD | B, C | none until first deploy |
| E — backups and secrets | C | none |
| F — parity harness | B, E | none (read-only) |
| G — documentation | A-F | none |
| H — cutover | A-G complete, **post-Grand Final** | all of it |

G is last because it documents what was actually built, not what was planned — with the
exception of G4 (`docs/runbook.md`), which must exist *before* H, since §6 of the plan
chose `pg_dump` over droplet snapshots and "how do I rebuild this box" therefore has to be
written down before it's needed.

## What to watch for

The plan calls these out, but they are the places this work is most likely to go wrong:

- **`edge_picks` is the highest-stakes table in the database** and was nearly left out of
  the migration scope entirely. It stores pre-kickoff bookmaker odds that cannot be bought
  back at any price. Any step that touches dumps, restores or verification should be
  checked against that table first, not the big re-scrapable ones.
- **Finals URL parsing** (`finals-week-N` vs `round-N`) has broken the match nav once
  already. The Phase F parity harness must cover a finals round, not just a regular one.
- **`scrape.yml` will silently stop working** the moment Postgres has no published port.
  D3 exists for that reason; don't discover it during cutover.
- **Cutover ordering**: stop the Render backend *before* taking the final dump. Its two
  background loops keep writing otherwise, and the dump is stale before it finishes.
