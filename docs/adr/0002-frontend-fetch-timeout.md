# Every frontend fetch goes through a hand-rolled timeout wrapper, not a data-fetching library

`frontend/src/api.js` exports `fetchJson()`: a thin wrapper around `fetch()` that aborts after 45s and throws a descriptive error. Every component that talks to the backend uses it instead of calling `fetch()` directly.

This exists because plain `fetch()` has no timeout ceiling — if the backend (a Render free-tier service that sleeps after inactivity) never responds, a bare `fetch().then()` chain leaves `loading` state `true` forever, which is what caused the site to appear stuck with an endless spinner. We picked a 45s timeout specifically because Render cold starts can take 30-60s+; shorter would false-positive on a legitimate cold start.

We wrote a small wrapper instead of adding a data-fetching library (React Query, SWR) because the app has no other dependency-management need it would justify — the only problem to solve was "give every fetch a ceiling and a visible failure mode," not caching/retries/dedup. If the app's fetch needs grow more complex, revisit this.
