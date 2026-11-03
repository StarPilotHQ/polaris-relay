# AGENTS.md

Notes for coding agents working on polaris-relay. The README covers what it does for users. The site's API is
documented on the site at `/docs/api` (its OpenAPI document is the contract); comma's API at https://api.comma.ai.
This file covers what neither says: conventions, invariants, traps, and the comma behavior comma doesn't document.

## Keep this file current

Before you finish a task, ask whether it taught something the next agent would otherwise get wrong. If so, add it here
in the same commit (a trap, an invariant, a convention), or change or remove a line your change made false. Don't add
what the code or README already says plainly.

## Working here

- `uv sync`, then `uv run pytest -q`. CI runs exactly that.
- Run it from source as the README's Development section shows, with `POLARIS_HOST=http://localhost:8000` for a local
  site. `POLARIS_HOST` is a build argument in the image (from the release workflow's `vars.POLARIS_HOST`): the README
  mentions it only under Development, never in the user setup, and the UI never shows it.
  `RELAY_COMMA_API`, `RELAY_ATHENA_API`, `RELAY_REGISTRY` and `RELAY_TICK_S` exist for tests and development.
- Style: 2-space indents, ~120-character lines, dense code, few comments (only why). UI copy and docs
  in American English, plain words. No em-dashes anywhere. Shipped copy says "Polaris Relay" and "access token"; the
  lowercase `polaris-relay` is only for identifiers (repo, image, package, User-Agent, CLI).
- Pages use StarPilot Galaxy (`static/vendor/galaxy`, the same extract sp-intake vendors) and its `gx-*` classes, so the
  relay looks like the site. Relay-only styles go in `relay.css` as `rl-*` and use only Galaxy tokens. Backdrop blur is
  off there, as on the site; don't add it back to anything that scrolls.
- Commits are conventional commits with no scope and, unless it's really needed, no body: `fix: the description`,
  lowercase, imperative, no trailing period. Not `fix(pipeline): ...`.

## Privacy rules (hard)

- Nothing of a segment in `Route.excluded` may leave the machine: not its qlog bytes, not its files, not an upload
  request to the device. `Worker.guard` is the second wall in every step that sends or requests by segment; keep it
  in any new one. The site also drops suggestions touching excluded segments, and the daemon drops them again.
- The filter fails closed: no schema, no GPS read, no good fix, an unreadable or timed-out log: the segment (or the
  whole route) stays. Every position field the schema has goes to the filter (`qlogs.py`'s tables); when the vendored
  schema gains one, add it there with a fixture in `tests/test_privacy.py`.
- Any change to zones or the surround setting must call `pipeline.filter_changed(s)` in the same transaction. Routes
  record the `filter_generation` they were filtered under; `guard` and the start of every step compare it with a fresh
  read (`Worker.generation`, its own session: a long-lived session keeps seeing its old SQLite snapshot).
- Never request `dcameras`. `comma.request_uploads` refuses them.
- Never log a comma URL, a token or a route name at INFO or above. httpx and httpcore loggers are pinned to WARNING in
  `comma.py`; uvicorn's access log is off.
- The database keeps metadata only (no GPS tracks). Working files live under `DATA_DIR/work/<hash of route>` and are
  deleted when a route reaches a final state; `housekeeping` deletes any folder not belonging to an open route.
- Refusals don't explain themselves to the requester: the host and origin guards answer a bare 400/403 and `/healthz`
  a bare `unhealthy`; the reason goes to the log through `warn_quietly` (once a minute per message, since anyone can
  send them).
- The comma JWT goes only to comma's API and athena, never to the signed file URLs (a separate client without the
  header).

## Pipeline invariants

- Skip is a flag the web page sets (`Route.skip_requested`), never a state change: the worker may be mid-step on its
  own thread and would overwrite it. `run_steps` and `guard` read it fresh (`skip_wanted`), so a transfer stops before
  its next file. The submit delay holds only the final submit (READY, after files are in place); suggest-only devices
  never reach it. Tests onboard with `submit_delay_min=0` unless they test the delay.
- Every step is safe to repeat; state is committed before anything outward-facing that a restart must know about:
  the `Idempotency-Key` before the submit, `public_since` before making a route public, each relayed file in
  `Route.relayed` after it lands.
- `safety_net` runs on its own thread (`run_safety_net_forever`, its own session and comma client) and at the start of
  each pass, and makes private any public route past the cap or that the pipeline won't carry on copying, one route's
  failure not stopping the others. Every change to whether a route is public happens under `Worker.public_lock` after
  `s.refresh(r)`: the two threads each hold their own copy of the route. Public time adds up across stretches
  (`public_seconds`); `make_public` refuses a route past the cap or 2 days after it was first public, and re-reads the
  device too. Re-reads `s.commit()` first so no write of this session is pending (pysqlite opens a transaction only at
  the first write, so a plain SELECT already reads fresh rows). The safety net also ends copying when the route's
  `filter_gen` isn't the current generation, and the pass ends a `PUBLIC` route whose device is off, paused or gone
  (`stop_orphaned_copy`). A public-mode route whose zones change after a submit answer was lost is ended
  (`end_public`, the submission found by repeating the submit with its key) rather than made public again for the
  repeat. A withdraw the site never answers ends the route after the files window, telling the user to withdraw it
  themselves (the route is already private).
- Steps that move backward count `regressions`, which never reset; `attempts` reset only when a step moves forward.
- Route-level site errors no retry fixes are in `pipeline.TERMINAL`, account-level ones in `pipeline.ACCOUNT`; add new
  site error codes to one of them rather than letting them burn retries.
- `discover` makes every comma call before writing anything: a pending write autoflushes on the next query and would
  hold SQLite's write lock across the network.
- Steps return True when the route moved on; `advance` keeps going while the state changes, and sends every exception
  to `route_error`, which must never let one escape the pass (a poison route used to stall every route after it).
  `Waiting` isn't a failure; an outage (`transient`: unreachable, timeouts, 5xx) waits without using an attempt; other
  errors count toward `MAX_ATTEMPTS`. `describe` gives local file errors and unexpected ones only their type: their
  text carries paths and data from outside.
- Names that leave the route's row (work folders, per-route notification keys, log lines) use `Worker.route_key`, keyed
  with the data volume's secret; a plain hash of the route name can be matched by anyone who knows the route. Per-route
  notification keys are listed in `ROUTE_EVENTS` and pruned with the route.
- No write may be pending across a call to comma or the site: commit first (`note_growth` returns whether it wrote),
  and read before writing (`watch_uploads` reads the queue before touching the route).
- Global stops (comma token refused, access token refused or the account blocked) are settings (`pause:comma`,
  `pause:site`), cleared when the user saves a new token. There's no client version gate: the site marks a deprecated
  API version with `Deprecation`/`Sunset`/`Link` headers (`site.deprecation`), recorded after each pass
  (`note_deprecation`) and never a reason to stop.
- `run_forever` beats only after a pass that completed; steps beat between files (download, upload, each qlog, after
  reading GPS) so `/healthz`'s budget (`STEP_BUDGET_S`) covers one 30-minute file, not a whole transfer.
- `notify.event` sets its dedupe key only once a target took the message (or there are no targets), so a service
  that's down is tried again; tests that fake `notify.send` must return a count.
- Forward-only: routes that start before onboarding or before the device was enabled are never picked up.
- The site answers `device_not_verified` alike for never verified, lapsed and revoked; a device that submitted before
  and now comes back unverified is paused with a vague "verify it on the site again" (probably revoked).
- One submission per route on the site: a route whose suggestions don't fit the daily range limit is deferred whole,
  never split.

## Traps

- Nothing is released yet, so new `Route` columns aren't in `ADDED_COLUMNS`: delete a pre-release dev database
  (`DATA_DIR/relay.db*`) when the models change. Once a release ships, add columns there instead.
- SQLAlchemy's identity map holds objects weakly: a test of a re-read must keep a reference to the object (the
  pipeline does), or the session reloads it anyway and the test passes without the re-read.
- The worker only hears the site's headers on calls it makes; a test of something learned from a response must make
  the pass call the site (`token_info` with an old `checked` forces `GET /me`).
- `relay.web.app` imports `now_utc` by name: the `clock` fixture doesn't reach it; patch `relay.web.app.now_utc` too.
- athenad drops upload requests without saying so (the item uploading during a restart, retries run out, an expired
  upload URL, a cancel in Connect), and dedupes by upload URL without the query. `Worker.watch_uploads` reads the queue
  and asks again with a doubling wait; don't replace it with "re-request every N hours", which overrode cancels and
  took hours to notice a restart.
- pycapnp aborts the whole process (no exception) when a second copy of the same schema is loaded from another path,
  which a new schema bundle version would do. Positions are read in a short-lived spawned process (`qlogs.read_scans`,
  killed past its timeout); keep every capnp load out of the main process. Tests load the fixture schema in the main
  process to build qlogs, which is only safe because of this. A spawned child imports its target by name, so
  monkeypatching it does nothing; `read_scans` takes a `target` for tests instead.
- Jinja escapes apostrophes (`&#39;`); compare flash text in tests through `html.unescape`.
- `run_until_settled` advances the clock after its last tick, so a route's `next_attempt_at` may already be past. The
  other way round, a tick soon after one that set `next_attempt_at` (5 minutes while waiting) skips the route: advance
  past it before asserting a step ran.
- `FakeComma`'s ping follows `online` (a day old when offline). A device that should finish its qlogs and then be
  offline needs `last_ping=AFTER_DRIVE`; bringing it back needs `last_ping = None` too.
- A route's `timings` hold ISO strings only; the activity page formats them as such. Counts go in their own columns.
- `Referrer-Policy` must stay `same-origin`: under `no-referrer` browsers send `Origin: null` on form POSTs and the
  origin guard refused every form. Tests set `Origin` by hand, so they can't catch this; check a form in a browser.
- Leaflet adds layers only once the map has a view, and a circle's `getBounds` throws until then: `zones.js` sets a view
  before drawing the saved zones. The tests don't run JavaScript; open the zones page with a circle saved after changing it.
- `TestClient` sends no `Origin`; POSTs in tests need `headers={"origin": "http://testserver"}` or the origin guard
  answers 403.
- The device runs requests for `rlog.zst` when only an uncompressed `rlog` exists (it compresses on the fly), and
  Connect asks for both `rlog.bz2` and `rlog.zst`; the missing one comes back in athena's `failed` list. That's normal.

## comma, beyond api.comma.ai

From comma's own clients, not its docs (old Connect `src/api.js`, new Connect `src/api/*.ts`, openpilot
`system/athena/athenad.py`; read November 2026, never checked with a real token):

- `GET /v1/devices/<dongle>/routes_segments?start=&end=&limit=` (old Connect) is the only route list with
  `segment_numbers`; it also has `maxqlog`/`procqlog` (the docs say `maxlog`/`proclog`) and `end_time_utc_millis`
  (`/routes` has ISO `end_time`). A coordinate of 0 means unknown. If `routes_segments` goes away, the contiguity check
  can't pass.
- `PATCH /v1/route/<fullname>/` with `{"is_public": bool}` makes a route public or private.
- Connect sends `uploadFilesToUrls` items with `allow_cellular: false` and `priority: 1`: stock athenad then skips
  metered connections, and StarPilot's `AlwaysAllowUploads` setting overrides that, so the device's setting decides.
  It asks for both `rlog.bz2` and `rlog.zst` (the missing one comes back in `failed`).
- athenad's queue: `listUploadQueue` lists every item, the uploading one included, whoever queued it (`path` ends in
  `<route>--<segment>/<file>`); a request whose URL minus the query is already queued is skipped. The queue survives a
  restart (`AthenadUploadQueue` param) except the item uploading at that moment; a failing item is dropped after 30
  tries 10 s apart, a 401/403/412 from the upload URL counts as done, a cancelled item never comes back. athenad never
  reports any of that.
- New Connect treats a device as online when `last_athena_ping` is within 120 s; Polaris Relay uses 10 minutes.

## Not verified against reality

No real route has been through the daemon, and StarPilot's athenad hasn't been compared with upstream's upload queue.
The comma calls come from Connect's and athenad's source, not live traffic, and the qlog-completeness thresholds are
untested on real drives: record what you find in "comma, beyond api.comma.ai" above and the defaults in
`db.DEFAULTS`.
