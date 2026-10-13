---
title: 'Redpanda container integration-test job'
type: 'chore'
ticket: '3'
created: '2026-10-13'
status: 'built'
baseline_revision: '48151198777e2523f8f1862a730380f0c433af58'
route: 'oneshot'
route_source: 'auto'
risk: 'medium'
review: 'quick'
review_source: 'pinned'
lenses_ran: ['quick']
review_loop_iteration: 0
context: []
---

<frozen-after-approval reason="human-owned intent — do not modify unless human renegotiates">

## Intent

**Problem:** AD-8 needs a never-ack integration test against a real Redpanda broker, but `task ci` can't start one yet, so epic-safe-against-real-systems has nowhere to put that test.

**Approach:** Add package `test/integration` (build tag `integration`) that uses testcontainers-go's Redpanda module to start a broker, with a smoke test that produces one record and consumes it back. Run it from `task ci` through a new `test:integration` task, and make sure no container is left running afterwards.

</frozen-after-approval>

## Implementation Notes

The oneshot route was chosen because the change is about 85 hand-written lines: one test file, a package doc, the Taskfile, `.golangci.yml` and the README.

- Versions follow Connect v4.112.0's own graph so the module graph doesn't churn: `testcontainers-go v0.44.0` (already in go.sum), `modules/redpanda v0.42.0`, and `franz-go v1.21.6` (kgo) as the client.
- The image is pinned to `docker.redpanda.com/redpandadata/redpanda:v26.2.4`, not `latest`, so the job is reproducible. It's the newest tag already pulled on the dev machine.
- `test/integration/doc.go` has no build tag, so `go test ./...` and `go vet` see a package and don't trip over a directory whose files are all excluded. The tagged test file holds the smoke test.
- `.golangci.yml` sets `run.build-tags: [integration]`, so the tagged file is linted too.
- Cleanup goes through `testcontainers.CleanupContainer(t, ctr)` (registered before the error check, as Connect does), with Ryuk as the backstop.

Implementation (2026-10-13):
- Files: `test/integration/doc.go`, `test/integration/redpanda_test.go` (`startRedpanda` helper plus `TestRedpandaSmoke`, using kgo), `go.mod`/`go.sum` (three new direct requires, only additions, no existing pin moved), `Taskfile.yml` (`test:integration` with `-count=1 -timeout 10m`, in `ci` after `test:go`), `.golangci.yml` (`run.build-tags`), `README.md`.
- `startRedpanda` is the seam epic-safe-against-real-systems reuses for the never-ack test.
- Verified: `task test:integration` passes in about 12s with the image cached; `task ci` exits 0; after Ryuk's idle timeout, `docker ps -a --filter label=org.testcontainers=true` is empty. The Redpanda container itself is removed at test end, and only Ryuk lingers for about 10s.
- Surprise: none. The cold first pull of the image isn't timed here because it was already cached.

## Review Triage Log

Pass 1 (quick): high 0, medium 2, low 0, false 0, maybe-false 0. Both were patched.
- medium, patch: Ryuk outlived `task ci` by about 10s, so a container was still running when the job returned. The plan had softened the verify line. Fix: `test:integration` sets `TESTCONTAINERS_RYUK_RECONNECTION_TIMEOUT=1s`, and a follow-up step fails the task if any container labelled `rpcn-debugger.test=integration` remains. Verified: Ryuk is gone within about 1–2s of the test's exit, and a planted labelled container trips the gate. The reaper is testcontainers' own and removes itself. Only the about-1s window remains; whether it counts as "left running" is the user's call.
- medium, patch: cleanup on abnormal exit (go test timeout or a crash skips `t.Cleanup`) relies on Ryuk, and an environment with `TESTCONTAINERS_RYUK_DISABLED=true` would leak the broker. Also, a hung pull was bounded only by the 10m process timeout, which skips cleanup. Fix: the task forces `TESTCONTAINERS_RYUK_DISABLED=false`, and `startRedpanda` gives the pull and startup a 3-minute deadline, so a hang returns an error and `t.Cleanup` runs. The README records that Ryuk stays on.

## Verification

**Commands:**
- `task test:integration`: expected PASS, with the smoke test producing and consuming one record.
- `task ci`: expected exit 0.
- `docker ps -aq --filter label=rpcn-debugger.test=integration` after the task: expected empty. `test:integration` checks this itself, and Ryuk exits about 1s after the test.
