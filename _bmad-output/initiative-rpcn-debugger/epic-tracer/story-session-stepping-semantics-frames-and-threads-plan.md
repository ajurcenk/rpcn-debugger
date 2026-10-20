---
title: 'Session stepping semantics, frames and threads'
type: 'feature'
ticket: '6'
created: '2026-10-19'
status: 'built'
baseline_revision: 'b861d149cf01940ecd0564d8369468ef6959c1f3'
route: 'full'
route_source: 'auto'
risk: 'high'
review: 'quick'
review_source: 'pinned'
lenses_ran: ['quick']
review_loop_iteration: 0
context:
  - '{project-root}/_bmad-output/initiative-rpcn-debugger/architecture-rpcn-debugger/architecture-rpcn-debugger.md'
---

<frozen-after-approval reason="human-owned intent — do not modify unless human renegotiates">

## Intent

**Problem:** The Session's stop predicate is still the tracer one. It stops at the first Arrive only, and Next and Continue both run to the end. There are no frames: the call stack is one flat frame. The steppers that 2.5 now writes for every nested parent are entered, but a user can't step into them, out of them or between them. `StepIn`, `StepOut` and `Pause` answer `NOT_SUPPORTED`.

**Approach:**
- `core.Session` implements AD-10 stepping:
  - stop positions before each child and after the last child (childIndex = n, at the parent's key line);
  - Next, StepIn, StepOut, Continue and Pause, with per-thread step state decided under the pause gate (AD-9).
- It also implements AD-17 frames:
  - an immutable frame stack in ctx under a core-owned key, pushed once per stepper entry;
  - one Snapshot per live frame;
  - `ForkThread`/`ExitThread`, with a single root thread in this epic.
- The stepper pushes its frame, calls Arrive at every stop position including n, and passes the new ctx to its children.
- DAP shows the stack (innermost first), with each frame's line and its own Variables. It handles stepIn, stepOut and pause.

## Boundaries & Constraints

**Always:**
- Stop rules (AD-10). With `d0` the stack depth and `f0` the frame of the stop the command was issued at:
  - **Next** stops at the next Arrive whose depth is < `d0`, or whose frame is `f0`.
  - **StepIn** stops at the very next Arrive on the thread.
  - **StepOut** stops at the next Arrive whose depth is < `d0`, or at `f0`'s end stop (childIndex = n) when the current stop isn't already it.
  - **Continue** stops nowhere (breakpoints arrive later).
  - **Pause** makes the next Arrive on any thread stop, with reason `pause`.
  - After ConfigurationDone, the first Arrive stops with reason `entry`. Other stops have reason `step`.
- AD-9:
  - Arrive takes the process-wide gate, evaluates the predicate while holding it, and holds it while paused.
  - The gate is never held while a processor runs.
  - Step state is per thread, and resumes are process-wide (`allThreadsContinued: true`); a stop is reported with `allThreadsStopped: false`.
- AD-17:
  - The frame stack lives in `context.Context` under an unexported `debugger/core` key. Each stepper entry pushes `{frameId, stepperId, callSite}` (the `repeatLabel` slot exists and stays empty until 2.7) and passes the new ctx to its children.
  - An entry with no frame in ctx runs on the root thread, which the Session starts once (`ThreadStarted`) and ends with the run (`ThreadExited`).
  - `threadId` is stable. DAP frame ids and `variablesReference` values are minted per stop and invalid after a resume.
  - Goroutine IDs are forbidden.
- One Snapshot per live frame (AD-11). Each frame shows the batches from its own latest Arrive. A parent shows its state before the child that is running.
- Step In on a `mapping`/`mutation`/`bloblang` processor behaves as Step Over here, which falls out of the rule "the next Arrive" (statement stepping is epic-bloblang-statement-stepping).
- The 2.1–2.5 tests and F5 paths keep passing, and the AD-8 output is unchanged: the extra end stop only observes.

**Never:**
- No repeat labels, `try`/strict modes, `CTX_LOST`, AttemptStarted, MessagesSkipped or the time-budget warning (2.7). No concurrent forking (`concurrent: true` stays opaque, epic-concurrency).
- No breakpoints or exception stops (epic-breakpoints-and-evaluation). No SetMessage (epic-edit-restart-replay).

## I/O & Edge-Case Matrix

| Scenario | Input / State | Expected Output / Behavior | Error Handling |
|----------|--------------|---------------------------|----------------|
| Next through a list | Stopped before `pipeline.processors[0]` of 3 | Stops before [1], [2], then at the n stop (the `processors:` key line), then the next top-level stepper | (none) |
| Next over a parent | Stopped before a `branch` | The branch runs with no stop inside, then stops before the next sibling | (none) |
| StepIn into a parent | Stopped before a `branch` | Stops before the branch's first child, with 2 frames on the stack | (none) |
| StepIn on a leaf | Stopped before a `mapping` | Same as Next | (none) |
| StepOut | Inside a branch at child 0 of 2 | Stops at the branch's n stop. A StepOut there stops at the parent's next stop | (none) |
| Pause | Running (fake engine with a slow processor) | The next Arrive stops with reason `pause` | Pause while already stopped succeeds and changes nothing |
| Call stack | Stopped inside a branch inside the pipeline | stackTrace: [branch frame at the child's line, pipeline frame at the branch's line] | (none) |
| Per-frame Variables | Same | The branch frame shows the branch's batch; the pipeline frame shows the batch before the branch | (none) |
| Stale ids | A frameId / variablesReference from the previous stop | Scopes and variables resolve to nothing | (none) |
| Threads | Session run | `thread` event `started` for thread 1 before the first stop, `exited` at the end; threads lists one thread | (none) |
| Disconnect while nested | Stopped two frames deep | Gate released, streams stopped, `Serve` returns nil | (none) |

</frozen-after-approval>

## Code Map

- `debugger/core/session.go` (359 lines):
  - `Session` holds one `frame *Frame` and `stopNext bool`, and `Frame` is `{ID, Snapshot}`.
  - `Arrive(ctx, stepperID, childIndex, batches)` (line ~262) builds one Frame and stops if `stopNext`. The rewrite target is the AD-10 predicate.
  - `resumeRun` (line ~326) serves both next and continue.
  - Replace these with per-thread step state (`{mode, depth, frameID, atEnd}`) and a stop record holding the whole stack of frames with their latest Snapshots.
  - `Do` (line ~78) returns `NOT_SUPPORTED` for StepIn, StepOut and Pause; implement them.
- `debugger/core/gate.go`: a mutex. It stays the single process-wide gate.
- `debugger/core/port.go` `Arriver{Captured, Arrive}`: add frame entry and exit. For example, `Enter(ctx, stepperID) (context.Context, func())` pushes the frame on ctx, registers it as live with the Session and returns its exit func. Alternatively export `core.WithFrame`/`core.Frames` helpers and let the Session read the stack from ctx in Arrive. The core-owned ctx key is unexported. `ForkThread`/`ExitThread` live on the Session.
- `debugger/core/protocol.go`: StepIn, StepOut and Pause already exist as commands; Paused carries `Reason`. Add the reason values (`entry`, `step`, `pause`). `ThreadStarted`/`ThreadExited` events exist.
- `debugger/engine/stepper.go` `ProcessBatch` (line ~70):
  - It loops the children and calls `Arrive(ctx, id, i, …)` with the caller's ctx, and has no end stop.
  - Change it to enter the frame and pass the new ctx to `Arrive` and to each child's `ProcessBatch`.
  - After the loop, call `Arrive(ctx, id, len(procs), batches)`, even when a child returned no batches (with empty batches).
  - Exit the frame on return.
- `debugger/rewrite`: LineMap already answers childIndex = n at the parent key line (2.5). For top-level steppers, n is the `processors:` key line.
- `debugger/dap/server.go` and `variables.go`:
  - stackTrace returns one frame today. It should return the stopped thread's stack innermost first: name = the frame's list user path (e.g. `pipeline.processors[1].branch.processors`), line = its current position.
  - Mint per-stop DAP frame ids, and encode the frame in `variablesReference`. The stop encoding from 2.2 extends to (stop, frame index, kind, batch, message).
  - Add stepIn, stepOut and pause requests.
  - Emit `thread` events, and `stopped` with `allThreadsStopped: false` and the reason. Continue/next/stepIn/stepOut responses use `allThreadsContinued: true`.
- Tests to extend: `debugger/core/session_test.go` (with a fake stepper that drives Arrive and frames over a scripted tree), `debugger/dap/session_test.go`, `debugger/serve_test.go` (stepping through `testdata/tracer/nested.yaml`) and `vscode/src/test/adapter.test.ts` (the F5 path now sees the end stop before terminating; adjust).

## Tasks & Acceptance

**Execution:**
- [x] `debugger/core/frames.go` (new), `session.go`, `port.go`, `protocol.go`:
  - the ctx frame stack with its unexported key;
  - frame enter and exit;
  - `ForkThread`/`ExitThread` with the root thread;
  - the AD-10 predicate under the gate;
  - per-thread step state;
  - one Snapshot per live frame;
  - StepIn, StepOut and Pause.

  Unit tests cover every matrix row with a fake stepper tree.
- [x] `debugger/engine/stepper.go` (+ test): enter the frame, pass ctx to Arrive and the children, add the end stop, exit on return. An engine test runs `testdata/tracer/nested.yaml` and asserts the frame depth seen at the branch's child.
- [x] `debugger/dap/server.go`, `variables.go` (+ tests): the multi-frame stackTrace, per-stop ids, per-frame scopes and variables, stepIn/stepOut/pause, and the thread and stopped events per AD-9.
- [x] `debugger/serve_test.go`, `vscode/src/test/adapter.test.ts`: step through `nested.yaml` with Next, StepIn and StepOut, checking lines and stack depth. Update the tracer F5 path for the end stop.
- [x] `README.md`: one line on stepping.

**Acceptance Criteria:**
- Given `testdata/tracer/nested.yaml` in VS Code, when the user steps with F10/F11/Shift+F11 from the first stop, then:
  - each processor's line is highlighted in order;
  - F11 on the `branch` enters it and shows 2 frames in the call stack;
  - Shift+F11 returns to the pipeline level;
  - clicking the outer frame shows that frame's batch in Variables.
- Given a running session, when the user presses Pause, then it stops at the next processor.
- Given `task ci`, when it runs, then all Go and TS tests pass and the equivalence and tracer tests are unchanged.

## Plan Change Log

## Implementation Notes

- Port: `Arriver` gained `Enter(ctx, stepperID, children) (context.Context, func())`. The Session mints the frame, registers it as live, records its call site (the parent's latest stop) and pushes it with `core.PushFrame`; `core.FrameStack`/`core.TopFrame` read it. The ctx key (`frameKey`) is unexported; the frame stack is an immutable linked list (`debugger/core/frames.go`). Passing `children` lets the Session know the end stop (childIndex = n) for StepOut and name the frame from the LineMap's end-stop entry (the list's user path).
- Top-level reading of "the parent's next stop": a top-level frame's parent is the run itself, so with d0 = 1 Next and StepOut also stop at the next Arrive of another depth-1 entry. Without this the matrix row "Next through a list ... then the next top-level stepper" can't hold under the literal "depth < d0 or frame f0" rule. Nested entries follow the literal rule, so Next at a for-each message's end stop still leaves the for-each.
- Threads: `ForkThread()` always returns the root thread (ThreadID 1) in this epic and emits `ThreadStarted` once; `ExitThread` is a no-op for the root, which the Session ends after `Engine.Start` returns (`ThreadExited` before `Terminated`). `Session.Threads()` lists live threads; DAP `threads` uses it (empty before the run starts and after it ends).
- `Session.Frame()` is replaced by `Session.Stopped() (Stop, bool)`: the stop id (minted per stop), thread, reason and the frame stack innermost first, each with its own latest Snapshot.
- DAP ids: a frame id encodes (stop mod 64, frame index); a variablesReference encodes (stop mod 64, frame index, kind, batch, message), with limits of 32 frames, 64 batches and 1024 messages per batch (beyond them a node is not expandable / a frame is not listed). The previous layout had 128 stops, 256 batches and 4096 messages.
- Only the DAP `continue` response has an `allThreadsContinued` field; next/stepIn/stepOut resume all threads anyway (single-thread execution is not advertised). `stopped` carries `allThreadsStopped: false` (omitted on the wire as the zero value).
- An `Arrive` without an `Enter` (old-style fakes) still works as a one-off frame on the root thread.
- `debugger/dap/server_test.go`'s unknown-request case now uses `restart` instead of `stepIn` (stepIn is implemented).
- Verified: `go test -race -count=1 ./debugger/...` PASS; `task ci` exit 0 (lint 0 issues, equivalence and integration unchanged, TS adapter tests including a new nested Step Over/In/Out test). The VS Code manual checks (F10/F11/Shift+F11 on nested.yaml, Pause with a `sleep` processor) were not run.

## Review Triage Log

Pass 1 (quick): high 0, medium 1, low 5, false 0, maybe-false 0. Four were patched, one deferred, one rejected.

| # | Finding | Verdict | Route | Evidence |
|---|---|---|---|---|
| 1 | The ACs were never run in VS Code, and Pause was tested only against fakes | medium | patch | `TestServePause` uses a real pipeline with `sleep: 1s`: continue, pause during the sleep, then a stop with reason `pause` at the mapping's line. The VS Code checks are the user's manual step |
| 2 | No thread exists until capture, so `threads` is empty while the input waits and an early Pause is swallowed | low | patch | Thread 1 is marked started at ConfigurationDone; `ThreadStarted` is emitted at run start, after the response, to avoid the write-lock deadlock. A Pause sent before capture still stops at the entry stop, with reason `entry` |
| 3 | The `Arrive` fallback without `Enter` can report a stop on an unstarted thread with depth 1 | low | reject | Only test fakes reach it: the engine stepper always calls `Enter` first. Removing it would mean rewriting the old fakes for no user-visible gain |
| 4 | DAP reference limits dropped to 1024 messages and 64 batches, while `maxMessages` has no upper bound | low | patch | `max:"1024"` tag → schema `maximum`, with LAUNCH_INVALID above it. The schema test ties the cap to `maxRefMessages`. Regenerated |
| 5 | The README status line names `pipeline.processors` as the first stop | low | patch | Reworded: the first processor (input, then pipeline, then output) |
| 6 | next/stepIn/stepOut don't announce `allThreadsContinued` (AD-9); `Continued` is never emitted | low | defer | Invisible with one thread. Deferred to epic-concurrency (deferred-work.md) |

## Design Notes

Depth comes from the ctx stack, not from LineMap parent ids. That way a `processor_resources` stepper reached from two call sites gets the right depth at each one, and later epics (concurrency, labels) reuse the same stack. Next's rule "depth < d0 or same frame" lets a Step Over at a for-each message's end stop leave the for-each instead of walking the next message. That is AD-10's "the parent's next stop", and Step In is how the user walks entries one by one.

## Verification

**Commands:**
- `go test -race ./debugger/...`: expected PASS, including the stepping matrix.
- `task ci`: expected exit 0.

**Manual checks:**
- In VS Code on `testdata/tracer/nested.yaml`: F10, F11 and Shift+F11 behave as the AC says, the call stack shows nested frames, and Pause works while running. To make Pause observable, use a pipeline with a `sleep` processor.
