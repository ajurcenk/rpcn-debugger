---
title: 'VS Code acceptance run'
type: 'chore'
ticket: '11'
created: '2026-10-22'
status: done
baseline_revision: '2ef8e0df7e7bc1c8b3c54d1f483590b13a2b6d36'
route: 'oneshot'
route_source: 'auto'
risk: 'low'
review: ''
review_source: ''
lenses_ran: []
review_loop_iteration: 0
context: []
---

<frozen-after-approval reason="human-owned intent — do not modify unless human renegotiates">

## Intent

**Problem:** epic-tracer's Done when items 1, 2 and 6 are user-facing. They hold only once a person sees them in VS Code, with the installed `.vsix` and stock `rpcn-debug`.

**Approach:** Two demo pipelines, plus a checklist the user ticks in VS Code:
- `testdata/tracer/acceptance.yaml` uses every in-scope parent, metadata (string, bytes, internal) and output batching.
- `testdata/tracer/acceptance-strict.yaml` covers strict mode.

</frozen-after-approval>

## Checklist

Done when 1:
- [x] F5 on `acceptance.yaml` stops before the first `input.processors` child; the line is highlighted.
- [x] Variables › Batches › batch 0 shows 3 messages, each with content, metadata (`source`; `kafka_key` as text) and an internal node holding `__rpcn_internal`.
- [x] Step In (F11) reaches every parent's children. The call stack shows `[msg i/3]` (for_each, try), `[loop i]` (while), `[attempt i/2]` (retry) and `[group i]` (group_by). switch, branch and catch stop at their child lines.
- [x] Step Over (F10) and Step Out (Shift+F11) move as expected. Pause (F6) works while running (e.g. on `testdata/tracer/pipeline.yaml` with a `sleep`, as in 2.6).
- [x] F5 on `acceptance-strict.yaml`: the second mapping doesn't stop, or runs on no message that has `after`. The Debug Console shows the strict rejection note, with no "run failed".

Done when 2:
- [x] Continue to the end on `acceptance.yaml`. The Debug Console shows the `output.batching` warning at launch and one `Delivered batch …` block per output batch, with content, metadata and errors. There's no error dialog.

Done when 6:
- [x] `task ci` is green on the committed tree, and all of the above works from the installed `.vsix`.

## Implementation Notes

The oneshot route was chosen because the entry is two demo pipelines and this checklist; there's no code.

- Both demos were run through `test/dapdrive` with Step In at every stop:
  - `acceptance.yaml`: 50 stops; labels msg, loop, attempt and group; the batching warning; the try skip; retry attempts; the time-budget warning; 2 delivered batches.
  - `acceptance-strict.yaml`: runs to the strict note.
- Seen during the probe and deferred (cosmetic): benthos's own strict Warn line names `rpcn_debug_sink` and an untranslated `(line 1)` from the example error. That's recorded in deferred-work.md.

- 2026-10-22 acceptance (user): every checklist item passed in VS Code with the installed .vsix; no issues. Done when 1, 2 and 6 are met.
