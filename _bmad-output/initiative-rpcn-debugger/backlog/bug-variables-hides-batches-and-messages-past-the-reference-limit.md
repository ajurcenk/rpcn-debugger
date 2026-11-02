---
id: 2
type: bug
title: "Variables silently hides batches past 64 and messages past 1024"
parent: none
covers: []
after: []
assignee: ""
refined: false
hitl: false
risk: low
severity: P2
---

# Variables silently hides batches past 64 and messages past 1024

## Description

After a processor fans a batch out (`unarchive`, `split`), a stop can hold more than 64 batches or a batch more than 1024 messages. In the Variables view, batch 64 onward and message 1024 onward show as leaves that can't be expanded, with no notice. The user can't tell that the content exists but can't be shown. The view should either make them expandable or say plainly that they aren't, and why.

## Reproduction

1. A pipeline whose input yields 10 messages of 110 lines each, then `unarchive: {format: lines}` and `split: {size: 10}` (1100 messages in 110 batches at the stop after `split`).
2. F5 and Step Over to the stop after `split`; expand Batches.
3. Actual: batches 0–63 expand; batches 64–109 are leaves with no children and no notice.
4. Expected: every batch expands, or the view shows a line such as "46 more batches can't be expanded (limit 64)".

The retro of epic-tracer reproduced this with `e10_many.yaml` (1100 messages; 110 batches, 64–109 unexpandable). That file is gone; on 2026-11-02 at `e65a2c5` `encodeRef` in `debugger/dap/variables.go` is unchanged since `00fc71c` and still returns 0 (not expandable) past `maxRefBatches` and `maxRefMessages`.

## Cause Hypothesis

`encodeRef` packs stop, frame, batch and message into one integer `variablesReference`, with fixed limits per field (64 batches, 1024 messages). Past a limit it returns 0, which DAP reads as "no children", and nothing tells the user. The `maxMessages` cap (1024) assumed the captured size bounds every stop, which fan-out processors break.

## Acceptance Criteria

1. **Content past the limits is reachable or announced**
   **Given** a stop with more than 64 batches, or a batch with more than 1024 messages
   **When** the user expands Batches or the batch in the Variables view
   **Then** each batch and message expands, or a visible entry says how many can't be expanded and the limit
2. **Tests cover the condition found and fixed**
   **Given** the `dap` Variables tests
   **When** they run
   **Then** a test that fails on the defect and passes on the fix covers a stop with more than 64 batches, and one covers a batch with more than 1024 messages
3. **Or: no change is needed, with proof**
   **Given** the reproduction
   **When** it is run on the current code
   **Then** every batch and message already expands, or a notice already shows, with the evidence recorded in Notes — this supersedes 1 and 2

## References

- parent — none
- retrospective — _bmad-output/initiative-rpcn-debugger/epic-tracer/epic-tracer-retrospective.md, Cross-ticket seams S3; action A4
- retrospective — _bmad-output/initiative-rpcn-debugger/epic-safe-against-real-systems/epic-safe-against-real-systems-retrospective.md, action B8
- code — debugger/dap/variables.go, encodeRef, batchVar, messageVar

## Notes

- Assumption: severity P2; content is hidden with no notice, and a smaller input is the workaround.
- Open question: widen the reference encoding, or keep the limits and show the notice. The retro offered both.
