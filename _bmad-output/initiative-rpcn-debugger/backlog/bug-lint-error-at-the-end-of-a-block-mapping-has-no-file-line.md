---
id: 1
type: bug
title: "A lint error at the end of a multi-line mapping has no file line"
parent: none
covers: []
after: []
assignee: ""
refined: false
hitl: false
risk: low
severity: P3
---

# A lint error at the end of a multi-line mapping has no file line

## Description

When a launch fails on a lint error in a `|` block (a multi-line `mapping` or `bloblang`) and the error sits on the block's last line, the error names no file line and no user path. The Debug Console shows only the rewritten config's position, such as `pipeline.processors: lint errors: (10,30) expected query`, so the user can't click to the line. The error should land at the block's last line, under the processor's path, as single-line mappings already do. The same happens for a multi-line `processor_resources` entry.

## Reproduction

1. Pipeline (the `generate` input from `debugger/engine/positions_test.go` `genInput`, 5 lines), then:
   ```yaml

   pipeline:
     processors:
       - mapping: |
           root = this
           root.a = 
   output:
     drop: {}
   ```
2. Run `Check` with the position map wired (`loadMapped` in `debugger/engine/positions_test.go`), as F5 does.
3. Actual: PIPELINE_INVALID with `UserPath=""`, `Line=0`, message `…/pipeline.yaml: pipeline.processors: lint errors: (10,30) expected query, got: `. Adding a second processor after the mapping gives the same result.
4. Expected: `UserPath="pipeline.processors[0]"` and `Line=11`, the line of `root.a = `.
5. The same block as a `processor_resources` entry `r` (referenced by `- resource: r`) gives `resources: lint errors: (4,26) expected query`, unmapped too. The only resource lint test (`"bad resource"`) is single-line.

Re-run on 2026-11-02 at `e65a2c5` with a throwaway probe; all three cases unmapped.

## Cause Hypothesis

The linter reports the error one position past the block's last character (the point where it expected more query), which in the rewritten YAML is on the line after the block scalar. `rewrite/positions.go` `FilePosition` then finds that position outside the processor's node (or on the next node) and gives up or maps it to the wrong node. The retro of epic-tracer saw both outcomes: the wrong (next) node, or unmapped.

## Acceptance Criteria

1. **The error lands at the block's last line**
   **Given** a lint error at the end of a `|` block in a processor, or in a `processor_resources` entry
   **When** the launch runs
   **Then** the error names that processor's user path and the file line of the block's last line, and no position from the rewritten config
2. **Tests cover the condition found and fixed**
   **Given** `TestCheckErrorPositions`
   **When** it runs
   **Then** rows that fail on the defect and pass on the fix cover a multi-line mapping with the error on its last line (alone, and followed by another processor) and a multi-line `processor_resources` entry
3. **Or: no change is needed, with proof**
   **Given** the reproduction
   **When** it is run on the current code
   **Then** the error already lands at the block's last line, with the evidence recorded in Notes — this supersedes 1 and 2

## References

- parent — none
- retrospective — _bmad-output/initiative-rpcn-debugger/epic-tracer/epic-tracer-retrospective.md, Cross-ticket seams S4, S5; action A3
- retrospective — _bmad-output/initiative-rpcn-debugger/epic-safe-against-real-systems/epic-safe-against-real-systems-retrospective.md, action B8
- code — debugger/rewrite/positions.go, FilePosition
- tests — debugger/engine/positions_test.go, TestCheckErrorPositions

## Notes

- Assumption: severity P3; the error text still shows, only its line is missing.
