# Glossary: rpcn-debugger

| Term | Meaning |
| --- | --- |
| Session | One debug run from launch to terminate. It owns all debug state (AD-3) |
| Capture | Running only the input to take one batch, without acking it (AD-4) |
| Capture config / step config | The two configs the rewrite produces: input-only for capture, and all processors plus the sink for stepping (AD-4) |
| Stepper | `rpcn_debug_stepper`, the plugin that wraps one processor list and stops between its children (AD-7, AD-16) |
| Stepper entry / frame | One execution of a stepper, e.g. once per message in `for_each`. It's shown as a call-stack frame with a repeat label (AD-10, AD-17) |
| Stop position | Before child `i` of a stepper entry, or after its last child (AD-10) |
| Pause gate | The process-wide lock held only while a thread is stopped (AD-9) |
| Thread | A debug thread. A new one is forked for each concurrent branch (AD-17) |
| Snapshot | The single, lossless shape of message state at a stop (AD-11) |
| Replay | Feeding a saved capture file instead of the live input (AD-4, AD-11) |
| Dry-run | The default delivery mode: results go to VSCode only (AD-5, AD-21) |
| Sink | `rpcn_debug_sink`, the step config's final output (AD-21) |
| Input class | replay-safe / side-effecting / rejected / unknown (AD-5) |
| Prefix replay | Bloblang statement stepping by running statements 1..k (AD-13) |
| Recomputed | A traced Bloblang value from a non-deterministic function, which may differ from the real run (AD-13) |
| Equivalence | The guarantee that output with the debugger equals output without it (AD-8) |
| User path | A processor location as the user wrote it, e.g. `pipeline.processors[2].branch[1]` |
