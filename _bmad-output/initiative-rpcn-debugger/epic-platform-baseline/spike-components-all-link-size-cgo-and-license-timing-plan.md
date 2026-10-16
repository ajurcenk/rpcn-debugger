---
title: 'components/all link: size, CGO and license timing'
type: 'chore'
ticket: '6'
created: '2026-10-14'
status: done
baseline_revision: '781400f9e4c17a4a6d006397f4bcf69c9f119bbf'
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

**Problem:** The spine leaves open three questions that U-REL and enterprise users depend on:
- How big is a binary that links `components/all`?
- What CGO does it need across OS/arch?
- Are enterprise components in `processor_resources` built during `Build()` before `public/license` can apply a license?

**Approach:** Measure the entry-1 `rpcn-debug` binary for size and pure-Go builds, both per OS/arch and with CGO. In a throwaway nested `spike/go.mod` module, probe license timing with an enterprise processor (`jira`), using the license at `/etc/redpanda/redpanda.license` read at run time and never copied. Record the answers in the spine's Open Questions through `bmad-architecture`, then delete `spike/`.

</frozen-after-approval>

## Implementation Notes

The oneshot route was chosen because the spike code is about 100 throwaway lines, and the lasting output is two rows in the spine.

Facts from investigation (Connect v4.112.0):
- `public/license`: `LocateLicense(res, opts...)` reads the default path `/etc/redpanda/redpanda.license` (`internal/license/service.go:23`) or the environment. `StoreCustomLicenseBytes(res, bytes)` parses the given bytes. Both need a `*service.Resources`.
- `rpk connect` registers the license in `CLIOptOnConfigParse` (`internal/cli/enterprise.go:111`), before anything is built. `StreamBuilder` has no such hook; `Stream.Resources()` exists only after `Build()`.
- `jira`'s constructor calls `license.CheckRunningEnterprise(mgr)` first (`internal/impl/jira/processor_jira.go:107`), so a build-time license failure surfaces as that error.

Probes (the spike module requires connect v4.112.0 and benthos v4.81.0):
1. `processor_resources: [{label: j, jira: …}]` through `StreamBuilder.SetYAML` + `Build()`, with no license step. Expect a build error if resources are built eagerly.
2. The same `jira` used inline in `pipeline.processors`. Is it built in `Build()` or later?
3. A custom processor plugin, standing in for `rpcn_debug_stepper`, whose constructor calls `license.LocateLicense(res)` and then builds a child `jira` from `FieldProcessorList`. Does a license applied at plugin-construction time reach children? And do `processor_resources` get built before or after the plugin?
4. Control: the same configs with no readable license (`LocateLicense` with a custom path that doesn't exist), so the failure looks different from success.

Findings: license timing (2026-10-14, `spike/main.go`, run against `/etc/redpanda/redpanda.license`):
- `StreamBuilder.Build()` constructs nothing. Every probe built OK, including those without a license. Inputs and `pipeline.processors` are constructed in `Stream.Run()`, and `processor_resources` lazily, on first use.
- **The answer:** `license.StoreCustomLicenseBytes(strm.Resources(), raw)` called after `Build()` and before `Run()` licenses everything (P8): a `jira` processor in `processor_resources` used via `resource:`, an inline `jira` processor, and a `jira` input. No hook or wrapper is needed, and the earlier fear that resources are built inside `Build()` doesn't hold.
- A license applied in a processor plugin's constructor (the stand-in for `rpcn_debug_stepper`) reaches its children and lazily built resources (P3, P4). It doesn't reach anything constructed before it: an inline enterprise processor earlier in the list (P5), or the input (P6). An input-wrapper plugin licenses its child input (P7). Neither is needed, given P8.
- Without a license, how it fails depends on where the component sits:
  - an enterprise **input** or inline **processor** makes `Run()` fail at once with `failed to init … unable to access license service` (P2, P6);
  - an enterprise **`processor_resources`** entry doesn't fail `Run()`. Every message logs `Failed to obtain processor resource 'j' … unable to access license service` at ERROR and is retried until ctx ends (P1).

  The debugger has to surface that log as a session error, not hang.
- `SetPrintLogger(nil)` gives a nil logger and panics in `AsyncReader.loop`. Always set a logger.

Findings: size and CGO (2026-10-14; `./cmd/rpcn-debug`; Connect v4.112.0; `go build -trimpath -ldflags "-s -w"`; `CGO_ENABLED=0`; Go 1.26.6):

| Target | Stripped size | Cold build here |
|---|---|---|
| linux/amd64 | 337.0 MB | 186 s |
| linux/arm64 | 310.1 MB | 211 s |
| darwin/amd64 | 345.8 MB | 234 s |
| darwin/arm64 | 321.0 MB | 227 s |
| windows/amd64 | 343.5 MB | 200 s |

- Unstripped linux/amd64 is 467.4 MB. Stripped linux/amd64 is 92.1 MB gzipped (gzip -6; only linux/amd64 was measured), which is roughly what a per-platform `.vsix` would add.
- All five targets cross-compile pure Go with no C toolchain.
- What `CGO_ENABLED=0` leaves out (the `go list -deps` diff against `CGO_ENABLED=1`):
  - only the **`tigerbeetle_cdc` input** (`internal/impl/tigerbeetle` is `//go:build cgo`);
  - the cgo-only `github.com/DataDog/zstd`. Its importer, `apache/pulsar-client-go/pulsar/internal/compression`, switches to its pure-Go zstd codec under `!cgo` (`zstd_go.go`), so pulsar's zstd compression just changes implementation.

  With `CGO_ENABLED=1`, the build would also need a C toolchain, and for tigerbeetle-go its native client library.
- The `x_benthos_extra` tag additionally adds `zmq4` (libzmq) and `purego`/ffi. It stays unset.

Outcome: through `bmad-architecture` (Update, narrow), both answers went into the spine's Open Questions as resolved rows, and the memlog gained four entries (two events, two decisions). `lint_spine.py` is clean. After review, AD-22's License rule was amended in place (user, option A); see the triage log. The `spike/` folder was deleted. The probe source isn't committed; its findings are recorded above.

## Review Triage Log

Pass 1 (quick): high 0, medium 2, low 5, false 0, maybe-false 0.

| # | Finding | Verdict | Route | Evidence |
|---|---|---|---|---|
| 1 | The binding outcome (license after `Build()`, before `Run()`) lived only in a "Closed" Open Questions row; AD-22 named no call site, so a stepper-constructor placement would leave the input unlicensed | medium | user decision, then patch | Confirmed against P5/P6 and benthos `public/service/stream.go:61-90`. The fix amends an AD, so the user chose (option A): AD-22's License rule now fixes the call site and ordering, adds `/etc/redpanda/redpanda.license` as the last fallback, and makes an unlicensed resource a `LICENSE_REQUIRED` session error. Memlog decision logged, and the lint is clean |
| 2 | "Surface the unlicensed-resource log, don't hang" and "always set a logger" were recorded nowhere binding | medium | user decision (with #1) | The first is now part of AD-22. The second is an implementation fact, below spine altitude, and stays in this plan as a note for the engine stories |
| 3 | The DataDog/zstd pure-Go fallback was credited to the wrong package | low | patch | Corrected in the plan. A memlog correction entry was appended, since the memlog is append-only |
| 4 | The spine row's "about 92 MB gzipped" read as holding for all platforms | low | patch | The row now says "linux/amd64 is 92 MB gzipped" |
| 5 | Outcome said the memlog gained three entries; it gained four | low | patch | Corrected |
| 6 | Spine `sources:` didn't cite spike 1.6 | low | patch | Source line added |
| 7 | The frozen Intent says "with CGO", but no `CGO_ENABLED=1` build was measured | low | reject | The ticket only asks whether `CGO_ENABLED=0` builds and which components need CGO, which the `go list -deps` diff answers (reviewer confirmed tigerbeetle holds the only `//go:build cgo` files). A CGO=1 size has no consumer |
