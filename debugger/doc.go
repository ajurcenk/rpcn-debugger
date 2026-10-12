// Package debugger is the public entry point of the Redpanda Connect pipeline
// debugger. All debugger behaviour lives behind Serve(env, in, out, opts...),
// which the thin cmd/rpcn-debug binary and users' own main packages call
// (AD-1). Serve arrives in a later story; this package only reserves the
// layout for now.
package debugger
