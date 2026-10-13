// Package debugger is the public entry point of the Redpanda Connect pipeline
// debugger (AD-1). All debugger behaviour lives behind
// Serve(env, in, out, opts...), which the thin cmd/rpcn-debug binary calls
// over stdio. Users with compiled-in plugins call Serve from their own main
// package with their own environment.
//
// Serve speaks the Debug Adapter Protocol on in and out. Only the DAP encoder
// writes to out (AD-15); diagnostics go to stderr through log/slog.
package debugger
