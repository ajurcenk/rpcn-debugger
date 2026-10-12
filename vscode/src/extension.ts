import * as vscode from 'vscode';

// The skeleton only contributes the `redpanda-connect` debug type through
// package.json. Starting the rpcn-debug adapter over stdio
// (DebugAdapterExecutable) arrives with the DAP server (AD-12, AD-23).
export function activate(_context: vscode.ExtensionContext): void {
  // Intentionally empty.
}

export function deactivate(): void {
  // Intentionally empty.
}
