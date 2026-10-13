import * as vscode from 'vscode';
import { binaryPathSetting, resolveBinaryPath } from './binaryPath';

const debugType = 'redpanda-connect';

// The extension stays thin (AD-12): it registers the `redpanda-connect` debug
// type and starts the rpcn-debug binary as a DAP adapter over stdio. The
// binary comes from the `rpcnDebug.binaryPath` setting (AD-23 MVP); bundling
// arrives with the local release.
export function activate(context: vscode.ExtensionContext): void {
  context.subscriptions.push(
    vscode.debug.registerDebugAdapterDescriptorFactory(debugType, new AdapterFactory()),
  );
}

export function deactivate(): void {
  // Intentionally empty.
}

class AdapterFactory implements vscode.DebugAdapterDescriptorFactory {
  createDebugAdapterDescriptor(): vscode.DebugAdapterDescriptor {
    const binaryPath = resolveBinaryPath(
      vscode.workspace.getConfiguration().get<string>(binaryPathSetting),
    );
    return new vscode.DebugAdapterExecutable(binaryPath);
  }
}
