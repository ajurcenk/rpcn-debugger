import * as vscode from 'vscode';
import { binaryPathSetting, resolveBinaryPath } from './binaryPath';
import { fillLaunchConfig } from './launchConfig';

const debugType = 'redpanda-connect';

// The extension stays thin (AD-12): it registers the `redpanda-connect` debug
// type, fills a missing `pipeline` from the active editor, and starts the
// rpcn-debug binary as a DAP adapter over stdio. The binary comes from the
// `rpcnDebug.binaryPath` setting (AD-23 MVP); bundling arrives with the local
// release.
export function activate(context: vscode.ExtensionContext): void {
  context.subscriptions.push(
    vscode.debug.registerDebugAdapterDescriptorFactory(debugType, new AdapterFactory()),
    vscode.debug.registerDebugConfigurationProvider(debugType, new ConfigurationProvider()),
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

class ConfigurationProvider implements vscode.DebugConfigurationProvider {
  resolveDebugConfiguration(
    _folder: vscode.WorkspaceFolder | undefined,
    config: vscode.DebugConfiguration,
  ): vscode.DebugConfiguration {
    const editor = vscode.window.activeTextEditor;
    const activeFile = editor?.document.uri.scheme === 'file' ? editor.document.uri.fsPath : undefined;
    return fillLaunchConfig(config, debugType, activeFile) as vscode.DebugConfiguration;
  }
}
