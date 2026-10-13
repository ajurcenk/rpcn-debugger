// Kept free of the vscode module so `node --test` can exercise it.

export const binaryPathSetting = 'rpcnDebug.binaryPath';

// resolveBinaryPath returns the rpcn-debug binary to start from the
// `rpcnDebug.binaryPath` setting's value (AD-23 MVP). It throws when the
// setting is unset or blank; VS Code shows the message and starts no adapter.
export function resolveBinaryPath(setting: string | undefined): string {
  const binaryPath = (setting ?? '').trim();
  if (binaryPath === '') {
    throw new Error(
      `Set "${binaryPathSetting}" to the path of the rpcn-debug binary to debug Redpanda Connect pipelines.`,
    );
  }
  return binaryPath;
}
