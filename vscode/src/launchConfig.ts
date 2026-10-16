// Kept free of the vscode module so `node --test` can exercise it.

export interface LaunchConfig {
  type?: string;
  request?: string;
  name?: string;
  pipeline?: string;
  [key: string]: unknown;
}

// fillLaunchConfig returns the launch config VS Code should start. The only
// key it adds to a launch.json entry is a missing `pipeline`, set to the
// active editor's file (AD-12). Pressing F5 with no launch.json gives an
// empty config; it then also gets the type, request and name every launch
// needs. Validation stays in Go: a config still lacking `pipeline` is passed
// on, and the binary's launch error names it.
export function fillLaunchConfig(
  config: LaunchConfig,
  debugType: string,
  activeFile: string | undefined,
): LaunchConfig {
  const out: LaunchConfig = { ...config };
  if (!out.type && !out.request && !out.name) {
    out.type = debugType;
    out.request = 'launch';
    out.name = 'Debug pipeline';
  }
  if (out.pipeline === undefined && activeFile !== undefined) {
    out.pipeline = activeFile;
  }
  return out;
}
