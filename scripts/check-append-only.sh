#!/usr/bin/env bash
# Fails when deferred-work.md loses or changes a line: the file is
# append-only (epic-tracer retro A10, E3 retro B8). It checks every commit
# that touched the file, and the working tree against HEAD.
set -euo pipefail
f=_bmad-output/initiative-rpcn-debugger/deferred-work.md
fail() { echo "append-only: $f: $1 removes or changes lines; append a new entry instead" >&2; exit 1; }
removed() { git diff --no-ext-diff --numstat "$@" -- "$f" | awk '{s+=$2} END {print s+0}'; }

for c in $(git log --format=%H -- "$f"); do
  git rev-parse -q --verify "$c~1" >/dev/null || continue
  [ "$(removed "$c~1" "$c")" = 0 ] || fail "commit $(git log -1 --format='%h %s' "$c")"
done
[ "$(removed HEAD)" = 0 ] || fail "the working tree"
