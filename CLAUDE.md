# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project state

This repository has no application source code yet: no build system, tests, README or git history. It contains BMAD Method skills (from `bmad-code-org/BMAD-METHOD`), installed under `.agents/skills/` and pinned by hash in `skills-lock.json`, plus planning artifacts. The product is a VSCode visual debugger for Redpanda Connect pipelines. Its binding architecture (ADs) is `_bmad-output/initiative-rpcn-debugger/architecture-rpcn-debugger/architecture-rpcn-debugger.md`, with the decision log in `.memlog.md` next to it. Follow its ADs when writing code. Update this file once code and tooling exist.

## BMAD skills

- The skills live in `.agents/skills/`. Each one is symlinked into `.claude/skills/` (`../../.agents/skills/<name>`) so Claude Code can find it. When a skill is added or removed in `.agents/skills/`, add or remove its symlink too.
- Treat `.agents/skills/` and `skills-lock.json` as vendored. Do not edit them by hand. Customization goes through each skill's `customize.toml` / the `bmad-customize` skill.
- The `bmad` skill is the entry point (help, next-step recommendations, setup). Its scripts run via `uv run`, and it expects a `_bmad/` folder at the project root (set up; the active initiative is `initiative-rpcn-debugger`, and outputs go to `_bmad-output/`).
