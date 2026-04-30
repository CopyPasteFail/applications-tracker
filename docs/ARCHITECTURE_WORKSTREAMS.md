# Architecture Workstreams

This document is a repo-local continuation note for future ChatGPT/Codex sessions. It captures the current architecture state, what has already been completed, and the safest next extraction step without relying on chat history.

## Current Architecture State

- `tracker.py` still owns the end-to-end CLI, Gmail/Sheets IO, draft creation, and the digest execution flow.
- Recent architectural work has already separated lifecycle and policy concepts from raw Sheet status strings.
- The code now distinguishes between:
  - user-visible Sheet statuses, which remain unchanged for compatibility
  - lifecycle projections, which group statuses into simpler internal categories
  - per-action digest policies, which control whether follow-up, withdrawal, and deletion-request drafts are auto-created
- Digest behavior is now intentionally action-oriented:
  - `--sync` runs Gmail ingestion only
  - `--digest` computes actions and creates drafts without syncing
  - `--daily` runs sync first, then computes actions and creates drafts
- The current validated state was clean after `dc8a887`, with full pytest, ruff, and `git diff --check` passing.

## Completed Workstream Summary

The following commits already landed the architectural foundation for the next extraction step:

- `dc8a887` - Add application lifecycle projection
- `a0a7f96` - Centralize application status rules
- `62640ee` - Make digest command action-only
- `c0558af` - Add explicit daily digest command
- `c28fabb` - Add per-action application policies

Collectively, these commits:

- introduced lifecycle projection helpers without renaming Sheet statuses
- centralized status normalization and status rule helpers
- made the digest command compute actions only
- separated daily workflow orchestration from digest-only behavior
- added per-action policy helpers, including compatibility with legacy opt-out columns

## Recommended Next Workstream

### Extract action planning from `tracker.py`

This is the next recommended slice because it is lower risk than changing user-facing statuses and it builds directly on the new action policy helpers and lifecycle projection. It also makes any future status simplification safer by shrinking the amount of digest logic that remains coupled to CLI and IO code.

## Proposed Target Extraction

- Move `FollowUpEngine` and the pure action-planning helpers into a small module, likely `tracker_actions.py`.
- Use a package only if that becomes clearly justified by the amount of shared logic or future modules.
- Keep Gmail IO, Sheets IO, draft creation, and command orchestration in `tracker.py` for now.
- Keep command behavior unchanged during this extraction.
- Preserve backward-compatible imports if tests or callers still import action-planning symbols from `tracker.py`.

## Explicit Non-Goals For The Next Workstream

- Do not change Sheet statuses.
- Do not add `lifecycle_status` or `latest_signal` columns.
- Do not change the Gemini extraction schema.
- Do not change `sync`, `digest`, or `daily` behavior.
- Do not remove legacy opt-out compatibility yet.

## Suggested First Implementation Slice

1. Move pure helpers and types first only if import cycles stay manageable.
2. Move `FollowUpEngine` with tests.
3. Keep public imports backward-compatible from `tracker.py` if tests or code still depend on them.

## Validation Checklist

- Targeted tests for action planning and status helpers.
- Full `pytest`.
- `ruff`.
- `git diff --check`.
- CLI help sanity check to confirm command behavior stayed the same.

## Handoff Prompt

Paste this into a future ChatGPT/Codex session:

> Continue the applications-tracker architecture work by extracting action planning from `tracker.py` into a small module, likely `tracker_actions.py`. Keep IO, Gmail/Sheets access, and draft creation in `tracker.py` for now. Preserve all current command behavior, do not change Sheet statuses, do not add `lifecycle_status` or `latest_signal` columns, do not alter the Gemini extraction schema, and do not remove legacy opt-out compatibility yet. Start with pure helpers only if import cycles are manageable, then move `FollowUpEngine` with tests, and keep backward-compatible imports from `tracker.py` if needed. Validate with targeted action-planning tests, full pytest, ruff, `git diff --check`, and CLI help sanity.
