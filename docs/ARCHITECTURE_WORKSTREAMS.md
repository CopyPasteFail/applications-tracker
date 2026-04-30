# Architecture Workstreams

This document is a repo-local continuation note for future ChatGPT/Codex sessions. It captures the current architecture state, what has already been completed, and the safest next extraction step without relying on chat history.

## Current Architecture State

- `tracker.py` still owns the end-to-end CLI, Gmail/Sheets IO, draft creation, template rendering, contact discovery, digest execution flow, and backward-compatible imports.
- `tracker_actions.py` now owns `FollowUpEngine` plus the pure action-planning, status, lifecycle, and policy helpers extracted from `tracker.py`.
- Recent architectural work has already separated lifecycle and policy concepts from raw Sheet status strings.
- The code now distinguishes between:
  - user-visible Sheet statuses, which remain unchanged for compatibility
  - lifecycle projections, which group statuses into simpler internal categories
  - per-action digest policies, which control whether follow-up, withdrawal, and deletion-request drafts are auto-created
- Digest behavior is now intentionally action-oriented:
  - `--sync` runs Gmail ingestion only
  - `--digest` computes actions and creates drafts without syncing
  - `--daily` runs sync first, then computes actions and creates drafts
- The current validated state is clean at `3091b00`, after the helper-boundary audit and section-label cleanup landed, with full pytest, ruff, `git diff --check`, and CLI help passing.

## Completed Workstream Summary

The following commits already landed the architectural foundation and action-planning extraction:

- `dc8a887` - Add application lifecycle projection
- `a0a7f96` - Centralize application status rules
- `62640ee` - Make digest command action-only
- `c0558af` - Add explicit daily digest command
- `c28fabb` - Add per-action application policies
- `cabc9ed` - Extract action planning module
- `000995c` - Document helper boundary audit
- `3091b00` - Label action helper sections

Collectively, these commits:

- introduced lifecycle projection helpers without renaming Sheet statuses
- centralized status normalization and status rule helpers
- made the digest command compute actions only
- separated daily workflow orchestration from digest-only behavior
- added per-action policy helpers, including compatibility with legacy opt-out columns
- moved `FollowUpEngine` and pure action-planning helpers into `tracker_actions.py` while preserving user-facing behavior and backward-compatible imports from `tracker.py`

## Recommended Next Workstream

### Open Next Workstream

The helper-boundary work is complete for now. The next session should inspect the repo docs and code, then choose a new bounded workstream only if the current state clearly supports one. Do not assume a helper split is the next step; the current decision is to keep the helpers together.

## Proposed Target Extraction

- Audit the helpers now living in `tracker_actions.py` and identify whether status normalization, lifecycle projection, and per-action policy rules would be clearer in a focused module such as `tracker_status.py` or `tracker_policy.py`.
- Prefer no code movement if the current module still reads clearly after the action-planning extraction.
- If a split is justified, move only pure helper functions and types with no Gmail, Sheets, draft creation, or CLI dependencies.
- Keep `FollowUpEngine` and action planning in `tracker_actions.py`.
- Keep command behavior and user-facing Sheet statuses unchanged.
- Preserve backward-compatible imports from `tracker.py` if tests or callers still rely on them.

## Explicit Non-Goals For The Next Workstream

- Do not change Sheet statuses.
- Do not add `lifecycle_status` or `latest_signal` columns.
- Do not change the Gemini extraction schema.
- Do not change `sync`, `digest`, or `daily` behavior.
- Do not remove legacy opt-out compatibility yet.
- Do not start a broad package restructure.
- Do not move Gmail, Sheets, draft creation, template rendering, or contact discovery code.

## Suggested First Implementation Slice

1. Read `tracker_actions.py` and list the helpers that are status/lifecycle/policy-specific rather than action-planning-specific.
2. Decide whether a focused helper module would materially improve clarity; if not, leave the code as-is and update this doc with that decision.
3. If moving code, move only pure helpers first and keep imports backward-compatible from `tracker.py`.
4. Run targeted tests before broader validation.

### Audit decision at 77e7386

Helper classification in `tracker_actions.py`:

- Status / lifecycle helpers: `normalize_application_status`, `status_rank`, `application_lifecycle_from_status`, `application_progress_signal_from_status`, `is_active_application_lifecycle`, `is_terminal_application_status`, `is_paused_application_status`, `status_blocks_pipeline_actions`, `status_blocks_auto_withdraw`, `should_clear_deferred_until_for_status`
- Policy helpers: `normalize_action_policy`, `_has_explicit_action_policy`, `get_effective_action_policy`, `action_blocks_automatic_digest`
- Generic date / sheet-value helpers: `parse_iso_date`, `is_truthy_sheet_value`, `_default_today_utc`
- Action-planning / `FollowUpEngine` logic: `FollowUpEngine`, including `_get_withdraw_reference_date` and `compute_actions`

Decision: no code split yet.

Rationale: the helper clusters are still small, pure, and directly support `FollowUpEngine`, so `tracker_actions.py` remains readable enough for this stage. The current `tracker.py` imports and test coverage also show the compatibility boundary is still stable, so a module split would be premature.

Next recommended stage: minor doc/comment cleanup only, unless a later pass finds a genuinely confusing compatibility import boundary that can be tightened with a very small import-only cleanup.

## Validation Checklist

- Targeted tests for action planning, status, lifecycle, and policy helpers.
- Full `pytest`.
- `ruff`.
- `git diff --check`.
- CLI help sanity check to confirm command behavior stayed the same.

## Handoff Prompt

Paste this into a future ChatGPT/Codex session:

> Continue the applications-tracker architecture work after `3091b00`, which labeled the helper sections in `tracker_actions.py`. The helper-boundary audit is complete, no helper module split is currently justified, and `tracker_actions.py` section comments were added to make the boundary easier to read. The next session should inspect the repo docs and code before choosing a new bounded workstream. Be conservative: do not change Sheet statuses, do not add `lifecycle_status` or `latest_signal` columns, do not alter the Gemini extraction schema, do not change `sync`, `digest`, or `daily` behavior, do not move Gmail/Sheets IO, draft creation, template rendering, contact discovery, or CLI orchestration out of `tracker.py`, and keep all existing non-goals. If a future split is ever considered, it must be justified by the repo state rather than assumed up front.
