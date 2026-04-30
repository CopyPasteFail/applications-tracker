# Architecture

This document captures the durable runtime boundaries for the tracker. It is the stable reference for where behavior lives and how the main commands fit together.

## Module Boundaries

- `tracker.py` owns CLI orchestration, Gmail and Sheets I/O, draft creation, template rendering, contact discovery, digest execution flow, and compatibility imports that preserve the public entry points.
- `tracker_actions.py` owns `FollowUpEngine` plus the pure action-planning, status, lifecycle, and policy helpers.

## Status And Policy Model

- Sheet `status` values remain user-facing compatibility values.
- Lifecycle projection is internal only and groups stored statuses into simplified categories such as `active`, `paused`, `rejected`, `withdrawn`, and `offer`.
- Per-action policies control whether a due action is drafted automatically.
- Legacy opt-out columns remain compatibility inputs, but explicit policy fields take precedence.

## Command Semantics

- `--sync` runs Gmail/Sheets ingestion only.
- `--digest` computes due actions and creates drafts without syncing first.
- `--daily` runs sync first, then computes due actions and creates drafts.
- `pending_actions.json` is the automatic draft-backed send queue.
- `confirm()` is draft-send only.

## Ask-When-Due Behavior

- `ask_when_due` is currently display-only during `--digest` and `--daily`.
- It does not create drafts, persist manual-review candidates, or write to `pending_actions.json`.
- The digest can still show manual-review candidates so they are visible without changing runtime behavior.
