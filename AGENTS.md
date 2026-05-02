# Repository Guidelines

## Project Overview

- This repo is a Python job application tracker that syncs Gmail into Google Sheets, uses Gemini for grouping/contact discovery, and drafts Gmail follow-up, withdrawal, and deletion-request emails.
- Main entry points are `tracker.py` for CLI/runtime behavior, `tracker_actions.py` for pure status/policy/action planning, `scheduler.py` for OS scheduling, and `JobTracker.js` for the Google Sheets Apps Script menu.

## Repo Layout

- `tracker.py`: CLI orchestration, Gmail/Sheets I/O, draft creation, templates, grouping, runtime state, and compatibility imports.
- `tracker_actions.py`: lifecycle status helpers, per-action policy helpers, and `FollowUpEngine`.
- `scheduler.py`: Windows Task Scheduler, macOS launchd, and Linux cron setup/removal.
- `templates/*.txt`: editable email templates.
- `tests/`: pytest-discovered `unittest` modules covering tracker behavior.
- `docs/ARCHITECTURE.md`: read before changing module boundaries, status semantics, policies, or command behavior.
- Treat `.venv/`, `__pycache__/`, `.pytest_cache/`, `.ruff_cache/`, `logs/`, and generated/runtime state as non-source.

## Commands

Ask first before dependency installation because it may use the network:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r dev-requirements.txt
```

Safe local validation from the repo root:

```powershell
python -m ruff check .
python -m pytest
python scripts\run-pre-push-checks.py
```

Run a targeted test module from the repo root:

```powershell
python -m pytest tests\test_follow_up_engine.py
```

CI runs `ruff check .`, `pytest`, and `pip-audit --requirement requirements.txt` on Python 3.13. No dedicated format or type-check command was found; do not invent one.

Local runtime commands such as `python tracker.py --sync`, `--digest`, `--daily`, `--confirm`, `--manage`, `--add-linkedin`, and `python scheduler.py` touch Gmail, Sheets, drafts, scheduler state, or local runtime files. Run them only when the task explicitly calls for live operational behavior.

## Validation Before Completion

- For code changes, run `python -m ruff check .` and the relevant pytest target; run full `python -m pytest` when behavior spans multiple areas.
- For status, policy, digest, Sheet-column, or command-flow changes, include tests near the changed behavior in `tests/`.
- Before saying work is complete, report exact commands run, pass/fail result, skipped checks with reasons, and `git --no-pager status --short` when git is available.

## Safety And Approval Boundaries

Allowed without asking:

- Read/search source, docs, tests, safe templates, and config examples.
- Edit repo source, tests, docs, and `config.example.yaml` when directly relevant.
- Run safe validation commands listed above.

Ask first:

- Install or upgrade dependencies, change lock/package/dependency metadata, or run networked shell commands.
- Edit `templates/*.txt` if wording affects real outgoing emails.
- Run live Gmail/Sheets/Gemini commands, scheduler setup/removal, or Apps Script changes.
- Delete files, regenerate runtime artifacts, or write outside the repo.
- Modify `.githooks/` or hook installation behavior.

Never unless explicitly approved:

- Commit, push, force-push, rebase, reset, destructive checkout, or bypass hooks with `--no-verify`.
- Modify secrets, credentials, private keys, real environment/config files, or credential files such as `config.yaml` and `credentials/*`. Safe examples may be edited only if they contain no real secrets.
- Use production credentials or send/delete real Gmail drafts/messages without clear user approval.
- Hide failing tests, lint, audit, or CI by weakening checks.
- Rewrite unrelated areas for a small task.

## Testing Conventions

- Tests are pytest-discovered `unittest` files under `tests/`.
- Match tests to the changed area, for example lifecycle/policy changes in `test_application_status_helpers.py`, `test_follow_up_engine.py`, and `test_sheet_status_migration.py`; CLI/scheduler behavior in `test_resume_run_cli.py` and `test_scheduler_commands.py`; Gmail/Gemini retry behavior in the corresponding retry/fallback tests.
- Prefer mocking external Google/Gemini services; do not require live credentials for tests.

## Architecture And Code Conventions

- Preserve the module boundaries in `docs/ARCHITECTURE.md`: `tracker.py` owns orchestration and I/O, while `tracker_actions.py` owns pure action planning and status/policy helpers.
- Google Sheets columns are resolved by header label, not position. Preserve this contract in Python and `JobTracker.js`; `app_id` remains a compatibility alias for `appl_id`.
- Lifecycle statuses are only `Active`, `Paused`, `Rejected`, `Withdrawn`, and `Offer`. Legacy active pipeline values normalize to `Active`.
- Per-action policies are `enabled`, `disabled`, and `ask_when_due`; blank/unrecognized values default to `enabled`.
- `ask_when_due` is display-only during digest/daily flows: it should not create drafts or write to `pending_actions.json`.
- Keep `pending_actions.json` as the automatic draft-backed send queue; `confirm()` sends drafts only.

## Docs

- Update `README.md` for user-visible commands, setup, Sheet columns, or workflow changes. Update `docs/ARCHITECTURE.md` when durable runtime boundaries, status semantics, or command semantics change.

Nested `AGENTS.md` files are not needed right now; this repo has one stack and one validation path.
