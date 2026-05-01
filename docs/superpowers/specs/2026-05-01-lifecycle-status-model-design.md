# Lifecycle Status Model Design

## Purpose

Simplify application status handling by making `status` a coarse lifecycle value everywhere it is stored, displayed, or used for business logic. Mid-pipeline details such as screening, interview, and assessment should no longer be statuses or affect behavior.

## Status Model

The sheet stores Title Case lifecycle statuses:

- `Active`
- `Paused`
- `Rejected`
- `Withdrawn`
- `Offer`

Status normalization is case-insensitive and migration-safe. Existing values map as follows:

- `Applied`, `Screening`, `Interview`, `Assessment`, blank values, and unknown values -> `Active`
- `Rejected` -> `Rejected`
- `Withdrawn` -> `Withdrawn`
- `Paused` -> `Paused`
- `Offer` -> `Offer`

Unknown values become `Active` so existing custom or stale sheet entries stay in the active pipeline unless the user explicitly sets a terminal lifecycle.

## Runtime Behavior

The digest and action planner use lifecycle statuses only:

- `Active` applications can receive follow-ups and automatic ghosted withdrawal actions.
- `Rejected` applications can receive deletion-request actions.
- `Paused`, `Withdrawn`, and `Offer` applications are excluded from normal pipeline actions.

There is no special behavior for interview, assessment, screening, or any other granular progress stage. In particular, all `Active` applications are treated the same for auto-withdraw, even if the latest email text mentions an interview or assessment.

## Email Signals

Email parsing can still detect detailed signals, but those signals collapse to lifecycle statuses before storage:

- Rejection signals -> `Rejected`
- Offer signals -> `Offer`
- Application/progress signals, including applied, screening, interview, assessment, recruiter call, coding challenge, or scheduled interview text -> `Active`

Detailed progress signals may still be useful as heuristics for identifying application-related messages or choosing dates, but they must not be persisted as statuses or branch business behavior.

## Migration Scope

The implementation should update:

- Python status helpers and action-planning logic.
- Gmail deterministic status extraction and Gemini prompts/schema expectations.
- Sheet upsert/read normalization so old rows are converted when touched or during an explicit migration command.
- Google Sheets validation/dropdown values.
- `JobTracker.js` menu options and labels.
- README and architecture docs.
- Tests that currently assert granular status ranking or interview/assessment exceptions.

The implementation should remove:

- `STATUS_RANK` progression between applied, screening, interview, and assessment.
- Progress-signal status helpers if they only exist to preserve old granular statuses.
- `Interview`/`Assessment` auto-withdraw blocking.
- User-facing dropdowns, docs, prompts, and tests that present screening/interview/assessment as statuses.

## Sheet Migration

The live sheet needs a one-time migration for existing rows and data validation:

- Convert existing status cell values to the lifecycle mapping above.
- Replace status dropdown validation with `Active`, `Paused`, `Rejected`, `Withdrawn`, `Offer`.
- Keep other columns unchanged.

This migration should be explicit and visible because it edits user data. It can be implemented as a CLI command or a guarded maintenance operation in the existing tracker workflow.

## Testing

Tests should cover:

- Status normalization from old values, blank values, unknown values, and lowercase inputs.
- Action planning for each lifecycle status.
- Rejected deletion-request behavior.
- Active follow-up and auto-withdraw behavior, including old `Interview` and `Assessment` inputs mapping to `Active`.
- Email status extraction collapsing progress signals to `Active`.
- Sheet migration mapping and dropdown configuration, with unrelated columns preserved.

## Non-Goals

- Preserve historical progress stage detail in the sheet.
- Add a replacement `progress_signal` column.
- Keep interview or assessment-specific action exceptions.
- Infer withdrawn status automatically from email text unless existing code already has a reliable explicit withdrawal signal.
