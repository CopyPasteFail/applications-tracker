# Lifecycle Status Model Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace granular application statuses with lifecycle-only statuses stored in the sheet and used by runtime logic.

**Architecture:** `tracker_actions.py` becomes the source of truth for lifecycle status normalization and action gating. `tracker.py` consumes those helpers when grouping Gmail messages, writing Sheets rows, and running the explicit sheet migration command. Docs, tests, and Apps Script are updated to expose only `Active`, `Paused`, `Rejected`, `Withdrawn`, and `Offer`.

**Tech Stack:** Python 3, unittest/pytest, gspread/Google Sheets API through existing `SheetsClient`, Google Apps Script in `JobTracker.js`, Markdown docs.

---

## File Structure

- Modify `tracker_actions.py`: lifecycle status constants, normalization, and action-planning predicates.
- Modify `tracker.py`: imports, Gmail status extraction, grouping status merges, sheet row normalization, migration command, manage/add-linkedin status writes, Gemini prompt.
- Modify `JobTracker.js`: Apps Script menu status choices and default writes.
- Modify `README.md` and `docs/ARCHITECTURE.md`: lifecycle status model docs.
- Modify `tests/test_application_status_helpers.py`: lifecycle normalization and predicates.
- Modify `tests/test_follow_up_engine.py`: active auto-withdraw behavior including old `Interview` and `Assessment` inputs.
- Modify `tests/test_rejection_defer_reset.py`, `tests/test_sync_fail_closed.py`, and related tests that still construct `Applied` rows.
- Add `tests/test_sheet_status_migration.py`: pure coverage for sheet row mapping and validation request shape.
- Update existing outreach/extraction tests where progress signals should now collapse to `Active`.

---

### Task 1: Lock Lifecycle Status Helper Behavior With Tests

**Files:**
- Modify: `tests/test_application_status_helpers.py`
- Modify: `tests/test_follow_up_engine.py`
- Test: `tests/test_application_status_helpers.py`
- Test: `tests/test_follow_up_engine.py`

- [ ] **Step 1: Replace granular status helper tests**

Replace the import block in `tests/test_application_status_helpers.py` with:

```python
from tracker import (
    APPLICATION_STATUSES,
    is_active_application_lifecycle,
    is_paused_application_status,
    is_terminal_application_status,
    normalize_application_status,
    should_clear_deferred_until_for_status,
    status_blocks_pipeline_actions,
)
```

Replace the test class body with:

```python
class ApplicationStatusHelperTests(unittest.TestCase):
    def test_application_statuses_are_lifecycle_values_only(self) -> None:
        self.assertEqual(
            APPLICATION_STATUSES,
            ("Active", "Paused", "Rejected", "Withdrawn", "Offer"),
        )

    def test_normalize_application_status_maps_legacy_active_values_to_active(self) -> None:
        legacy_active_values = [
            "Applied",
            "Screening",
            "Interview",
            "Assessment",
            "",
            None,
            "Custom Stage",
            "active",
            " ACTIVE ",
        ]

        for raw_status in legacy_active_values:
            with self.subTest(raw_status=raw_status):
                self.assertEqual(normalize_application_status(raw_status), "Active")

    def test_normalize_application_status_preserves_lifecycle_values_title_case(self) -> None:
        expected_statuses = {
            "paused": "Paused",
            "Rejected": "Rejected",
            "WITHDRAWN": "Withdrawn",
            " offer ": "Offer",
        }

        for raw_status, expected_status in expected_statuses.items():
            with self.subTest(raw_status=raw_status):
                self.assertEqual(normalize_application_status(raw_status), expected_status)

    def test_active_lifecycle_helper_matches_lifecycle_values(self) -> None:
        self.assertTrue(is_active_application_lifecycle("Active"))
        self.assertTrue(is_active_application_lifecycle("Interview"))
        self.assertFalse(is_active_application_lifecycle("Paused"))
        self.assertFalse(is_active_application_lifecycle("Rejected"))
        self.assertFalse(is_active_application_lifecycle("Withdrawn"))
        self.assertFalse(is_active_application_lifecycle("Offer"))

    def test_pipeline_blocking_statuses_match_lifecycle_model(self) -> None:
        self.assertTrue(status_blocks_pipeline_actions("Offer"))
        self.assertTrue(status_blocks_pipeline_actions("Withdrawn"))
        self.assertTrue(status_blocks_pipeline_actions("Paused"))
        self.assertFalse(status_blocks_pipeline_actions("Rejected"))
        self.assertFalse(status_blocks_pipeline_actions("Active"))
        self.assertFalse(status_blocks_pipeline_actions("Interview"))

    def test_terminal_statuses_match_lifecycle_outcomes(self) -> None:
        self.assertTrue(is_terminal_application_status("Rejected"))
        self.assertTrue(is_terminal_application_status("Withdrawn"))
        self.assertTrue(is_terminal_application_status("Offer"))
        self.assertFalse(is_terminal_application_status("Paused"))
        self.assertFalse(is_terminal_application_status("Active"))
        self.assertFalse(is_terminal_application_status("Assessment"))

    def test_paused_and_rejection_helpers_use_lifecycle_statuses(self) -> None:
        self.assertTrue(is_paused_application_status("Paused"))
        self.assertFalse(is_paused_application_status("Rejected"))
        self.assertTrue(should_clear_deferred_until_for_status("Rejected"))
        self.assertFalse(should_clear_deferred_until_for_status("Withdrawn"))
```

- [ ] **Step 2: Replace interview/assessment follow-up test expectation**

In `tests/test_follow_up_engine.py`, rename `test_interview_and_assessment_do_not_auto_withdraw_at_threshold` to:

```python
def test_legacy_interview_and_assessment_inputs_auto_withdraw_as_active(self) -> None:
```

Keep the two apps with `status` values `Interview` and `Assessment`, but change the assertions to:

```python
self.assertEqual([action["type"] for action in actions], ["withdraw", "withdraw"])
self.assertEqual([action["app"]["appl_id"] for action in actions], ["WUR-AIP-15", "WUR-AIP-16"])
```

- [ ] **Step 3: Run tests to verify they fail against old behavior**

Run:

```powershell
pytest tests/test_application_status_helpers.py tests/test_follow_up_engine.py -q
```

Expected: failures mentioning missing `APPLICATION_STATUSES`, removed/changed imports such as `status_rank`, and the old `Interview`/`Assessment` follow-up behavior returning `follow_up` instead of `withdraw`.

- [ ] **Step 4: Commit the failing tests**

Run:

```powershell
git add tests/test_application_status_helpers.py tests/test_follow_up_engine.py
git commit -m "test: define lifecycle status behavior"
```

---

### Task 2: Implement Lifecycle Status Helpers

**Files:**
- Modify: `tracker_actions.py`
- Modify: `tracker.py`
- Test: `tests/test_application_status_helpers.py`
- Test: `tests/test_follow_up_engine.py`

- [ ] **Step 1: Replace status constants and helper functions in `tracker_actions.py`**

Replace the status type aliases and constants near the top of `tracker_actions.py` with:

```python
ActionPolicy: TypeAlias = str
ApplicationStatus: TypeAlias = Literal["Active", "Paused", "Rejected", "Withdrawn", "Offer"]
ApplicationLifecycle: TypeAlias = ApplicationStatus

APPLICATION_STATUSES: tuple[ApplicationStatus, ...] = (
    "Active",
    "Paused",
    "Rejected",
    "Withdrawn",
    "Offer",
)
DEFAULT_APPLICATION_STATUS: ApplicationStatus = "Active"
TERMINAL_APPLICATION_STATUSES: set[ApplicationStatus] = {"Rejected", "Withdrawn", "Offer"}
PIPELINE_BLOCKING_APPLICATION_STATUSES: set[ApplicationStatus] = {"Paused", "Withdrawn", "Offer"}
DEFERRED_UNTIL_CLEARING_APPLICATION_STATUSES: set[ApplicationStatus] = {"Rejected"}
LEGACY_ACTIVE_APPLICATION_STATUSES = {
    "applied",
    "screening",
    "interview",
    "assessment",
    "ghosted",
}
APPLICATION_STATUS_BY_NORMALIZED_VALUE: dict[str, ApplicationStatus] = {
    status.lower(): status for status in APPLICATION_STATUSES
}
```

Replace the existing status helper section with:

```python
def normalize_application_status(raw_status: Any) -> ApplicationStatus:
    """Return the lifecycle status stored and used by current tracker logic."""
    status = str(raw_status or "").strip()
    normalized_status = status.lower()
    if not normalized_status or normalized_status in LEGACY_ACTIVE_APPLICATION_STATUSES:
        return DEFAULT_APPLICATION_STATUS
    return APPLICATION_STATUS_BY_NORMALIZED_VALUE.get(normalized_status, DEFAULT_APPLICATION_STATUS)


def application_lifecycle_from_status(status: Any) -> ApplicationLifecycle:
    """Return the normalized lifecycle status for compatibility with older imports."""
    return normalize_application_status(status)


def is_active_application_lifecycle(lifecycle: Any) -> bool:
    """Return whether a status belongs to the active pipeline group."""
    return normalize_application_status(lifecycle) == "Active"


def is_terminal_application_status(status: Any) -> bool:
    """Return whether a status is a completed lifecycle outcome."""
    return normalize_application_status(status) in TERMINAL_APPLICATION_STATUSES


def is_paused_application_status(status: Any) -> bool:
    """Return whether a status represents a manually paused application."""
    return normalize_application_status(status) == "Paused"


def status_blocks_pipeline_actions(status: Any) -> bool:
    """Return whether normal digest pipeline actions should be skipped for this status."""
    return normalize_application_status(status) in PIPELINE_BLOCKING_APPLICATION_STATUSES


def should_clear_deferred_until_for_status(status: Any) -> bool:
    """Return whether applying this status should clear a stored deferral."""
    return normalize_application_status(status) in DEFERRED_UNTIL_CLEARING_APPLICATION_STATUSES
```

Remove `ApplicationProgressSignal`, `STATUS_RANK`, `AUTO_WITHDRAW_BLOCKING_APPLICATION_STATUSES`, `APPLICATION_LIFECYCLE_BY_STATUS`, `APPLICATION_PROGRESS_SIGNAL_BY_STATUS`, `status_rank`, `application_progress_signal_from_status`, and `status_blocks_auto_withdraw`.

- [ ] **Step 2: Update imports in `tracker.py`**

Replace the import group from `tracker_actions` so it imports:

```python
    APPLICATION_STATUSES,
    FollowUpEngine,
    TrackerError,
    action_blocks_automatic_digest,
    application_lifecycle_from_status as application_lifecycle_from_status,
    get_effective_action_policy,
    is_active_application_lifecycle as is_active_application_lifecycle,
    is_paused_application_status as is_paused_application_status,
    is_terminal_application_status,
    is_truthy_sheet_value,
    normalize_action_policy,
    normalize_application_status,
    should_clear_deferred_until_for_status,
    status_blocks_pipeline_actions as status_blocks_pipeline_actions,
```

Remove imports for `STATUS_RANK`, `application_progress_signal_from_status`, `status_blocks_auto_withdraw`, and `status_rank`.

- [ ] **Step 3: Remove auto-withdraw special casing in `tracker_actions.py`**

In both `compute_manual_review_candidates` and `compute_actions`, replace conditions shaped like:

```python
days >= self.withdraw_days
and not status_blocks_auto_withdraw(status)
and ...
```

with:

```python
days >= self.withdraw_days
and ...
```

Also remove the comment `Withdraw threshold — don't auto-withdraw if actively in Interview/Assessment`.

- [ ] **Step 4: Run helper/action tests**

Run:

```powershell
pytest tests/test_application_status_helpers.py tests/test_follow_up_engine.py -q
```

Expected: tests pass or remaining failures point to `tracker.py` imports still referencing removed helper names.

- [ ] **Step 5: Commit lifecycle helpers**

Run:

```powershell
git add tracker_actions.py tracker.py tests/test_application_status_helpers.py tests/test_follow_up_engine.py
git commit -m "refactor: use lifecycle application statuses"
```

---

### Task 3: Collapse Gmail Status Extraction And Grouping To Lifecycle Values

**Files:**
- Modify: `tracker.py`
- Modify: `tests/test_outreach_detection.py`
- Modify: `tests/test_rejection_defer_reset.py`
- Modify: `tests/test_sync_fail_closed.py`
- Test: `tests/test_outreach_detection.py`
- Test: `tests/test_rejection_defer_reset.py`
- Test: `tests/test_sync_fail_closed.py`

- [ ] **Step 1: Update deterministic status extraction**

In `extract_status_from_email_message`, change progress pattern status names from `Interview`, `Assessment`, and `Screening` to `Active`. Keep `Rejected` and `Offer` unchanged. The English pattern tuple should include:

```python
(
    "Active",
    (
        r"\binterview\b",
        r"\binterviews?\b",
        r"\bhiring manager\b",
        r"\bonsite\b",
        r"\bassessment\b",
        r"\bcoding challenge\b",
        r"\btake[- ]?home\b",
        r"\bphone screen\b",
        r"\brecruiter call\b",
        r"\bscreening call\b",
    ),
),
```

The German progress tuple should include:

```python
(
    "Active",
    (
        r"\binterview\b",
        r"\bvorstellungsgespr[äa]ch\b",
        r"\bgespr[äa]chstermin\b",
        r"\baufgabe\b",
        r"\btestaufgabe\b",
        r"\bcoding[- ]?challenge\b",
        r"\btelefoninterview\b",
        r"\btelefonat\b",
        r"\berstgespr[äa]ch\b",
    ),
),
```

- [ ] **Step 2: Remove status ranking from application merging**

In `merge_application_records`, replace:

```python
primary_status = normalize_application_status(primary_app.get("status"))
secondary_status = normalize_application_status(secondary_app.get("status"))
if status_rank(secondary_status) > status_rank(primary_status):
    merged_app["status"] = secondary_status
```

with:

```python
primary_status = normalize_application_status(primary_app.get("status"))
secondary_status = normalize_application_status(secondary_app.get("status"))
if primary_status == "Active" and secondary_status != "Active":
    merged_app["status"] = secondary_status
else:
    merged_app["status"] = primary_status
```

- [ ] **Step 3: Normalize AI and deterministic statuses during grouping**

In `AIGrouper.group_emails`, replace the `extracted_status` block around the current `ai_extracted_status` logic with:

```python
deterministic_status = extract_status_from_email_message(message_record)
ai_extracted_status = normalize_application_status(ex.get("status"))
if sender_is_ats and deterministic_status:
    extracted_status = normalize_application_status(deterministic_status)
else:
    deterministic_lifecycle_status = normalize_application_status(deterministic_status)
    extracted_status = ai_extracted_status
    if extracted_status == "Active" and deterministic_lifecycle_status != "Active":
        extracted_status = deterministic_lifecycle_status
```

Then replace any `extracted_status == "Interview"` branches with status-independent date logic:

```python
extracted_last_activity_date = choose_latest_activity_date(
    extracted_applied_date,
    deterministic_scheduled_interview_date,
    deterministic_last_activity_date,
)
```

For the matched-existing update block, replace:

```python
new_st = extracted_status
if new_st and status_rank(new_st) > status_rank(base.get("status")):
    base["status"] = normalize_application_status(new_st)
```

with:

```python
current_status = normalize_application_status(base.get("status"))
new_status = normalize_application_status(extracted_status)
if current_status == "Active" and new_status != "Active":
    base["status"] = new_status
else:
    base["status"] = current_status
```

Also remove `strongest_match_score = max(STATUS_RANK.values(), default=0) + 1` and replace it with:

```python
strongest_match_score = 1_000_000
```

- [ ] **Step 4: Update related-message status backfill**

In `_find_best_related_status`, replace ranking logic with:

```python
current_status = normalize_application_status(application_row.get("status"))
if current_status != "Active":
    return ""

for related_message in related_messages:
    candidate_status = normalize_application_status(extract_status_from_email_message(related_message))
    if candidate_status in {"Rejected", "Offer"}:
        return candidate_status
return ""
```

In `_backfill_statuses_from_gmail`, replace the final update condition with:

```python
if recovered_status and normalize_application_status(current_status) == "Active":
    normalized_application_row["status"] = normalize_application_status(recovered_status)
    recovered_status_count += 1
```

- [ ] **Step 5: Update Gemini prompt status schema**

In the prompt near `status      : "Applied"|"Screening"|"Interview"|"Assessment"|"Offer"|"Rejected"|null`, replace that line with:

```text
    status      : "Active"|"Offer"|"Rejected"|null
```

Add this instruction near the existing extraction rules:

```text
11. Use status "Active" for submitted applications and all mid-pipeline progress such as screening, recruiter calls, interviews, assessments, and coding challenges. Do not output Screening, Interview, Assessment, or Applied as statuses.
```

Renumber the following unsolicited-outreach rule if needed so the prompt remains sequential.

- [ ] **Step 6: Update tests that still expect `Applied` as current default**

In tests that construct active applications, replace `"status": "Applied"` with `"status": "Active"` unless the test intentionally verifies legacy input mapping. Keep old values only in tests named around legacy compatibility.

In `tests/test_rejection_defer_reset.py`, update AI mocked extracted statuses from `"Applied"` to `"Active"` and existing active app rows from `"Applied"` to `"Active"`.

In `tests/test_sync_fail_closed.py`, update saved successful app status fixtures from `"Applied"` to `"Active"`.

- [ ] **Step 7: Run grouping and extraction tests**

Run:

```powershell
pytest tests/test_outreach_detection.py tests/test_rejection_defer_reset.py tests/test_sync_fail_closed.py -q
```

Expected: pass after all granular status references in grouping logic are removed.

- [ ] **Step 8: Commit Gmail lifecycle collapse**

Run:

```powershell
git add tracker.py tests/test_outreach_detection.py tests/test_rejection_defer_reset.py tests/test_sync_fail_closed.py
git commit -m "refactor: collapse email statuses to lifecycle values"
```

---

### Task 4: Add Sheet Status Migration And Validation Update

**Files:**
- Modify: `tracker.py`
- Add: `tests/test_sheet_status_migration.py`
- Test: `tests/test_sheet_status_migration.py`

- [ ] **Step 1: Add tests for row migration and validation request shape**

Create `tests/test_sheet_status_migration.py` with:

```python
import unittest
from unittest.mock import Mock

from tracker import APPLICATION_STATUSES, SheetsClient


class SheetStatusMigrationTests(unittest.TestCase):
    def test_normalize_rows_converts_legacy_status_values_to_lifecycle_values(self) -> None:
        rows = [
            ["appl_id", "status", "notes"],
            ["A1", "Applied", "keep"],
            ["A2", "Interview", ""],
            ["A3", "Assessment", ""],
            ["A4", "Rejected", ""],
            ["A5", "Withdrawn", ""],
            ["A6", "Paused", ""],
            ["A7", "Offer", ""],
            ["A8", "Custom Stage", ""],
            ["A9", "", ""],
        ]

        normalized_rows = SheetsClient._normalize_rows(rows)
        status_index = normalized_rows[0].index("status")

        self.assertEqual(normalized_rows[1][status_index], "Active")
        self.assertEqual(normalized_rows[2][status_index], "Active")
        self.assertEqual(normalized_rows[3][status_index], "Active")
        self.assertEqual(normalized_rows[4][status_index], "Rejected")
        self.assertEqual(normalized_rows[5][status_index], "Withdrawn")
        self.assertEqual(normalized_rows[6][status_index], "Paused")
        self.assertEqual(normalized_rows[7][status_index], "Offer")
        self.assertEqual(normalized_rows[8][status_index], "Active")
        self.assertEqual(normalized_rows[9][status_index], "Active")
        self.assertEqual(normalized_rows[1][normalized_rows[0].index("notes")], "keep")

    def test_apply_status_validation_uses_lifecycle_statuses(self) -> None:
        client = SheetsClient.__new__(SheetsClient)
        client.ws = Mock()
        client.ws.id = 123
        client.spreadsheet = Mock()
        client._execute_with_retry = Mock(side_effect=lambda _name, operation: operation())

        client._apply_status_validation(row_count=25, headers=["appl_id", "status", "notes"])

        request_body = client.spreadsheet.batch_update.call_args.args[0]
        condition_values = request_body["requests"][0]["setDataValidation"]["rule"]["condition"]["values"]
        self.assertEqual(
            [value["userEnteredValue"] for value in condition_values],
            list(APPLICATION_STATUSES),
        )
        self.assertEqual(
            request_body["requests"][0]["setDataValidation"]["range"]["startColumnIndex"],
            1,
        )
        self.assertEqual(
            request_body["requests"][0]["setDataValidation"]["range"]["endColumnIndex"],
            2,
        )


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run migration tests to verify they fail**

Run:

```powershell
pytest tests/test_sheet_status_migration.py -q
```

Expected: failure because `_normalize_rows` does not yet normalize status values and `_apply_status_validation` does not exist.

- [ ] **Step 3: Normalize status cells in `SheetsClient._normalize_rows`**

In `tracker.py`, add `@staticmethod` immediately above `def _normalize_rows(...)` because the method only transforms the provided rows and the tests call it as a pure helper:

```python
@staticmethod
def _normalize_rows(rows: list[list[str]]) -> list[list[str]]:
```

Inside `_normalize_rows`, after `header_count = len(headers)`, add:

```python
status_column_index = headers.index("status") if "status" in headers else -1
```

Then after padding each `normalized_row`, add:

```python
if status_column_index >= 0:
    normalized_row[status_column_index] = normalize_application_status(
        normalized_row[status_column_index]
    )
```

- [ ] **Step 4: Add status validation helper**

Add this method to `SheetsClient` near `_apply_header_filter`:

```python
def _apply_status_validation(self, row_count: int, headers: list[str]) -> None:
    """
    Apply lifecycle status dropdown validation to the status column.

    The validation starts below the header and covers the represented sheet rows.
    """
    if row_count <= 1 or "status" not in headers:
        return

    status_column_index = headers.index("status")
    request_body: JsonObject = {
        "requests": [
            {
                "setDataValidation": {
                    "range": {
                        "sheetId": self.ws.id,
                        "startRowIndex": 1,
                        "endRowIndex": row_count,
                        "startColumnIndex": status_column_index,
                        "endColumnIndex": status_column_index + 1,
                    },
                    "rule": {
                        "condition": {
                            "type": "ONE_OF_LIST",
                            "values": [
                                {"userEnteredValue": status}
                                for status in APPLICATION_STATUSES
                            ],
                        },
                        "strict": True,
                        "showCustomUi": True,
                    },
                }
            }
        ]
    }
    self._execute_with_retry(
        "configure status dropdown validation",
        lambda: self.spreadsheet.batch_update(request_body),
    )
```

- [ ] **Step 5: Apply validation after sheet writes**

At the end of `_write_rows`, after all chunk updates complete, add:

```python
self._apply_status_validation(total_rows, normalized_rows[0])
```

After `reset_applications_sheet` calls `_apply_header_filter`, add:

```python
self._apply_status_validation(row_count=2, headers=list(COLUMNS))
```

- [ ] **Step 6: Add explicit migration method and CLI command**

Add this method to `SheetsClient`:

```python
def migrate_statuses_to_lifecycle(self) -> int:
    """
    Convert existing status cell values and refresh lifecycle dropdown validation.

    Returns the number of data rows whose status value changed.
    """
    rows = self._load_sheet_rows()
    normalized_rows = self._normalize_rows(rows)
    status_column_index = normalized_rows[0].index("status")
    changed_count = 0

    for before_row, after_row in zip(rows[1:], normalized_rows[1:]):
        before_value = before_row[status_column_index] if status_column_index < len(before_row) else ""
        after_value = after_row[status_column_index]
        if str(before_value or "").strip() != after_value:
            changed_count += 1

    self._write_rows(normalized_rows)
    return changed_count
```

Add this method to `Tracker`:

```python
def migrate_statuses(self) -> None:
    console.rule("[bold blue]Migrating Sheet Statuses")
    changed_count = self.sheets.migrate_statuses_to_lifecycle()
    console.print(
        f"  [green]Migrated {changed_count} status values to lifecycle statuses.[/green]"
    )
    console.print(
        "  Status dropdown now uses: "
        + ", ".join(f"[cyan]{status}[/cyan]" for status in APPLICATION_STATUSES)
    )
```

In `main()`, add the CLI flag:

```python
g.add_argument("--migrate-statuses", action="store_true", help="Convert Sheet statuses to lifecycle values and update the dropdown")
```

And route it:

```python
elif args.migrate_statuses:
    t.migrate_statuses()
```

- [ ] **Step 7: Run sheet migration tests**

Run:

```powershell
pytest tests/test_sheet_status_migration.py -q
```

Expected: pass.

- [ ] **Step 8: Commit sheet migration**

Run:

```powershell
git add tracker.py tests/test_sheet_status_migration.py
git commit -m "feat: migrate sheet statuses to lifecycle values"
```

---

### Task 5: Update User-Facing Defaults, Apps Script, And Docs

**Files:**
- Modify: `tracker.py`
- Modify: `JobTracker.js`
- Modify: `README.md`
- Modify: `docs/ARCHITECTURE.md`
- Test: `tests/test_manage_actions.py`

- [ ] **Step 1: Update CLI writes from old active status to `Active`**

In `tracker.py`, change:

```python
self.sheets.set_field(app["appl_id"], "status", "Applied")
```

to:

```python
self.sheets.set_field(app["appl_id"], "status", "Active")
```

Change the resume console message to:

```python
console.print("  [green]✓ Resumed — back in pipeline with status 'Active'[/green]")
```

In `add_linkedin`, change:

```python
"status":               "Applied",
```

to:

```python
"status":               "Active",
```

- [ ] **Step 2: Update Apps Script statuses**

In `JobTracker.js`, change the resume prompt to:

```javascript
"Clear deferral and set status back to Active?",
```

Change the resume status write to:

```javascript
setCellValue(row, "status",        "Active");
```

Change the resume alert to:

```javascript
ui.alert("Resumed. Back in pipeline with status Active.");
```

Change the status list in `menuSetStatus` to:

```javascript
const statuses = ["Active","Paused","Rejected","Withdrawn","Offer"];
```

Change the LinkedIn default to:

```javascript
row[COL.status             - 1] = "Active";
```

- [ ] **Step 3: Update README status docs**

In `README.md`, update the manage action row:

```markdown
| `resume` or `r` | Clear deferral and set status back to Active |
```

Update the status column row:

```markdown
| `status` | Active / Paused / Rejected / Withdrawn / Offer |
```

Replace the compatibility note with:

```markdown
Status note: `status` is now a lifecycle value. Older Sheet values such as `Applied`, `Screening`, `Interview`, `Assessment`, blanks, and unknown custom statuses are normalized to `Active`.
```

Add the migration command near the reset/resync maintenance commands:

````markdown
### Migrate statuses to lifecycle values

```bash
python tracker.py --migrate-statuses
```

This converts existing `status` cells to `Active`, `Paused`, `Rejected`, `Withdrawn`, or `Offer` and refreshes the Google Sheets dropdown validation. Other columns are preserved.
````

- [ ] **Step 4: Update architecture docs**

In `docs/ARCHITECTURE.md`, replace the status model bullets with:

```markdown
- Sheet `status` values are lifecycle values: `Active`, `Paused`, `Rejected`, `Withdrawn`, and `Offer`.
- Legacy status text such as `Applied`, `Screening`, `Interview`, and `Assessment` is normalized to `Active`.
- Mid-pipeline email signals can help identify application-related messages, but they do not create separate stored statuses or action behavior.
```

- [ ] **Step 5: Update manage action tests**

In `tests/test_manage_actions.py`, update expectations for resume from:

```python
tracker.sheets.set_field.assert_called_once_with(
    app["appl_id"],
    "status",
    "Applied",
)
```

to:

```python
tracker.sheets.set_field.assert_any_call(app["appl_id"], "status", "Active")
tracker.sheets.set_field.assert_any_call(app["appl_id"], "deferred_until", "")
```

Also update any active fixtures from `"Applied"` to `"Active"` unless the test is explicitly checking legacy normalization.

- [ ] **Step 6: Run docs-facing behavior tests**

Run:

```powershell
pytest tests/test_manage_actions.py -q
```

Expected: pass.

- [ ] **Step 7: Commit user-facing lifecycle updates**

Run:

```powershell
git add tracker.py JobTracker.js README.md docs/ARCHITECTURE.md tests/test_manage_actions.py
git commit -m "docs: expose lifecycle statuses"
```

---

### Task 6: Full Verification And Live Sheet Migration

**Files:**
- Runtime verification only.
- Live Google Sheet changed by `python tracker.py --migrate-statuses`.

- [ ] **Step 1: Search for stale granular status logic**

Run:

```powershell
rg -n "Screening|Interview|Assessment|Applied|STATUS_RANK|status_rank|status_blocks_auto_withdraw|application_progress_signal" tracker.py tracker_actions.py JobTracker.js README.md docs tests
```

Expected: remaining matches are only valid historical compatibility tests, email signal patterns that map to `Active`, non-status text such as test email subjects, or migration docs. There should be no user-facing dropdown/status schema or business branch using `Screening`, `Interview`, `Assessment`, or `Applied` as statuses.

- [ ] **Step 2: Run the full test suite**

Run:

```powershell
pytest -q
```

Expected: all tests pass.

- [ ] **Step 3: Run the live sheet migration**

Run:

```powershell
python tracker.py --migrate-statuses
```

Expected: command prints how many status values were migrated and confirms the dropdown values are `Active`, `Paused`, `Rejected`, `Withdrawn`, `Offer`.

- [ ] **Step 4: Verify live sheet status values**

Run this read-only verification snippet:

```powershell
@'
import json
from collections import Counter
from pathlib import Path
import yaml
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

SCOPES = [
    "https://www.googleapis.com/auth/gmail.modify",
    "https://www.googleapis.com/auth/gmail.labels",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file",
]
root = Path.cwd()
config = yaml.safe_load((root / "config.yaml").read_text())
spreadsheet_id = config["google_sheets"]["spreadsheet_id"]
sheet_name = config["google_sheets"].get("sheet_name", "Applications")
creds = Credentials.from_authorized_user_file(str(root / "credentials" / "token.json"), SCOPES)
service = build("sheets", "v4", credentials=creds, cache_discovery=False)
values = service.spreadsheets().values().get(
    spreadsheetId=spreadsheet_id,
    range=f"{sheet_name}!A:AF",
).execute().get("values", [])
headers = values[0]
status_index = headers.index("status")
statuses = [row[status_index] for row in values[1:] if len(row) > status_index and row[status_index]]
print(json.dumps(Counter(statuses).most_common(), indent=2))
'@ | python -
```

Expected: only `Active`, `Paused`, `Rejected`, `Withdrawn`, and `Offer` appear.

- [ ] **Step 5: Commit any verification-driven fixes**

If verification required fixes, run:

```powershell
git add tracker.py tracker_actions.py JobTracker.js README.md docs tests
git commit -m "fix: complete lifecycle status migration"
```

If no fixes were needed, do not create an empty commit.

---

## Self-Review Notes

- Spec coverage: tasks cover helper model, action behavior, Gmail signal collapse, sheet row migration, dropdown validation, Apps Script, docs, tests, and live migration.
- Placeholder scan: no unfinished work markers or unspecified test steps remain.
- Type consistency: `APPLICATION_STATUSES` is a tuple of Title Case lifecycle strings; `normalize_application_status` returns those values; sheet validation uses the same tuple.
