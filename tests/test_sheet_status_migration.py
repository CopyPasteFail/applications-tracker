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

    def test_migrate_statuses_to_lifecycle_counts_changed_legacy_statuses(self) -> None:
        rows = [
            ["appl_id", "status", "notes"],
            ["A1", "Applied", "legacy applied"],
            ["A2", "Interview", "legacy interview"],
            ["A3", "Rejected", "already lifecycle"],
            ["A4", "", "blank legacy"],
        ]
        client = SheetsClient.__new__(SheetsClient)
        client.ws = Mock()
        client.ws.get_all_values.return_value = rows
        client._execute_with_retry = Mock(side_effect=lambda _name, operation: operation())
        client._write_rows = Mock()

        changed_count = client.migrate_statuses_to_lifecycle()

        self.assertEqual(changed_count, 3)
        written_rows = client._write_rows.call_args.args[0]
        status_index = written_rows[0].index("status")
        self.assertEqual(
            [row[status_index] for row in written_rows[1:]],
            ["Active", "Active", "Rejected", "Active"],
        )


if __name__ == "__main__":
    unittest.main()
