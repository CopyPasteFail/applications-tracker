import unittest
from unittest.mock import Mock

from tracker import APPLICATION_STATUSES, COLUMNS, SheetsClient


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
            ["appl_id", "status", "notes", "custom_formula"],
            ["A1", "Applied", "legacy applied", "=CUSTOM(A2)"],
            ["A2", "Interview", "legacy interview", "=CUSTOM(A3)"],
            ["A3", "Rejected", "already lifecycle", "=CUSTOM(A4)"],
            ["A4", "", "blank legacy", "=CUSTOM(A5)"],
        ]
        client = SheetsClient.__new__(SheetsClient)
        client.ws = Mock()
        client.ws.get_all_values.return_value = rows
        client._execute_with_retry = Mock(side_effect=lambda _name, operation: operation())
        client._write_rows = Mock()
        client._apply_status_validation = Mock()

        changed_count = client.migrate_statuses_to_lifecycle()

        self.assertEqual(changed_count, 3)
        client._write_rows.assert_not_called()
        self.assertEqual(
            [
                (call.kwargs["range_name"], call.kwargs["values"])
                for call in client.ws.update.call_args_list
            ],
            [
                ("B2", [["Active"]]),
                ("B3", [["Active"]]),
                ("B5", [["Active"]]),
            ],
        )
        client._apply_status_validation.assert_called_once_with(
            row_count=5,
            headers=["appl_id", "status", "notes", "custom_formula"],
        )

    def test_migrate_statuses_to_lifecycle_validates_when_no_statuses_changed(self) -> None:
        rows = [
            ["appl_id", "status", "custom_formula"],
            ["A1", "Active", "=CUSTOM(A2)"],
            ["A2", "Rejected", "=CUSTOM(A3)"],
        ]
        client = SheetsClient.__new__(SheetsClient)
        client.ws = Mock()
        client.ws.get_all_values.return_value = rows
        client._execute_with_retry = Mock(side_effect=lambda _name, operation: operation())
        client._write_rows = Mock()
        client._apply_status_validation = Mock()

        changed_count = client.migrate_statuses_to_lifecycle()

        self.assertEqual(changed_count, 0)
        client._write_rows.assert_not_called()
        client.ws.update.assert_not_called()
        client._apply_status_validation.assert_called_once_with(
            row_count=3,
            headers=["appl_id", "status", "custom_formula"],
        )

    def test_load_sheet_rows_does_not_rewrite_status_only_normalization(self) -> None:
        headers = list(COLUMNS) + ["custom_formula"]
        row = [""] * len(headers)
        row[headers.index("appl_id")] = "A1"
        row[headers.index("status")] = "Applied"
        row[headers.index("custom_formula")] = "=CUSTOM(A2)"
        rows = [
            headers,
            row,
        ]
        client = SheetsClient.__new__(SheetsClient)
        client.ws = Mock()
        client.ws.get_all_values.return_value = rows
        client._execute_with_retry = Mock(side_effect=lambda _name, operation: operation())
        client._write_rows = Mock()

        loaded_rows = client._load_sheet_rows()

        self.assertEqual(loaded_rows[1][loaded_rows[0].index("status")], "Active")
        client._write_rows.assert_not_called()

    def test_upsert_many_uses_appl_id_header_when_columns_move(self) -> None:
        rows = [
            ["company", "appl_id", "status", "notes"],
            ["Old Co", "APP-1", "Active", "keep"],
        ]
        client = SheetsClient.__new__(SheetsClient)
        client._load_sheet_rows = Mock(return_value=rows)
        client._write_rows = Mock()

        client.upsert_many([
            {
                "appl_id": "APP-1",
                "company": "New Co",
                "status": "Rejected",
                "notes": "updated",
            }
        ])

        written_rows = client._write_rows.call_args.args[0]
        headers = written_rows[0]
        updated_row = dict(zip(headers, written_rows[1]))
        self.assertEqual(headers[:4], ["company", "appl_id", "status", "notes"])
        self.assertEqual(updated_row["company"], "New Co")
        self.assertEqual(updated_row["appl_id"], "APP-1")
        self.assertEqual(updated_row["status"], "Rejected")
        self.assertEqual(updated_row["notes"], "updated")

    def test_set_field_uses_appl_id_header_when_columns_move(self) -> None:
        rows = [
            ["company", "appl_id", "status", "notes"],
            ["Acme", "APP-1", "Active", ""],
        ]
        client = SheetsClient.__new__(SheetsClient)
        client._load_sheet_rows = Mock(return_value=rows)
        client.ws = Mock()
        client._execute_with_retry = Mock(side_effect=lambda _name, operation: operation())

        client.set_field("APP-1", "status", "Rejected")

        client.ws.update.assert_called_once_with(
            range_name="C2",
            values=[["Rejected"]],
        )


if __name__ == "__main__":
    unittest.main()
