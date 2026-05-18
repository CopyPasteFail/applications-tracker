import unittest

from tracker import (
    SyncSummary,
    build_sync_summary,
    format_sync_summary_lines,
)


class SyncSummaryFormattingTests(unittest.TestCase):
    def test_formats_compact_summary_with_safe_created_and_updated_rows(self) -> None:
        existing = [
            {
                "appl_id": "appl_4M9P",
                "company": "Northstar Labs",
                "role": "Platform Engineer",
                "status": "Active",
                "last_activity_date": "2026-05-12",
                "contact_email": "",
                "email_ids": '["old-1"]',
            }
        ]
        updates = [
            {
                "appl_id": "appl_7K2Q",
                "company": "Acme AI",
                "role": "Backend Engineer",
                "status": "Active",
                "last_activity_date": "2026-05-17",
                "contact_email": "maya@acme.example",
                "email_ids": '["new-1", "new-2", "new-3"]',
                "body": "raw private message body",
                "snippet": "private snippet",
            },
            {
                "appl_id": "appl_4M9P",
                "company": "Northstar Labs",
                "role": "Platform Engineer",
                "status": "Rejected",
                "last_activity_date": "2026-05-17",
                "contact_email": "dana@northstar.example",
                "email_ids": '["old-1", "new-4", "new-5"]',
                "notes": "private-ish notes are not part of the summary",
            },
        ]

        summary = build_sync_summary(
            query="label:career-job-seeking",
            lookback_days=60,
            existing_applications=existing,
            total_matching_email_count=18,
            new_unseen_email_count=15,
            matched_existing_email_count=5,
            new_application_email_count=9,
            ignored_email_count=1,
            updated_existing_application_ids={"appl_4M9P"},
            created_application_ids={"appl_7K2Q"},
            all_updates=updates,
            rows_written=14,
        )

        output = "\n".join(format_sync_summary_lines(summary))

        self.assertIn("Sync summary", output)
        self.assertIn("Gmail query: label:career-job-seeking", output)
        self.assertIn("Lookback window: 60 days", output)
        self.assertIn("Existing Sheet records: 1", output)
        self.assertIn("Unprocessed Gmail matches in window: 18", output)
        self.assertIn("New unseen emails: 15", output)
        self.assertIn("Relevant emails: 14", output)
        self.assertIn("Ignored emails: 1", output)
        self.assertIn("Emails merged into shared records: 12", output)
        self.assertIn("Created: 1", output)
        self.assertIn("Updated: 1", output)
        self.assertIn("Unchanged: 0", output)
        self.assertIn("Rows written: 14", output)
        self.assertIn("NEW  appl_7K2Q  Acme AI  Backend Engineer", output)
        self.assertIn("UPD  appl_4M9P  Northstar Labs  Platform Engineer", output)
        self.assertIn("status: Active -> Rejected", output)
        self.assertIn("last_activity_date: 2026-05-12 -> 2026-05-17", output)
        self.assertIn("contact_email: empty -> dana@northstar.example", output)
        self.assertIn("email_ids: +2", output)
        self.assertNotIn("raw private message body", output)
        self.assertNotIn("private snippet", output)
        self.assertNotIn("private-ish notes", output)

    def test_formats_no_changes_when_nothing_was_written(self) -> None:
        summary = SyncSummary(
            query="label:career-job-seeking",
            lookback_days=30,
            existing_sheet_record_count=2,
            total_matching_email_count=0,
            new_unseen_email_count=0,
            relevant_email_count=0,
            ignored_email_count=0,
            merged_email_count=0,
            created_application_count=0,
            updated_application_count=0,
            unchanged_application_count=0,
            rows_written=0,
            changes=[],
        )

        output = "\n".join(format_sync_summary_lines(summary))

        self.assertIn("Rows written: 0", output)
        self.assertIn("No created or updated application rows.", output)


if __name__ == "__main__":
    unittest.main()
