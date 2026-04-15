import json
import tempfile
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from tracker import EmailGroupingResult, GmailFetchResult, Tracker, TrackerError


class SyncFailClosedTests(unittest.TestCase):
    def test_sync_raises_and_skips_upsert_when_ai_grouping_fails(self) -> None:
        tracker = Tracker.__new__(Tracker)
        temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        tracker.grouping_run_state_dir = Path(temp_dir.name)
        tracker.cfg = {
            "gmail": {
                "query": "label:career-job-seeking",
                "lookback_days": 60,
            },
            "gemini": {},
        }
        tracker.processing_labels = Mock()
        tracker.processing_labels.root_label_name = "application-tracker"
        tracker.processing_labels.get_stage_label_name.return_value = "application-tracker/processed"
        tracker.sheets = Mock()
        tracker.sheets.consolidate_similar_applications.return_value = ([], 0)
        tracker.sheets.get_spreadsheet_url.return_value = "https://example.com/sheet"
        tracker.gmail = Mock()
        tracker.gmail.get_profile_email.return_value = "omer.big@gmail.com"
        tracker.gmail.ensure_processing_labels.return_value = {}
        tracker.gmail.get_emails.return_value = GmailFetchResult(
            total_matching_email_count=1,
            unseen_emails=[
                {
                    "id": "msg-1",
                    "timestamp": 1,
                    "thread_id": "thread-1",
                    "subject": "Hello",
                    "from": "test@example.com",
                    "body": "body",
                    "snippet": "body",
                    "attachment_text": "",
                }
            ],
        )
        tracker.ai = Mock()
        tracker.ai.group_emails.side_effect = Exception(
            "Unterminated string starting at: line 242 column 19 (char 6821)"
        )
        tracker.processed_message_state = Mock()
        tracker.processed_message_state.load.return_value = SimpleNamespace(
            processed_at_by_message_id={}
        )
        tracker._backfill_statuses_from_gmail = Mock(side_effect=AssertionError("should not backfill"))
        tracker._backfill_missing_companies_from_gmail = Mock(side_effect=AssertionError("should not backfill"))
        tracker._backfill_missing_roles_from_gmail = Mock(side_effect=AssertionError("should not backfill"))

        with patch("tracker.console.print") as console_print:
            with self.assertRaises(TrackerError) as error_context:
                tracker.sync()

        self.assertIn("AI grouping failed", str(error_context.exception))
        self.assertIn(
            "Gemini returned malformed JSON while classifying new Gmail messages.",
            str(error_context.exception),
        )
        self.assertIn("Cluster: 1 / 1", str(error_context.exception))
        self.assertIn("Emails in cluster: 1", str(error_context.exception))
        self.assertIn(
            "Parse error: Unterminated string starting at: line 242 column 19 (char 6821)",
            str(error_context.exception),
        )
        self.assertIn("Run state saved to:", str(error_context.exception))
        self.assertIn("Sync aborted before updating Sheets.", str(error_context.exception))
        self.assertIn(
            "Digest will not continue because actions could be stale.",
            str(error_context.exception),
        )
        self.assertIn("Recovery options:", str(error_context.exception))
        self.assertIn("python tracker.py --sync", str(error_context.exception))
        self.assertIn("python tracker.py --resume-run", str(error_context.exception))
        tracker.sheets.upsert_many.assert_not_called()
        printed_messages = [str(call.args[0]) for call in console_print.call_args_list]
        self.assertFalse(any("AI grouper error" in message for message in printed_messages))
        saved_run_files = list(Path(temp_dir.name).glob("*.json"))
        self.assertEqual(len(saved_run_files), 1)
        saved_payload = json.loads(saved_run_files[0].read_text(encoding="utf-8"))
        self.assertEqual(saved_payload["clusters"][0]["status"], "failed")
        self.assertEqual(
            saved_payload["clusters"][0]["error"],
            "Unterminated string starting at: line 242 column 19 (char 6821)",
        )

    def test_resume_run_finishes_saved_sync_and_deletes_cache(self) -> None:
        tracker = Tracker.__new__(Tracker)
        temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        tracker.grouping_run_state_dir = Path(temp_dir.name)
        tracker.cfg = {
            "gmail": {
                "query": "label:career-job-seeking",
                "lookback_days": 60,
            },
            "gemini": {},
        }
        tracker.processing_labels = Mock()
        tracker.processing_labels.root_label_name = "application-tracker"
        tracker.processing_labels.get_stage_label_name.return_value = "application-tracker/processed"
        tracker.sheets = Mock()
        tracker.sheets.consolidate_similar_applications.return_value = ([], 0)
        tracker.sheets.get_spreadsheet_url.return_value = "https://example.com/sheet"
        tracker.gmail = Mock()
        tracker.gmail.get_profile_email.return_value = "omer.big@gmail.com"
        tracker.gmail.ensure_processing_labels.return_value = {
            "application-tracker": "root-label-id",
            "application-tracker/processed": "processed-label-id",
        }
        tracker.gmail.get_emails.return_value = GmailFetchResult(
            total_matching_email_count=1,
            unseen_emails=[
                {
                    "id": "msg-1",
                    "timestamp": 1,
                    "thread_id": "thread-1",
                    "subject": "Hello",
                    "from": "recruiter@example.com",
                    "body": "body",
                    "snippet": "body",
                    "attachment_text": "",
                    "internet_message_id": "<msg-1@example.com>",
                }
            ],
        )
        tracker.ai = Mock()
        tracker.ai.group_emails.side_effect = [
            Exception("Unterminated string starting at: line 242 column 19 (char 6821)"),
            EmailGroupingResult(
                updates=[
                    {
                        "appl_id": "APP-1",
                        "company": "Example",
                        "role": "Engineer",
                        "status": "Applied",
                        "source": "email",
                        "applied_date": "2026-04-16",
                        "last_activity_date": "2026-04-16",
                        "recruiter_name": "Recruiter",
                        "recruiter_email": "recruiter@example.com",
                        "ats_email": "",
                        "contact_email": "recruiter@example.com",
                        "follow_up_sent_date": "",
                        "follow_up_count": "0",
                        "withdrawal_sent_date": "",
                        "follow_up_opt_out": "",
                        "withdraw_in_next_digest": "",
                        "notes": "",
                        "linkedin_contact": "",
                        "email_ids": "[\"msg-1\"]",
                        "thread_ids": "[\"thread-1\"]",
                        "internet_message_ids": "[\"<msg-1@example.com>\"]",
                        "gmail_review_url": "",
                        "draft_id": "",
                    }
                ],
                ignored_email_count=0,
                handled_message_ids=["msg-1"],
                matched_existing_email_count=0,
                new_application_email_count=1,
                updated_existing_application_count=0,
                created_application_count=1,
            ),
        ]
        tracker.processed_message_state = Mock()
        tracker.processed_message_state.load.return_value = SimpleNamespace(
            processed_at_by_message_id={}
        )
        tracker.processed_message_state.record_processed_message_ids.return_value = SimpleNamespace(
            processed_at_by_message_id={"msg-1": "2026-04-16T00:00:00+00:00"}
        )
        tracker._backfill_statuses_from_gmail = Mock(side_effect=lambda updates, base_query: updates)
        tracker._backfill_missing_companies_from_gmail = Mock(side_effect=lambda updates, base_query: updates)
        tracker._backfill_missing_roles_from_gmail = Mock(side_effect=lambda updates, base_query: updates)
        tracker._run_digest_after_sync = Mock()

        with self.assertRaises(TrackerError):
            tracker.sync()

        saved_run_files = list(Path(temp_dir.name).glob("*.json"))
        self.assertEqual(len(saved_run_files), 1)
        run_id = saved_run_files[0].stem

        tracker.resume_grouping_run(run_id)

        tracker.sheets.upsert_many.assert_called_once()
        tracker.gmail.apply_labels_to_messages.assert_called_once()
        tracker.processed_message_state.record_processed_message_ids.assert_called_once()
        self.assertFalse(saved_run_files[0].exists())
        tracker._run_digest_after_sync.assert_not_called()


if __name__ == "__main__":
    unittest.main()
