import unittest
from unittest.mock import patch

from tracker import AIGrouper


class RejectionDeferResetTests(unittest.TestCase):
    def _build_grouper(self) -> AIGrouper:
        grouper = AIGrouper.__new__(AIGrouper)
        grouper.user_owned_email_addresses = set()
        return grouper

    def test_rejection_update_clears_existing_deferral(self) -> None:
        grouper = self._build_grouper()
        new_email = {
            "id": "msg-1",
            "thread_id": "thread-1",
            "internet_message_id": "<msg-1@example.com>",
            "from": "Tal Cohen <notifications@walmart.comeet-notifications.com>",
            "subject": "AI Engineer opportunity at Walmart IL",
            "snippet": "We decided to move forward with other candidates.",
            "body": (
                "Hi Omer,\n\n"
                "After evaluating them against our needs for this position, "
                "we decided to move forward with other candidates."
            ),
            "date": "Mon, 06 Apr 2026 10:05:41 +0300",
        }
        existing_app = {
            "appl_id": "WAL-DIS-1",
            "company": "Walmart IL",
            "role": "Distinguished Engineer",
            "status": "Applied",
            "source": "email",
            "applied_date": "2026-03-16",
            "last_activity_date": "2026-03-23",
            "recruiter_name": "Tal Cohen",
            "recruiter_email": "tal@example.com",
            "ats_email": "",
            "contact_email": "tal@example.com",
            "follow_up_sent_date": "",
            "follow_up_count": "0",
            "withdrawal_sent_date": "",
            "deletion_request_sent_date": "",
            "deletion_request_opt_out": "",
            "follow_up_missing_email_policy": "",
            "withdraw_missing_email_policy": "",
            "deletion_request_missing_email_policy": "",
            "deferred_until": "2026-04-07",
            "notes": "",
            "linkedin_contact": "",
            "email_ids": "[]",
            "thread_ids": "[]",
            "internet_message_ids": "[]",
            "gmail_review_url": "",
            "draft_id": "",
        }

        with patch.object(
            grouper,
            "inspect_group_emails",
            return_value={
                "results": [
                    {
                        "action": "match_existing",
                        "email_id": "msg-1",
                        "appl_id": "WAL-DIS-1",
                        "extracted": {
                            "company": "Walmart IL",
                            "role": "Distinguished Engineer",
                            "status": "Rejected",
                        },
                    }
                ],
                "missing_email_ids": [],
                "duplicate_email_ids": [],
                "unexpected_email_ids": [],
            },
        ):
            result = grouper.group_emails([new_email], [existing_app])

        self.assertEqual(len(result.updates), 1)
        self.assertEqual(result.updates[0]["status"], "Rejected")
        self.assertEqual(result.updates[0]["deferred_until"], "")


if __name__ == "__main__":
    unittest.main()
