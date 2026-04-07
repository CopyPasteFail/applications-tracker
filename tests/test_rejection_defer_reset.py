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

    def test_new_application_uses_deterministic_company_and_role_when_ai_fields_are_unknown(self) -> None:
        grouper = self._build_grouper()
        new_email = {
            "id": "msg-2",
            "thread_id": "thread-2",
            "internet_message_id": "<msg-2@example.com>",
            "from": "Oversight Recruiting <no-reply@greenhouse.io>",
            "subject": "Oversight! We've received your application",
            "snippet": "We've received your application for the Platform Engineer position at Oversight.",
            "body": (
                "Hi Omer,\n\n"
                "We've received your application for the Platform Engineer position at "
                "Oversight. Our team will review it and get back to you."
            ),
            "date": "Tue, 07 Apr 2026 10:05:41 +0300",
        }

        with patch.object(
            grouper,
            "inspect_group_emails",
            return_value={
                "results": [
                    {
                        "action": "new_application",
                        "email_id": "msg-2",
                        "extracted": {
                            "company": "Unknown",
                            "role": "Unknown",
                            "status": "Applied",
                        },
                    }
                ],
                "missing_email_ids": [],
                "duplicate_email_ids": [],
                "unexpected_email_ids": [],
            },
        ):
            result = grouper.group_emails([new_email], [])

        self.assertEqual(len(result.updates), 1)
        self.assertEqual(result.updates[0]["company"], "Oversight")
        self.assertEqual(result.updates[0]["role"], "Platform Engineer")

    def test_reply_on_existing_application_thread_overrides_outreach_ignore(self) -> None:
        grouper = self._build_grouper()
        new_email = {
            "id": "msg-3",
            "thread_id": "thread-existing-1",
            "internet_message_id": "<msg-3@example.com>",
            "from": "Jorden Chan <jchan@client-server.com>",
            "subject": "Re: Following Up - Senior Platform Engineer AWS IaC Application",
            "snippet": "They have actually closed the position now.",
            "body": (
                "Hi Omer,\n\n"
                "Thanks for following up. My apologies for not coming back to you, they have "
                "actually closed the position now.\n"
            ),
            "date": "Fri, 27 Mar 2026 12:14:00 +0000",
        }
        existing_app = {
            "appl_id": "CSR-SPE-1",
            "company": "Client Server",
            "role": "Senior Platform Engineer AWS IaC",
            "status": "Applied",
            "source": "email",
            "applied_date": "2026-03-20",
            "last_activity_date": "2026-03-26",
            "recruiter_name": "Jorden Chan",
            "recruiter_email": "jchan@client-server.com",
            "ats_email": "",
            "contact_email": "jchan@client-server.com",
            "follow_up_sent_date": "2026-03-26",
            "follow_up_count": "1",
            "withdrawal_sent_date": "",
            "deletion_request_sent_date": "",
            "deletion_request_opt_out": "",
            "follow_up_missing_email_policy": "",
            "withdraw_missing_email_policy": "",
            "deletion_request_missing_email_policy": "",
            "deferred_until": "2026-04-02",
            "notes": "",
            "linkedin_contact": "",
            "email_ids": "[\"msg-0\"]",
            "thread_ids": "[\"thread-existing-1\"]",
            "internet_message_ids": "[\"<msg-0@example.com>\"]",
            "gmail_review_url": "",
            "draft_id": "",
        }

        with patch.object(
            grouper,
            "inspect_group_emails",
            return_value={
                "results": [
                    {
                        "action": "ignore",
                        "email_id": "msg-3",
                        "extracted": {
                            "company": "",
                            "role": "",
                            "status": "",
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
        self.assertEqual(result.updates[0]["appl_id"], "CSR-SPE-1")
        self.assertEqual(result.updates[0]["status"], "Rejected")
        self.assertEqual(result.updates[0]["deferred_until"], "")


if __name__ == "__main__":
    unittest.main()
