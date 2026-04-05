import unittest

from tracker import (
    extract_scheduled_interview_date_from_email_message,
    has_interview_schedule_signal,
)


class InterviewScheduleDateExtractionTests(unittest.TestCase):
    def test_extracts_calendar_style_month_name_invite_date(self) -> None:
        email_message = {
            "subject": "Interview - Site Reliability Engineer (m/f/d) / Solactive AG",
            "snippet": "Tue, Mar 31 • 2:00 PM – 2:30 PM",
            "body": (
                "Your application at Solactive AG\n"
                "Tue, Mar 31 • 2:00 PM – 2:30 PM\n"
                "https://teams.microsoft.com/l/meetup-join/abc\n"
                "Solactive AG - Organizer\n"
            ),
            "attachment_text": "",
            "date": "Thu, 27 Mar 2026 09:00:00 +0000",
            "timestamp": 1774602000,
        }

        self.assertEqual(
            extract_scheduled_interview_date_from_email_message(email_message),
            "2026-03-31",
        )

    def test_detects_cancellation_as_schedule_signal(self) -> None:
        email_message = {
            "subject": "Updated invitation: Interview with Solactive AG",
            "snippet": "This meeting has been canceled",
            "body": "The Microsoft Teams interview has been cancelled.",
            "attachment_text": "",
            "date": "Mon, 30 Mar 2026 10:00:00 +0000",
            "timestamp": 1774864800,
        }

        self.assertTrue(has_interview_schedule_signal(email_message))


if __name__ == "__main__":
    unittest.main()
