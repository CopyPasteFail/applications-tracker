import base64
import unittest
from unittest.mock import patch

from tracker import GmailClient


class AttachmentExtractionTests(unittest.TestCase):
    def test_extract_attachment_text_includes_calendar_invite_payload(self) -> None:
        gmail = GmailClient.__new__(GmailClient)
        calendar_payload = (
            "BEGIN:VCALENDAR\r\n"
            "BEGIN:VEVENT\r\n"
            "SUMMARY:Omer Reznik and Yousaf, Adnan\r\n"
            "DTSTART;TZID=W. Europe Standard Time:20260601T150000\r\n"
            "END:VEVENT\r\n"
            "END:VCALENDAR\r\n"
        )
        payload = {
            "parts": [
                {
                    "mimeType": "text/calendar",
                    "filename": "invite.ics",
                    "body": {
                        "data": base64.urlsafe_b64encode(calendar_payload.encode()).decode(),
                    },
                }
            ]
        }

        attachment_text = gmail._extract_attachment_text("msg-123", payload)

        self.assertIn("BEGIN:VCALENDAR", attachment_text)
        self.assertIn("DTSTART;TZID=W. Europe Standard Time:20260601T150000", attachment_text)

    def test_extract_attachment_text_logs_message_id_and_filename_for_pdf_parse_failure(self) -> None:
        gmail = GmailClient.__new__(GmailClient)
        payload = {
            "parts": [
                {
                    "mimeType": "application/pdf",
                    "filename": "resume.pdf",
                    "body": {
                        "data": base64.urlsafe_b64encode(b"not-a-real-pdf").decode(),
                    },
                }
            ]
        }

        with patch("tracker.console.print") as console_print:
            attachment_text = gmail._extract_attachment_text("msg-123", payload)

        self.assertEqual(attachment_text, "")
        printed_messages = [call.args[0] for call in console_print.call_args_list]
        self.assertTrue(any("msg-123" in message for message in printed_messages))
        self.assertTrue(any("resume.pdf" in message for message in printed_messages))


if __name__ == "__main__":
    unittest.main()
