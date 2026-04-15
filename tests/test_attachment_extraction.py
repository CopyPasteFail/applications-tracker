import base64
import unittest
from unittest.mock import patch

from tracker import GmailClient


class AttachmentExtractionTests(unittest.TestCase):
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
