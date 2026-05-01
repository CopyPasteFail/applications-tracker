import unittest
import base64
from email import message_from_bytes
from unittest.mock import Mock, patch

from tracker import GmailClient


class GmailRetryTests(unittest.TestCase):
    def test_create_draft_retries_after_windows_connection_aborted_error(self) -> None:
        gmail = GmailClient.__new__(GmailClient)
        draft_create = Mock()
        draft_create.execute.side_effect = [
            Exception("[WinError 10053] An established connection was aborted by the software in your host machine"),
            {"id": "draft-123"},
        ]
        gmail.service = Mock()
        gmail.service.users.return_value.drafts.return_value.create.return_value = draft_create

        with patch("tracker.console.print") as console_print, patch("tracker.time.sleep") as sleep_mock:
            draft_id = gmail.create_draft("hi@example.com", "Subject", "Body")

        self.assertEqual(draft_id, "draft-123")
        self.assertEqual(draft_create.execute.call_count, 2)
        sleep_mock.assert_called_once_with(1.0)
        printed_messages = [call.args[0] for call in console_print.call_args_list]
        self.assertTrue(any("gmail draft retry" in message.lower() for message in printed_messages))

def _make_gmail_for_mime_test():
    gmail = GmailClient.__new__(GmailClient)
    gmail.service = Mock()

    create_call = (
        gmail.service.users.return_value
        .drafts.return_value
        .create
    )
    create_call.return_value.execute.return_value = {"id": "draft-1"}

    return gmail, create_call


def _decode_created_draft_message(create_call):
    body = create_call.call_args.kwargs["body"]
    raw = body["message"]["raw"]
    decoded = base64.urlsafe_b64decode(raw.encode("utf-8"))
    return message_from_bytes(decoded)


class GmailSendAsMimeTests(unittest.TestCase):
    def test_create_draft_formats_from_header_with_display_name(self) -> None:
        gmail, create_call = _make_gmail_for_mime_test()

        draft_id = gmail.create_draft(
            "to@example.com",
            "Subject",
            "Body",
            from_addr="omer@example.com",
            from_name="Omer Reznik",
        )

        self.assertEqual(draft_id, "draft-1")
        message = _decode_created_draft_message(create_call)
        self.assertEqual(message["From"], "Omer Reznik <omer@example.com>")

    def test_create_draft_uses_bare_from_header_without_display_name(self) -> None:
        gmail, create_call = _make_gmail_for_mime_test()

        draft_id = gmail.create_draft(
            "to@example.com",
            "Subject",
            "Body",
            from_addr="omer@example.com",
            from_name="",
        )

        self.assertEqual(draft_id, "draft-1")
        message = _decode_created_draft_message(create_call)
        self.assertEqual(message["From"], "omer@example.com")


if __name__ == "__main__":
    unittest.main()
