import unittest
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


if __name__ == "__main__":
    unittest.main()
