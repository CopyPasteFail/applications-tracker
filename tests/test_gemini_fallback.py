import unittest
from unittest.mock import Mock, patch

from tracker import AIGrouper, TrackerError


class GeminiFallbackTests(unittest.TestCase):
    def test_generate_content_retries_same_model_after_transport_disconnect(self) -> None:
        grouper = AIGrouper.__new__(AIGrouper)
        grouper.model_names = ["gemini-3-flash-preview"]
        grouper.client = Mock()
        grouper.client.models.generate_content.side_effect = [
            Exception("Server disconnected without sending a response."),
            "ok",
        ]

        with patch("tracker.console.print") as console_print, patch("tracker.time.sleep") as sleep_mock:
            response = grouper._generate_content("prompt", Mock())

        self.assertEqual(response, "ok")
        self.assertEqual(grouper.client.models.generate_content.call_count, 2)
        sleep_mock.assert_called_once_with(1.0)
        printed_messages = [call.args[0] for call in console_print.call_args_list]
        self.assertTrue(any("transport disconnect" in message.lower() for message in printed_messages))
        self.assertTrue(any("retry 2 of 2" in message.lower() for message in printed_messages))

    def test_generate_content_falls_back_after_repeated_transport_disconnects(self) -> None:
        grouper = AIGrouper.__new__(AIGrouper)
        grouper.model_names = ["gemini-3-flash-preview", "gemini-2.5-flash"]
        grouper.client = Mock()
        grouper.client.models.generate_content.side_effect = [
            Exception("Server disconnected without sending a response."),
            Exception("Server disconnected without sending a response."),
            "ok",
        ]

        with patch("tracker.console.print") as console_print, patch("tracker.time.sleep") as sleep_mock:
            response = grouper._generate_content("prompt", Mock())

        self.assertEqual(response, "ok")
        self.assertEqual(grouper.client.models.generate_content.call_count, 3)
        self.assertEqual(sleep_mock.call_count, 1)
        printed_messages = [call.args[0] for call in console_print.call_args_list]
        self.assertTrue(any("falling back to gemini-2.5-flash" in message.lower() for message in printed_messages))

    def test_generate_content_retries_after_windows_connection_aborted_error(self) -> None:
        grouper = AIGrouper.__new__(AIGrouper)
        grouper.model_names = ["gemini-3-flash-preview"]
        grouper.client = Mock()
        grouper.client.models.generate_content.side_effect = [
            Exception("[WinError 10053] An established connection was aborted by the software in your host machine"),
            "ok",
        ]

        with patch("tracker.console.print") as console_print, patch("tracker.time.sleep") as sleep_mock:
            response = grouper._generate_content("prompt", Mock())

        self.assertEqual(response, "ok")
        self.assertEqual(grouper.client.models.generate_content.call_count, 2)
        sleep_mock.assert_called_once_with(1.0)
        printed_messages = [call.args[0] for call in console_print.call_args_list]
        self.assertTrue(any("transport disconnect" in message.lower() for message in printed_messages))

    def test_generate_content_falls_back_to_next_model_after_high_demand_503(self) -> None:
        grouper = AIGrouper.__new__(AIGrouper)
        grouper.model_names = ["gemini-3.1-flash-lite-preview", "gemini-2.5-flash"]
        grouper.client = Mock()
        grouper.client.models.generate_content.side_effect = [
            Exception(
                "503 UNAVAILABLE. {'error': {'code': 503, 'message': "
                "'This model is currently experiencing high demand. Spikes in demand are "
                "usually temporary. Please try again later.', 'status': 'UNAVAILABLE'}}"
            ),
            "ok",
        ]

        with patch("tracker.console.print") as console_print:
            response = grouper._generate_content("prompt", Mock())

        self.assertEqual(response, "ok")
        self.assertEqual(grouper.client.models.generate_content.call_count, 2)
        console_print.assert_called_once()
        printed_message = console_print.call_args.args[0]
        self.assertIn("gemini-3.1-flash-lite-preview", printed_message)
        self.assertIn("gemini-2.5-flash", printed_message)
        self.assertIn("high demand", printed_message.lower())

    def test_generate_content_keeps_non_transient_503_as_tracker_error(self) -> None:
        grouper = AIGrouper.__new__(AIGrouper)
        grouper.model_names = ["gemini-3.1-flash-lite-preview", "gemini-2.5-flash"]
        grouper.client = Mock()
        grouper.client.models.generate_content.side_effect = Exception(
            "503 INTERNAL. {'error': {'code': 503, 'message': 'backend misconfigured'}}"
        )

        with self.assertRaises(TrackerError):
            grouper._generate_content("prompt", Mock())


if __name__ == "__main__":
    unittest.main()
