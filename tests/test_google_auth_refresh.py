import unittest
from unittest.mock import Mock, patch

from tracker import TrackerError, _refresh_or_reauthorize_google_credentials


class GoogleAuthRefreshTests(unittest.TestCase):
    def test_refresh_or_reauthorize_returns_existing_credentials_when_refresh_succeeds(self) -> None:
        credentials = Mock()

        refreshed_credentials = _refresh_or_reauthorize_google_credentials(credentials)

        self.assertIs(refreshed_credentials, credentials)
        credentials.refresh.assert_called_once()

    @patch("tracker._run_google_oauth_flow")
    def test_refresh_or_reauthorize_starts_oauth_flow_when_refresh_token_is_invalid(
        self,
        run_google_oauth_flow_mock: Mock,
    ) -> None:
        credentials = Mock()
        credentials.refresh.side_effect = Exception(
            "('invalid_grant: Token has been expired or revoked.', {'error': 'invalid_grant'})"
        )
        replacement_credentials = Mock()
        run_google_oauth_flow_mock.return_value = replacement_credentials

        refreshed_credentials = _refresh_or_reauthorize_google_credentials(credentials)

        self.assertIs(refreshed_credentials, replacement_credentials)
        run_google_oauth_flow_mock.assert_called_once_with()

    def test_refresh_or_reauthorize_raises_tracker_error_for_other_refresh_failures(self) -> None:
        credentials = Mock()
        credentials.refresh.side_effect = Exception("temporary backend failure")

        with self.assertRaises(TrackerError) as raised_error:
            _refresh_or_reauthorize_google_credentials(credentials)

        self.assertIn("Could not refresh Google token", str(raised_error.exception))


if __name__ == "__main__":
    unittest.main()
