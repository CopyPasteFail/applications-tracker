import sys
import unittest
from unittest.mock import Mock, patch

from tracker import Tracker, main


class ResumeRunCliTests(unittest.TestCase):
    def test_main_routes_digest_argument_to_digest_only(self) -> None:
        with patch.object(sys, "argv", ["tracker.py", "--digest"]):
            with patch("tracker.Tracker") as tracker_cls:
                tracker_instance = tracker_cls.return_value
                main()

        tracker_instance.digest.assert_called_once_with()
        tracker_instance.run_daily_digest.assert_not_called()

    def test_main_routes_daily_argument(self) -> None:
        with patch.object(sys, "argv", ["tracker.py", "--daily"]):
            with patch("tracker.Tracker") as tracker_cls:
                tracker_instance = tracker_cls.return_value
                main()

        tracker_instance.run_daily_digest.assert_called_once_with()
        tracker_instance.digest.assert_not_called()

    def test_tracker_digest_runs_digest_only(self) -> None:
        tracker = Tracker.__new__(Tracker)
        tracker.run_digest_only = Mock()
        tracker.run_daily_digest = Mock()

        tracker.digest()

        tracker.run_digest_only.assert_called_once_with()
        tracker.run_daily_digest.assert_not_called()

    def test_main_routes_resume_run_argument(self) -> None:
        with patch.object(sys, "argv", ["tracker.py", "--resume-run", "2026-04-16T13-42-10"]):
            with patch("tracker.Tracker") as tracker_cls:
                tracker_instance = tracker_cls.return_value
                main()

        tracker_instance.resume_grouping_run.assert_called_once_with("2026-04-16T13-42-10")


if __name__ == "__main__":
    unittest.main()
