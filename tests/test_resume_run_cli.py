import sys
import unittest
from unittest.mock import patch

from tracker import main


class ResumeRunCliTests(unittest.TestCase):
    def test_main_routes_resume_run_argument(self) -> None:
        with patch.object(sys, "argv", ["tracker.py", "--resume-run", "2026-04-16T13-42-10"]):
            with patch("tracker.Tracker") as tracker_cls:
                tracker_instance = tracker_cls.return_value
                main()

        tracker_instance.resume_grouping_run.assert_called_once_with("2026-04-16T13-42-10")


if __name__ == "__main__":
    unittest.main()
