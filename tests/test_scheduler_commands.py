import unittest
from unittest.mock import patch

import scheduler


class SchedulerCommandTests(unittest.TestCase):
    def test_windows_daily_task_uses_daily_command(self) -> None:
        with patch("scheduler._is_current_user_windows_admin", return_value=True):
            with patch("scheduler._schtasks_create", return_value=True) as create_task:
                scheduler.setup_windows(8, 30)

        digest_call = create_task.call_args_list[1]
        self.assertEqual(digest_call.args[0], scheduler.TASK_DIGEST)
        self.assertIn("--daily", digest_call.args[1])
        self.assertNotIn("--digest", digest_call.args[1])

    def test_macos_daily_agent_uses_daily_command(self) -> None:
        with patch("scheduler._launchctl_load", return_value=True):
            with patch("scheduler._write_plist", return_value="fake.plist") as write_plist:
                scheduler.setup_macos(8, 30)

        digest_call = write_plist.call_args_list[1]
        self.assertEqual(digest_call.args[0], scheduler.PLIST_DIGEST)
        self.assertEqual(digest_call.args[1][-1], "--daily")

    def test_linux_daily_cron_uses_daily_command(self) -> None:
        with patch("scheduler._read_crontab", return_value=""):
            with patch("scheduler._write_crontab") as write_crontab:
                scheduler.setup_linux(8, 30)

        written_crontab = write_crontab.call_args.args[0]
        digest_lines = [
            line
            for line in written_crontab.splitlines()
            if "applications-tracker:digest" not in line and "digest.log" in line
        ]
        self.assertEqual(len(digest_lines), 1)
        self.assertIn("--daily", digest_lines[0])
        self.assertNotIn("--digest", digest_lines[0])


if __name__ == "__main__":
    unittest.main()
