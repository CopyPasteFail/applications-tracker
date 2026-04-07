import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from tracker import FollowUpEngine


class FixedDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        return cls(2026, 4, 3, tzinfo=tz or timezone.utc)


class FollowUpEngineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = FollowUpEngine(
            {
                "thresholds": {
                    "follow_up_days": 7,
                    "withdraw_days": 14,
                    "follow_up_repeat_days": 7,
                }
            }
        )

    def test_recent_follow_up_delays_withdrawal(self) -> None:
        app = {
            "appl_id": "WUR-AIP-1",
            "status": "Applied",
            "applied_date": "2026-03-20",
            "last_activity_date": "2026-03-20",
            "follow_up_sent_date": "2026-04-02",
            "follow_up_count": "1",
            "withdrawal_sent_date": "",
        }

        with patch("tracker.datetime", FixedDateTime):
            actions = self.engine.compute_actions([app])

        self.assertEqual(actions, [])

    def test_old_follow_up_allows_withdrawal_after_threshold(self) -> None:
        app = {
            "appl_id": "WUR-AIP-1",
            "status": "Applied",
            "applied_date": "2026-03-01",
            "last_activity_date": "2026-03-20",
            "follow_up_sent_date": "2026-03-20",
            "follow_up_count": "1",
            "withdrawal_sent_date": "",
        }

        with patch("tracker.datetime", FixedDateTime):
            actions = self.engine.compute_actions([app])

        self.assertEqual(len(actions), 1)
        self.assertEqual(actions[0]["type"], "withdraw")

    def test_manual_withdraw_flag_queues_withdrawal_before_threshold(self) -> None:
        app = {
            "appl_id": "WUR-AIP-2",
            "status": "Applied",
            "applied_date": "2026-04-01",
            "last_activity_date": "2026-04-02",
            "follow_up_sent_date": "",
            "follow_up_count": "0",
            "withdrawal_sent_date": "",
            "withdraw_in_next_digest": "TRUE",
        }

        with patch("tracker.datetime", FixedDateTime):
            actions = self.engine.compute_actions([app])

        self.assertEqual(len(actions), 1)
        self.assertEqual(actions[0]["type"], "withdraw")
        self.assertEqual(actions[0]["reason"], "Manual withdrawal requested for next digest")

    def test_manual_withdraw_flag_respects_terminal_statuses(self) -> None:
        app = {
            "appl_id": "WUR-AIP-3",
            "status": "Withdrawn",
            "applied_date": "2026-04-01",
            "last_activity_date": "2026-04-02",
            "follow_up_sent_date": "",
            "follow_up_count": "0",
            "withdrawal_sent_date": "",
            "withdraw_in_next_digest": "TRUE",
        }

        with patch("tracker.datetime", FixedDateTime):
            actions = self.engine.compute_actions([app])

        self.assertEqual(actions, [])

    def test_follow_up_opt_out_skips_follow_up_before_withdraw_threshold(self) -> None:
        app = {
            "appl_id": "WUR-AIP-4",
            "status": "Applied",
            "applied_date": "2026-03-27",
            "last_activity_date": "",
            "follow_up_sent_date": "",
            "follow_up_count": "0",
            "follow_up_opt_out": "yes",
            "withdrawal_sent_date": "",
        }

        with patch("tracker.datetime", FixedDateTime):
            actions = self.engine.compute_actions([app])

        self.assertEqual(actions, [])

    def test_follow_up_opt_out_still_allows_withdrawal_at_threshold_from_applied_date(self) -> None:
        app = {
            "appl_id": "WUR-AIP-5",
            "status": "Applied",
            "applied_date": "2026-03-20",
            "last_activity_date": "",
            "follow_up_sent_date": "",
            "follow_up_count": "0",
            "follow_up_opt_out": "yes",
            "withdrawal_sent_date": "",
        }

        with patch("tracker.datetime", FixedDateTime):
            actions = self.engine.compute_actions([app])

        self.assertEqual(len(actions), 1)
        self.assertEqual(actions[0]["type"], "withdraw")


if __name__ == "__main__":
    unittest.main()
