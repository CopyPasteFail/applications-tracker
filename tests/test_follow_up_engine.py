import unittest
from datetime import datetime, timezone

from tracker import (
    FollowUpEngine,
    action_blocks_automatic_digest,
    get_effective_action_policy,
    normalize_action_policy,
)


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
            },
            today_provider=lambda: FixedDateTime.now(timezone.utc).date(),
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

        actions = self.engine.compute_actions([app])

        self.assertEqual(len(actions), 1)
        self.assertEqual(actions[0]["type"], "withdraw")

    def test_interview_and_assessment_do_not_auto_withdraw_at_threshold(self) -> None:
        apps = [
            {
                "appl_id": "WUR-AIP-15",
                "status": "Interview",
                "applied_date": "2026-03-01",
                "last_activity_date": "2026-03-01",
                "follow_up_sent_date": "",
                "follow_up_count": "0",
                "withdrawal_sent_date": "",
            },
            {
                "appl_id": "WUR-AIP-16",
                "status": "Assessment",
                "applied_date": "2026-03-01",
                "last_activity_date": "2026-03-01",
                "follow_up_sent_date": "",
                "follow_up_count": "0",
                "withdrawal_sent_date": "",
            },
        ]

        actions = self.engine.compute_actions(apps)

        self.assertEqual([action["type"] for action in actions], ["follow_up", "follow_up"])
        self.assertEqual([action["app"]["appl_id"] for action in actions], ["WUR-AIP-15", "WUR-AIP-16"])

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

        actions = self.engine.compute_actions([app])

        self.assertEqual(len(actions), 1)
        self.assertEqual(actions[0]["type"], "withdraw")

    def test_normalize_action_policy_defaults_blank_and_unknown_to_enabled(self) -> None:
        self.assertEqual(normalize_action_policy(""), "enabled")
        self.assertEqual(normalize_action_policy(None), "enabled")
        self.assertEqual(normalize_action_policy("unexpected"), "enabled")
        self.assertEqual(normalize_action_policy("ASK_WHEN_DUE"), "ask_when_due")

    def test_legacy_follow_up_opt_out_disables_follow_up_when_new_policy_blank(self) -> None:
        app = {"follow_up_opt_out": "yes", "follow_up_policy": ""}

        self.assertEqual(get_effective_action_policy(app, "follow_up"), "disabled")
        self.assertTrue(action_blocks_automatic_digest(app, "follow_up"))

    def test_explicit_follow_up_policy_overrides_legacy_opt_out(self) -> None:
        app = {"follow_up_opt_out": "yes", "follow_up_policy": "enabled"}

        self.assertEqual(get_effective_action_policy(app, "follow_up"), "enabled")
        self.assertFalse(action_blocks_automatic_digest(app, "follow_up"))

    def test_ask_when_due_blocks_automatic_digest_creation(self) -> None:
        self.assertTrue(
            action_blocks_automatic_digest(
                {"follow_up_policy": "ask_when_due"},
                "follow_up",
            )
        )
        self.assertTrue(
            action_blocks_automatic_digest(
                {"withdraw_policy": "ask_when_due"},
                "withdraw",
            )
        )
        self.assertTrue(
            action_blocks_automatic_digest(
                {"deletion_request_policy": "ask_when_due"},
                "deletion_request",
            )
        )

    def test_follow_up_policy_disabled_suppresses_follow_up_but_not_withdrawal(self) -> None:
        follow_up_due_app = {
            "appl_id": "WUR-AIP-6",
            "status": "Applied",
            "applied_date": "2026-03-27",
            "last_activity_date": "",
            "follow_up_sent_date": "",
            "follow_up_count": "0",
            "follow_up_policy": "disabled",
            "withdrawal_sent_date": "",
        }
        withdraw_due_app = {
            "appl_id": "WUR-AIP-7",
            "status": "Applied",
            "applied_date": "2026-03-20",
            "last_activity_date": "",
            "follow_up_sent_date": "",
            "follow_up_count": "0",
            "follow_up_policy": "disabled",
            "withdrawal_sent_date": "",
        }

        actions = self.engine.compute_actions([follow_up_due_app, withdraw_due_app])

        self.assertEqual([action["type"] for action in actions], ["withdraw"])
        self.assertEqual(actions[0]["app"]["appl_id"], "WUR-AIP-7")

    def test_withdraw_policy_disabled_suppresses_withdrawal_but_not_follow_up(self) -> None:
        follow_up_due_app = {
            "appl_id": "WUR-AIP-8",
            "status": "Applied",
            "applied_date": "2026-03-27",
            "last_activity_date": "",
            "follow_up_sent_date": "",
            "follow_up_count": "0",
            "withdraw_policy": "disabled",
            "withdrawal_sent_date": "",
        }
        withdraw_due_app = {
            "appl_id": "WUR-AIP-9",
            "status": "Applied",
            "applied_date": "2026-03-20",
            "last_activity_date": "",
            "follow_up_sent_date": "",
            "follow_up_count": "0",
            "withdraw_policy": "disabled",
            "withdrawal_sent_date": "",
        }

        actions = self.engine.compute_actions([follow_up_due_app, withdraw_due_app])

        self.assertEqual([action["type"] for action in actions], ["follow_up", "follow_up"])
        self.assertEqual(actions[0]["app"]["appl_id"], "WUR-AIP-8")
        self.assertEqual(actions[1]["app"]["appl_id"], "WUR-AIP-9")

    def test_deletion_request_policy_disabled_suppresses_rejected_deletion_request(self) -> None:
        app = {
            "appl_id": "WUR-AIP-10",
            "status": "Rejected",
            "applied_date": "2026-03-20",
            "last_activity_date": "",
            "follow_up_sent_date": "",
            "follow_up_count": "0",
            "deletion_request_policy": "disabled",
            "withdrawal_sent_date": "",
            "deletion_request_sent_date": "",
        }

        actions = self.engine.compute_actions([app])

        self.assertEqual(actions, [])

    def test_ask_when_due_policy_does_not_create_automatic_digest_action(self) -> None:
        apps = [
            {
                "appl_id": "WUR-AIP-11",
                "status": "Applied",
                "applied_date": "2026-03-27",
                "last_activity_date": "",
                "follow_up_sent_date": "",
                "follow_up_count": "0",
                "follow_up_policy": "ask_when_due",
                "withdrawal_sent_date": "",
            },
            {
                "appl_id": "WUR-AIP-12",
                "status": "Applied",
                "applied_date": "2026-03-20",
                "last_activity_date": "",
                "follow_up_sent_date": "",
                "follow_up_count": "0",
                "follow_up_policy": "disabled",
                "withdraw_policy": "ask_when_due",
                "withdrawal_sent_date": "",
            },
            {
                "appl_id": "WUR-AIP-13",
                "status": "Rejected",
                "applied_date": "2026-03-20",
                "last_activity_date": "",
                "follow_up_sent_date": "",
                "follow_up_count": "0",
                "deletion_request_policy": "ask_when_due",
                "withdrawal_sent_date": "",
                "deletion_request_sent_date": "",
            },
        ]

        actions = self.engine.compute_actions(apps)

        self.assertEqual(actions, [])

    def test_legacy_deletion_request_opt_out_still_suppresses_deletion_request(self) -> None:
        app = {
            "appl_id": "WUR-AIP-14",
            "status": "Rejected",
            "applied_date": "2026-03-20",
            "last_activity_date": "",
            "follow_up_sent_date": "",
            "follow_up_count": "0",
            "deletion_request_opt_out": "yes",
            "withdrawal_sent_date": "",
            "deletion_request_sent_date": "",
        }

        actions = self.engine.compute_actions([app])

        self.assertEqual(actions, [])


if __name__ == "__main__":
    unittest.main()
