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

    def test_legacy_interview_and_assessment_inputs_auto_withdraw_as_active(self) -> None:
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

        self.assertEqual([action["type"] for action in actions], ["withdraw", "withdraw"])
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

    def test_explicit_ask_when_due_policy_overrides_legacy_opt_out(self) -> None:
        follow_up_app = {"follow_up_opt_out": "yes", "follow_up_policy": "ask_when_due"}
        deletion_request_app = {
            "deletion_request_opt_out": "yes",
            "deletion_request_policy": "ask_when_due",
        }

        self.assertEqual(get_effective_action_policy(follow_up_app, "follow_up"), "ask_when_due")
        self.assertEqual(
            get_effective_action_policy(deletion_request_app, "deletion_request"),
            "ask_when_due",
        )
        self.assertTrue(action_blocks_automatic_digest(follow_up_app, "follow_up"))
        self.assertTrue(action_blocks_automatic_digest(deletion_request_app, "deletion_request"))

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

    def test_ask_when_due_follow_up_due_becomes_manual_review_candidate_only(self) -> None:
        app = {
            "appl_id": "WUR-AIP-17",
            "company": "Acme",
            "role": "Engineer",
            "status": "Applied",
            "applied_date": "2026-03-27",
            "last_activity_date": "",
            "follow_up_sent_date": "",
            "follow_up_count": "0",
            "follow_up_policy": "ask_when_due",
            "withdrawal_sent_date": "",
        }

        actions = self.engine.compute_actions([app])
        candidates = self.engine.compute_manual_review_candidates([app])

        self.assertEqual(actions, [])
        self.assertEqual(
            candidates,
            [
                {
                    "mode": "manual_review",
                    "type": "follow_up",
                    "appl_id": "WUR-AIP-17",
                    "company": "Acme",
                    "role": "Engineer",
                    "reason": "7d inactive — follow-up #1",
                    "policy": "ask_when_due",
                    "follow_up_n": 1,
                }
            ],
        )

    def test_ask_when_due_withdrawal_due_becomes_manual_review_candidate_only(self) -> None:
        app = {
            "appl_id": "WUR-AIP-18",
            "company": "Beta",
            "role": "Staff Engineer",
            "status": "Applied",
            "applied_date": "2026-03-20",
            "last_activity_date": "",
            "follow_up_sent_date": "",
            "follow_up_count": "0",
            "follow_up_policy": "disabled",
            "withdraw_policy": "ask_when_due",
            "withdrawal_sent_date": "",
        }

        actions = self.engine.compute_actions([app])
        candidates = self.engine.compute_manual_review_candidates([app])

        self.assertEqual(actions, [])
        self.assertEqual(candidates[0]["type"], "withdraw")
        self.assertEqual(candidates[0]["reason"], "Ghosted — 14d since last activity")
        self.assertEqual(candidates[0]["policy"], "ask_when_due")

    def test_ask_when_due_rejected_deletion_request_due_becomes_manual_review_candidate_only(self) -> None:
        app = {
            "appl_id": "WUR-AIP-19",
            "company": "Gamma",
            "role": "Data Engineer",
            "status": "Rejected",
            "applied_date": "2026-03-20",
            "last_activity_date": "",
            "follow_up_sent_date": "",
            "follow_up_count": "0",
            "deletion_request_policy": "ask_when_due",
            "withdrawal_sent_date": "",
            "deletion_request_sent_date": "",
        }

        actions = self.engine.compute_actions([app])
        candidates = self.engine.compute_manual_review_candidates([app])

        self.assertEqual(actions, [])
        self.assertEqual(candidates[0]["type"], "deletion_request")
        self.assertEqual(candidates[0]["reason"], "Rejected — 14d since last activity")
        self.assertEqual(candidates[0]["policy"], "ask_when_due")

    def test_disabled_policy_produces_neither_automatic_action_nor_manual_review_candidate(self) -> None:
        app = {
            "appl_id": "WUR-AIP-20",
            "status": "Applied",
            "applied_date": "2026-03-27",
            "last_activity_date": "",
            "follow_up_sent_date": "",
            "follow_up_count": "0",
            "follow_up_policy": "disabled",
            "withdrawal_sent_date": "",
        }

        self.assertEqual(self.engine.compute_actions([app]), [])
        self.assertEqual(self.engine.compute_manual_review_candidates([app]), [])

    def test_enabled_policy_produces_automatic_action_not_manual_review_candidate(self) -> None:
        app = {
            "appl_id": "WUR-AIP-21",
            "status": "Applied",
            "applied_date": "2026-03-27",
            "last_activity_date": "",
            "follow_up_sent_date": "",
            "follow_up_count": "0",
            "follow_up_policy": "enabled",
            "withdrawal_sent_date": "",
        }

        actions = self.engine.compute_actions([app])
        candidates = self.engine.compute_manual_review_candidates([app])

        self.assertEqual([action["type"] for action in actions], ["follow_up"])
        self.assertEqual(candidates, [])

    def test_explicit_ask_when_due_manual_review_overrides_legacy_opt_out_fields(self) -> None:
        app = {
            "appl_id": "WUR-AIP-22",
            "status": "Applied",
            "applied_date": "2026-03-27",
            "last_activity_date": "",
            "follow_up_sent_date": "",
            "follow_up_count": "0",
            "follow_up_opt_out": "yes",
            "follow_up_policy": "ask_when_due",
            "withdrawal_sent_date": "",
        }

        self.assertEqual(self.engine.compute_actions([app]), [])
        self.assertEqual([candidate["type"] for candidate in self.engine.compute_manual_review_candidates([app])], ["follow_up"])

    def test_deferred_rows_do_not_produce_manual_review_candidates(self) -> None:
        app = {
            "appl_id": "WUR-AIP-23",
            "status": "Applied",
            "applied_date": "2026-03-20",
            "last_activity_date": "",
            "follow_up_sent_date": "",
            "follow_up_count": "0",
            "follow_up_policy": "disabled",
            "withdraw_policy": "ask_when_due",
            "withdrawal_sent_date": "",
            "deferred_until": "2026-04-04",
        }

        self.assertEqual(self.engine.compute_manual_review_candidates([app]), [])

    def test_already_sent_actions_do_not_produce_manual_review_candidates(self) -> None:
        apps = [
            {
                "appl_id": "WUR-AIP-24",
                "status": "Applied",
                "applied_date": "2026-03-20",
                "last_activity_date": "",
                "follow_up_sent_date": "",
                "follow_up_count": "0",
                "follow_up_policy": "disabled",
                "withdraw_policy": "ask_when_due",
                "withdrawal_sent_date": "2026-04-01",
            },
            {
                "appl_id": "WUR-AIP-25",
                "status": "Rejected",
                "applied_date": "2026-03-20",
                "last_activity_date": "",
                "follow_up_sent_date": "",
                "follow_up_count": "0",
                "deletion_request_policy": "ask_when_due",
                "withdrawal_sent_date": "",
                "deletion_request_sent_date": "2026-04-01",
            },
        ]

        self.assertEqual(self.engine.compute_manual_review_candidates(apps), [])


if __name__ == "__main__":
    unittest.main()
