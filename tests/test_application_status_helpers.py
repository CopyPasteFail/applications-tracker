import unittest

from tracker import (
    is_paused_application_status,
    is_terminal_application_status,
    normalize_application_status,
    should_clear_deferred_until_for_status,
    status_blocks_auto_withdraw,
    status_blocks_pipeline_actions,
    status_rank,
)


class ApplicationStatusHelperTests(unittest.TestCase):
    def test_normalize_application_status_preserves_known_statuses_and_defaults_blank_to_applied(self) -> None:
        self.assertEqual(normalize_application_status(" Interview "), "Interview")
        self.assertEqual(normalize_application_status(""), "Applied")
        self.assertEqual(normalize_application_status(None), "Applied")

    def test_normalize_application_status_preserves_unknown_status_text_for_compatibility(self) -> None:
        self.assertEqual(normalize_application_status("Custom Stage"), "Custom Stage")

    def test_status_rank_defaults_unknown_statuses_to_applied_rank(self) -> None:
        self.assertGreater(status_rank("Interview"), status_rank("Screening"))
        self.assertEqual(status_rank("Custom Stage"), status_rank("Applied"))

    def test_pipeline_blocking_statuses_match_current_digest_behavior(self) -> None:
        self.assertTrue(status_blocks_pipeline_actions("Offer"))
        self.assertTrue(status_blocks_pipeline_actions("Withdrawn"))
        self.assertTrue(status_blocks_pipeline_actions("Paused"))
        self.assertFalse(status_blocks_pipeline_actions("Rejected"))

    def test_terminal_statuses_match_current_backfill_behavior(self) -> None:
        self.assertTrue(is_terminal_application_status("Rejected"))
        self.assertTrue(is_terminal_application_status("Withdrawn"))
        self.assertTrue(is_terminal_application_status("Offer"))
        self.assertTrue(is_terminal_application_status("Ghosted"))
        self.assertFalse(is_terminal_application_status("Paused"))

    def test_paused_and_auto_withdraw_helpers_match_current_rules(self) -> None:
        self.assertTrue(is_paused_application_status("Paused"))
        self.assertFalse(is_paused_application_status("Rejected"))
        self.assertTrue(status_blocks_auto_withdraw("Interview"))
        self.assertTrue(status_blocks_auto_withdraw("Assessment"))
        self.assertFalse(status_blocks_auto_withdraw("Rejected"))

    def test_rejected_status_clears_deferred_until(self) -> None:
        self.assertTrue(should_clear_deferred_until_for_status("Rejected"))
        self.assertFalse(should_clear_deferred_until_for_status("Withdrawn"))


if __name__ == "__main__":
    unittest.main()
