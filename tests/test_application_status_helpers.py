import unittest

from tracker import (
    APPLICATION_STATUSES,
    is_active_application_lifecycle,
    is_paused_application_status,
    is_terminal_application_status,
    normalize_application_status,
    should_clear_deferred_until_for_status,
    status_blocks_pipeline_actions,
)


class ApplicationStatusHelperTests(unittest.TestCase):
    def test_application_statuses_are_lifecycle_values_only(self) -> None:
        self.assertEqual(
            APPLICATION_STATUSES,
            ("Active", "Paused", "Rejected", "Withdrawn", "Offer"),
        )

    def test_normalize_application_status_maps_legacy_active_values_to_active(self) -> None:
        legacy_active_values = [
            "Applied",
            "Screening",
            "Interview",
            "Assessment",
            "",
            None,
            "Custom Stage",
            "active",
            " ACTIVE ",
        ]

        for raw_status in legacy_active_values:
            with self.subTest(raw_status=raw_status):
                self.assertEqual(normalize_application_status(raw_status), "Active")

    def test_normalize_application_status_preserves_lifecycle_values_title_case(self) -> None:
        expected_statuses = {
            "paused": "Paused",
            "Rejected": "Rejected",
            "WITHDRAWN": "Withdrawn",
            " offer ": "Offer",
        }

        for raw_status, expected_status in expected_statuses.items():
            with self.subTest(raw_status=raw_status):
                self.assertEqual(normalize_application_status(raw_status), expected_status)

    def test_active_lifecycle_helper_matches_lifecycle_values(self) -> None:
        self.assertTrue(is_active_application_lifecycle("Active"))
        self.assertTrue(is_active_application_lifecycle("Interview"))
        self.assertFalse(is_active_application_lifecycle("Paused"))
        self.assertFalse(is_active_application_lifecycle("Rejected"))
        self.assertFalse(is_active_application_lifecycle("Withdrawn"))
        self.assertFalse(is_active_application_lifecycle("Offer"))

    def test_pipeline_blocking_statuses_match_lifecycle_model(self) -> None:
        self.assertTrue(status_blocks_pipeline_actions("Offer"))
        self.assertTrue(status_blocks_pipeline_actions("Withdrawn"))
        self.assertTrue(status_blocks_pipeline_actions("Paused"))
        self.assertFalse(status_blocks_pipeline_actions("Rejected"))
        self.assertFalse(status_blocks_pipeline_actions("Active"))
        self.assertFalse(status_blocks_pipeline_actions("Interview"))

    def test_terminal_statuses_match_lifecycle_outcomes(self) -> None:
        self.assertTrue(is_terminal_application_status("Rejected"))
        self.assertTrue(is_terminal_application_status("Withdrawn"))
        self.assertTrue(is_terminal_application_status("Offer"))
        self.assertFalse(is_terminal_application_status("Paused"))
        self.assertFalse(is_terminal_application_status("Active"))
        self.assertFalse(is_terminal_application_status("Assessment"))

    def test_paused_and_rejection_helpers_use_lifecycle_statuses(self) -> None:
        self.assertTrue(is_paused_application_status("Paused"))
        self.assertFalse(is_paused_application_status("Rejected"))
        self.assertTrue(should_clear_deferred_until_for_status("Rejected"))
        self.assertFalse(should_clear_deferred_until_for_status("Withdrawn"))


if __name__ == "__main__":
    unittest.main()
