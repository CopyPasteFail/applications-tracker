import unittest
from unittest.mock import Mock, patch

from tracker import Tracker


class ManageActionTests(unittest.TestCase):
    def test_normalize_manage_action_accepts_single_letter_shortcuts(self) -> None:
        self.assertEqual(Tracker._normalize_manage_action("d"), "defer")
        self.assertEqual(Tracker._normalize_manage_action("p"), "pause")
        self.assertEqual(Tracker._normalize_manage_action("r"), "resume")
        self.assertEqual(Tracker._normalize_manage_action("e"), "email")
        self.assertEqual(Tracker._normalize_manage_action("o"), "policy")
        self.assertEqual(Tracker._normalize_manage_action("w"), "withdraw")
        self.assertEqual(Tracker._normalize_manage_action("c"), "exit")

    def test_normalize_manage_action_accepts_full_action_names(self) -> None:
        self.assertEqual(Tracker._normalize_manage_action("defer"), "defer")
        self.assertEqual(Tracker._normalize_manage_action("policy"), "policy")
        self.assertEqual(Tracker._normalize_manage_action("policies"), "policy")
        self.assertEqual(Tracker._normalize_manage_action("optout"), "policy")
        self.assertEqual(Tracker._normalize_manage_action("opt-out"), "policy")
        self.assertEqual(Tracker._normalize_manage_action("withdraw"), "withdraw")
        self.assertEqual(Tracker._normalize_manage_action("exit"), "exit")
        self.assertEqual(Tracker._normalize_manage_action("cancel"), "exit")

    def test_normalize_manage_action_rejects_unknown_values(self) -> None:
        self.assertIsNone(Tracker._normalize_manage_action(""))
        self.assertIsNone(Tracker._normalize_manage_action("x"))
        self.assertIsNone(Tracker._normalize_manage_action("withdraw-now"))

    def test_describe_missing_email_policy_returns_manage_friendly_labels(self) -> None:
        self.assertEqual(Tracker._describe_missing_email_policy(""), "Ask every time")
        self.assertEqual(Tracker._describe_missing_email_policy("skip_always"), "Opt out")
        self.assertEqual(
            Tracker._describe_missing_email_policy("create_empty_draft"),
            "Create empty draft",
        )
        self.assertEqual(Tracker._describe_missing_email_policy("unexpected"), "Ask every time")

    def test_describe_action_opt_out_returns_enabled_disabled_labels(self) -> None:
        self.assertEqual(Tracker._describe_action_opt_out(""), "Enabled")
        self.assertEqual(Tracker._describe_action_opt_out("yes"), "Disabled")

    def test_describe_action_policy_returns_manage_friendly_labels(self) -> None:
        self.assertEqual(Tracker._describe_action_policy(""), "Enabled")
        self.assertEqual(Tracker._describe_action_policy("enabled"), "Enabled")
        self.assertEqual(Tracker._describe_action_policy("disabled"), "Disabled")
        self.assertEqual(Tracker._describe_action_policy("ask_when_due"), "Ask when due")
        self.assertEqual(Tracker._describe_action_policy("unexpected"), "Enabled")

    def test_manage_action_opt_outs_sets_deletion_request_policy(self) -> None:
        tracker = Tracker.__new__(Tracker)
        tracker.sheets = Mock()
        app = {"appl_id": "app-1", "deletion_request_policy": ""}

        with patch("tracker.console.print"), patch("tracker.Prompt.ask", side_effect=["d", "a"]):
            tracker._manage_action_opt_outs(app)

        tracker.sheets.set_field.assert_called_once_with(
            "app-1",
            "deletion_request_policy",
            "disabled",
        )
        self.assertEqual(app["deletion_request_policy"], "disabled")

    def test_manage_action_opt_outs_clears_deletion_request_policy(self) -> None:
        tracker = Tracker.__new__(Tracker)
        tracker.sheets = Mock()
        app = {"appl_id": "app-1", "deletion_request_policy": "disabled"}

        with patch("tracker.console.print"), patch("tracker.Prompt.ask", side_effect=["d", "e"]):
            tracker._manage_action_opt_outs(app)

        tracker.sheets.set_field.assert_called_once_with(
            "app-1",
            "deletion_request_policy",
            "enabled",
        )
        self.assertEqual(app["deletion_request_policy"], "enabled")

    def test_manage_action_opt_outs_sets_follow_up_policy(self) -> None:
        tracker = Tracker.__new__(Tracker)
        tracker.sheets = Mock()
        app = {"appl_id": "app-1", "follow_up_policy": ""}

        with patch("tracker.console.print"), patch("tracker.Prompt.ask", side_effect=["f", "a"]):
            tracker._manage_action_opt_outs(app)

        tracker.sheets.set_field.assert_called_once_with(
            "app-1",
            "follow_up_policy",
            "disabled",
        )
        self.assertEqual(app["follow_up_policy"], "disabled")

    def test_manage_action_opt_outs_shows_effective_legacy_policy_in_detail_view(self) -> None:
        tracker = Tracker.__new__(Tracker)
        tracker.sheets = Mock()
        app = {
            "appl_id": "app-1",
            "follow_up_policy": "",
            "follow_up_opt_out": "yes",
        }

        with patch("tracker.console.print") as mock_print, patch(
            "tracker.Prompt.ask",
            side_effect=["f", "c"],
        ):
            tracker._manage_action_opt_outs(app)

        printed_messages = [
            str(call.args[0])
            for call in mock_print.call_args_list
            if call.args
        ]
        self.assertTrue(any("Follow-up" in message and "current: [green]Disabled" in message for message in printed_messages))
        self.assertTrue(any("current behavior is [green]Disabled" in message for message in printed_messages))
        tracker.sheets.set_field.assert_not_called()

    def test_manage_action_opt_outs_sets_withdraw_policy_to_ask_when_due(self) -> None:
        tracker = Tracker.__new__(Tracker)
        tracker.sheets = Mock()
        app = {"appl_id": "app-1", "withdraw_policy": ""}

        with patch("tracker.console.print"), patch("tracker.Prompt.ask", side_effect=["w", "k"]):
            tracker._manage_action_opt_outs(app)

        tracker.sheets.set_field.assert_called_once_with(
            "app-1",
            "withdraw_policy",
            "ask_when_due",
        )
        self.assertEqual(app["withdraw_policy"], "ask_when_due")

    def test_persist_resolved_contact_email_skips_privacy_actions(self) -> None:
        tracker = Tracker.__new__(Tracker)
        tracker.sheets = Mock()
        app = {"appl_id": "app-1", "contact_email": ""}

        tracker._persist_resolved_contact_email(
            app,
            "privacy@example.com",
            action_type="deletion_request",
        )

        tracker.sheets.set_field.assert_not_called()
        self.assertEqual(app["contact_email"], "")

    def test_persist_resolved_contact_email_keeps_follow_up_contacts(self) -> None:
        tracker = Tracker.__new__(Tracker)
        tracker.sheets = Mock()
        app = {"appl_id": "app-1", "contact_email": ""}

        tracker._persist_resolved_contact_email(
            app,
            "recruiting@example.com",
            action_type="follow_up",
        )

        tracker.sheets.set_field.assert_called_once_with(
            "app-1",
            "contact_email",
            "recruiting@example.com",
        )
        self.assertEqual(app["contact_email"], "recruiting@example.com")

    def test_explicit_privacy_search_rejects_generic_contacts(self) -> None:
        tracker = Tracker.__new__(Tracker)
        tracker._extract_known_application_contact_emails = Mock(return_value=["recruiting@example.com"])
        tracker._discover_company_contact_via_web = Mock(return_value="careers@example.com")
        app = {
            "company": "Acme",
            "contact_email": "contact@acme.example",
            "recruiter_email": "",
            "ats_email": "",
        }

        with patch("tracker.console.print"):
            contact = tracker._resolve_company_contact_email(
                app,
                "deletion_request",
                log_progress=True,
                allow_stored_recipient=False,
                privacy_only=True,
            )

        self.assertEqual(contact, "")
        tracker._discover_company_contact_via_web.assert_called_once()

    def test_failed_privacy_search_reprompts_with_stored_email_option(self) -> None:
        tracker = Tracker.__new__(Tracker)
        tracker._resolve_company_contact_email = Mock(return_value="")
        app = {
            "company": "Acme",
            "role": "Founding Engineer",
            "contact_email": "contact@acme.example",
            "recruiter_email": "",
            "ats_email": "",
        }

        with patch("tracker.console.print"), patch("tracker.Prompt.ask", return_value=""):
            contact = tracker._resolve_privacy_contact_after_search_choice(app, "deletion_request")

        self.assertEqual(contact, "contact@acme.example")
        tracker._resolve_company_contact_email.assert_called_once_with(
            app,
            "deletion_request",
            log_progress=True,
            allow_stored_recipient=False,
            privacy_only=True,
        )


if __name__ == "__main__":
    unittest.main()
