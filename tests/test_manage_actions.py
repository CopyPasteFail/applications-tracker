import unittest
import json
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

from rich.table import Table

from tracker import Tracker


class ManageActionTests(unittest.TestCase):
    def _digest_tracker_with_pending_path(self, pending_path: Path) -> Tracker:
        tracker = Tracker.__new__(Tracker)
        tracker.cfg = {"user": {"name": "Omer", "career_email": "omer@example.com"}}
        tracker.sheets = Mock()
        tracker.gmail = Mock()
        tracker.ai = Mock()
        tracker.engine = Mock()
        tracker._delete_pending_gmail_drafts = Mock()
        tracker._clear_pending_action_draft_references = Mock()
        tracker._delete_pending_actions_file = Mock()
        return tracker

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

    def test_manage_resume_sets_active_and_clears_deferral(self) -> None:
        tracker = Tracker.__new__(Tracker)
        tracker.sheets = Mock()
        app = {
            "appl_id": "app-1",
            "company": "Acme",
            "role": "Engineer",
            "status": "Paused",
            "deferred_until": "2026-05-08",
        }
        tracker.sheets.get_all.return_value = [app]

        with patch("tracker.console.print"), patch(
            "tracker.Prompt.ask",
            side_effect=["", "1", "r"],
        ):
            tracker.manage()

        tracker.sheets.set_field.assert_any_call(app["appl_id"], "status", "Active")
        tracker.sheets.set_field.assert_any_call(app["appl_id"], "deferred_until", "")

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

        with patch("tracker.console.print"), patch("tracker.Prompt.ask", side_effect=["d", "d"]):
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

        with patch("tracker.console.print"), patch("tracker.Prompt.ask", side_effect=["f", "d"]):
            tracker._manage_action_opt_outs(app)

        tracker.sheets.set_field.assert_called_once_with(
            "app-1",
            "follow_up_policy",
            "disabled",
        )
        self.assertEqual(app["follow_up_policy"], "disabled")

    def test_manage_action_opt_outs_sets_follow_up_policy_to_ask_when_due(self) -> None:
        tracker = Tracker.__new__(Tracker)
        tracker.sheets = Mock()
        app = {"appl_id": "app-1", "follow_up_policy": ""}

        with patch("tracker.console.print"), patch("tracker.Prompt.ask", side_effect=["f", "k"]):
            tracker._manage_action_opt_outs(app)

        tracker.sheets.set_field.assert_called_once_with(
            "app-1",
            "follow_up_policy",
            "ask_when_due",
        )
        self.assertEqual(app["follow_up_policy"], "ask_when_due")

    def test_manage_action_opt_outs_ignores_legacy_follow_up_opt_out_in_detail_view(self) -> None:
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
        self.assertTrue(any("Follow-up" in message and "current: [green]Enabled" in message for message in printed_messages))
        self.assertTrue(any("current behavior is [green]Enabled" in message for message in printed_messages))
        self.assertFalse(any("follow_up_opt_out" in message for message in printed_messages))
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

    def test_manage_action_opt_outs_sets_deletion_request_policy_to_ask_when_due(self) -> None:
        tracker = Tracker.__new__(Tracker)
        tracker.sheets = Mock()
        app = {"appl_id": "app-1", "deletion_request_policy": ""}

        with patch("tracker.console.print"), patch("tracker.Prompt.ask", side_effect=["d", "k"]):
            tracker._manage_action_opt_outs(app)

        tracker.sheets.set_field.assert_called_once_with(
            "app-1",
            "deletion_request_policy",
            "ask_when_due",
        )
        self.assertEqual(app["deletion_request_policy"], "ask_when_due")

    def test_missing_follow_up_email_skip_always_disables_follow_up_policy(self) -> None:
        tracker = Tracker.__new__(Tracker)
        tracker.sheets = Mock()
        app = {
            "appl_id": "app-1",
            "company": "Acme",
            "role": "Engineer",
            "follow_up_policy": "enabled",
        }

        with patch("tracker.console.print"), patch("tracker.Prompt.ask", return_value="a"):
            target, requested_manual_draft, should_skip_action = tracker._resolve_missing_email_action(
                app,
                "follow_up",
            )

        self.assertEqual((target, requested_manual_draft, should_skip_action), ("", False, True))
        tracker.sheets.set_field.assert_called_once_with("app-1", "follow_up_policy", "disabled")
        self.assertEqual(app["follow_up_policy"], "disabled")

    def test_missing_withdraw_email_skip_always_disables_withdraw_policy(self) -> None:
        tracker = Tracker.__new__(Tracker)
        tracker.sheets = Mock()
        app = {
            "appl_id": "app-1",
            "company": "Acme",
            "role": "Engineer",
            "withdraw_policy": "enabled",
        }

        with patch("tracker.console.print"), patch("tracker.Prompt.ask", return_value="a"):
            target, requested_manual_draft, should_skip_action = tracker._resolve_missing_email_action(
                app,
                "withdraw",
            )

        self.assertEqual((target, requested_manual_draft, should_skip_action), ("", False, True))
        tracker.sheets.set_field.assert_called_once_with("app-1", "withdraw_policy", "disabled")
        self.assertEqual(app["withdraw_policy"], "disabled")

    def test_missing_deletion_request_email_skip_always_disables_deletion_request_policy(self) -> None:
        tracker = Tracker.__new__(Tracker)
        tracker.sheets = Mock()
        app = {
            "appl_id": "app-1",
            "company": "Acme",
            "role": "Engineer",
            "deletion_request_policy": "enabled",
        }

        with patch("tracker.console.print"), patch("tracker.Prompt.ask", return_value="a"):
            target, requested_manual_draft, should_skip_action = tracker._resolve_missing_email_action(
                app,
                "deletion_request",
            )

        self.assertEqual((target, requested_manual_draft, should_skip_action), ("", False, True))
        tracker.sheets.set_field.assert_called_once_with(
            "app-1",
            "deletion_request_policy",
            "disabled",
        )
        self.assertEqual(app["deletion_request_policy"], "disabled")

    def test_persist_resolved_action_email_keeps_follow_up_contacts(self) -> None:
        tracker = Tracker.__new__(Tracker)
        tracker.sheets = Mock()
        app = {"appl_id": "app-1", "contact_email": ""}

        tracker._persist_resolved_action_email(
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

    def test_persist_resolved_action_email_saves_deletion_request_to_privacy_contact(self) -> None:
        tracker = Tracker.__new__(Tracker)
        tracker.sheets = Mock()
        app = {"appl_id": "app-1", "contact_email": ""}

        tracker._persist_resolved_action_email(
            app,
            "privacy@example.com",
            action_type="deletion_request",
        )

        tracker.sheets.set_field.assert_called_once_with(
            "app-1",
            "privacy_contact_email",
            "privacy@example.com",
        )
        self.assertEqual(app["contact_email"], "")
        self.assertEqual(app["privacy_contact_email"], "privacy@example.com")

    def test_persist_resolved_action_email_saves_withdraw_to_privacy_contact(self) -> None:
        tracker = Tracker.__new__(Tracker)
        tracker.sheets = Mock()
        app = {"appl_id": "app-1", "contact_email": "recruiting@example.com"}

        tracker._persist_resolved_action_email(
            app,
            "legal@example.com",
            action_type="withdraw",
        )

        tracker.sheets.set_field.assert_called_once_with(
            "app-1",
            "privacy_contact_email",
            "legal@example.com",
        )
        self.assertEqual(app["contact_email"], "recruiting@example.com")
        self.assertEqual(app["privacy_contact_email"], "legal@example.com")

    def test_missing_privacy_email_manual_entry_persists_to_privacy_contact(self) -> None:
        tracker = Tracker.__new__(Tracker)
        tracker.sheets = Mock()
        app = {
            "appl_id": "app-1",
            "company": "Acme",
            "role": "Engineer",
            "contact_email": "recruiter@acme.example",
            "privacy_contact_email": "",
        }

        with patch("tracker.console.print"), patch(
            "tracker.Prompt.ask",
            side_effect=["privacy@acme.example", "y"],
        ):
            target, requested_manual_draft, should_skip_action = tracker._resolve_missing_email_action(
                app,
                "deletion_request",
            )

        self.assertEqual((target, requested_manual_draft, should_skip_action), ("privacy@acme.example", False, False))
        tracker.sheets.set_field.assert_called_once_with(
            "app-1",
            "privacy_contact_email",
            "privacy@acme.example",
        )
        self.assertEqual(app["contact_email"], "recruiter@acme.example")
        self.assertEqual(app["privacy_contact_email"], "privacy@acme.example")

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

    def test_privacy_contact_choice_prompts_with_stored_email_before_search(self) -> None:
        tracker = Tracker.__new__(Tracker)
        tracker._resolve_company_contact_email = Mock(
            side_effect=AssertionError("privacy search should wait for explicit choice")
        )
        tracker._confirm_email_value = Mock(
            side_effect=AssertionError("manual prompt should not run")
        )
        app = {
            "company": "Acme",
            "role": "Founding Engineer",
            "contact_email": "contact@acme.example",
            "recruiter_email": "",
            "ats_email": "",
            "privacy_contact_email": "",
        }

        with patch("tracker.console.print"), patch("tracker.Prompt.ask", return_value=""):
            contact, should_search, should_persist = tracker._choose_existing_or_manual_privacy_contact(
                app,
                "deletion_request",
            )

        self.assertEqual((contact, should_search, should_persist), ("contact@acme.example", False, False))
        tracker._resolve_company_contact_email.assert_not_called()

    def test_privacy_contact_choice_manual_entry_persists_to_privacy_contact_only(self) -> None:
        tracker = Tracker.__new__(Tracker)
        tracker.sheets = Mock()
        tracker._confirm_email_value = Mock(return_value="privacy@example.com")
        tracker._resolve_company_contact_email = Mock(
            side_effect=AssertionError("privacy search should wait for explicit choice")
        )
        app = {
            "appl_id": "app-1",
            "company": "Acme",
            "role": "Founding Engineer",
            "contact_email": "contact@acme.example",
            "recruiter_email": "",
            "ats_email": "",
            "privacy_contact_email": "",
        }

        with patch("tracker.console.print"), patch("tracker.Prompt.ask", return_value="m"):
            contact, should_search, should_persist = tracker._choose_existing_or_manual_privacy_contact(
                app,
                "deletion_request",
            )

        self.assertEqual((contact, should_search, should_persist), ("privacy@example.com", False, True))
        tracker.sheets.set_field.assert_called_once_with(
            "app-1",
            "privacy_contact_email",
            "privacy@example.com",
        )
        self.assertEqual(app["contact_email"], "contact@acme.example")
        self.assertEqual(app["privacy_contact_email"], "privacy@example.com")

    def test_privacy_contact_choice_search_persists_found_privacy_contact(self) -> None:
        tracker = Tracker.__new__(Tracker)
        tracker._resolve_company_contact_email = Mock(return_value="privacy@example.com")
        tracker._confirm_email_value = Mock(
            side_effect=AssertionError("manual prompt should not run")
        )
        app = {
            "company": "Acme",
            "role": "Founding Engineer",
            "contact_email": "contact@acme.example",
            "recruiter_email": "",
            "ats_email": "",
            "privacy_contact_email": "",
        }

        with patch("tracker.console.print"), patch("tracker.Prompt.ask", return_value="s"):
            contact, should_search, should_persist = tracker._choose_existing_or_manual_privacy_contact(
                app,
                "deletion_request",
            )
            if should_search:
                contact, should_persist = tracker._resolve_privacy_contact_after_search_choice(
                    app,
                    "deletion_request",
                )

        self.assertEqual((contact, should_persist), ("privacy@example.com", True))
        tracker._resolve_company_contact_email.assert_called_once_with(
            app,
            "deletion_request",
            log_progress=True,
            allow_stored_recipient=False,
            privacy_only=True,
        )

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
            contact, should_persist = tracker._resolve_privacy_contact_after_search_choice(app, "deletion_request")

        self.assertEqual((contact, should_persist), ("contact@acme.example", False))
        tracker._resolve_company_contact_email.assert_called_once_with(
            app,
            "deletion_request",
            log_progress=True,
            allow_stored_recipient=False,
            privacy_only=True,
        )

    def test_digest_privacy_action_with_privacy_contact_uses_it_without_prompt(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            pending_path = Path(temp_dir) / "pending_actions.json"
            tracker = self._digest_tracker_with_pending_path(pending_path)
            app = {
                "appl_id": "WUR-AIP-23",
                "company": "Acme",
                "role": "Engineer",
                "contact_email": "recruiter@acme.example",
                "privacy_contact_email": "privacy@acme.example",
            }
            action = {
                "type": "deletion_request",
                "app": app,
                "reason": "Rejected - privacy cleanup",
            }
            tracker.sheets.get_all.return_value = [app]
            tracker.engine.compute_actions.return_value = [action]
            tracker.engine.compute_manual_review_candidates.return_value = []
            tracker._resolve_privacy_contact_after_search_choice = Mock(
                side_effect=AssertionError("privacy contact should already be resolved")
            )
            tracker._resolve_company_contact_email = Mock(
                side_effect=AssertionError("fallback resolver should not run")
            )
            tracker._resolve_missing_email_action = Mock(
                side_effect=AssertionError("manual missing-email prompt should not run")
            )
            tracker.ai.generate_deletion_request.return_value = ("Delete data", "Please delete my data")
            tracker.gmail.create_draft.return_value = "draft-privacy"

            with patch("tracker.PENDING_PATH", pending_path), patch("tracker.console.print"), patch(
                "tracker.Prompt.ask",
                side_effect=AssertionError("prompt should not run"),
            ):
                tracker.run_digest_only()

            tracker.gmail.create_draft.assert_called_once_with(
                "privacy@acme.example",
                "Delete data",
                "Please delete my data",
                from_addr="omer@example.com",
                from_name="Omer",
            )
            self.assertEqual(app["contact_email"], "recruiter@acme.example")
            self.assertEqual(app["privacy_contact_email"], "privacy@acme.example")

    def test_digest_privacy_action_without_privacy_contact_prompts_before_contact_fallback(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            pending_path = Path(temp_dir) / "pending_actions.json"
            tracker = self._digest_tracker_with_pending_path(pending_path)
            app = {
                "appl_id": "WUR-AIP-24",
                "company": "Acme",
                "role": "Engineer",
                "contact_email": "recruiter@acme.example",
                "privacy_contact_email": "",
            }
            action = {
                "type": "deletion_request",
                "app": app,
                "reason": "Rejected - privacy cleanup",
            }
            tracker.sheets.get_all.return_value = [app]
            tracker.engine.compute_actions.return_value = [action]
            tracker.engine.compute_manual_review_candidates.return_value = []
            tracker._resolve_privacy_contact_after_search_choice = Mock(
                side_effect=AssertionError("privacy search should wait for explicit choice")
            )
            tracker._resolve_company_contact_email = Mock(
                side_effect=AssertionError("contact fallback should wait for explicit privacy path")
            )
            tracker._resolve_missing_email_action = Mock(
                side_effect=AssertionError("target email should be resolved by privacy path")
            )
            tracker.ai.generate_deletion_request.return_value = ("Delete data", "Please delete my data")
            tracker.gmail.create_draft.return_value = "draft-privacy"

            with patch("tracker.PENDING_PATH", pending_path), patch("tracker.console.print"), patch(
                "tracker.Prompt.ask",
                return_value="",
            ):
                tracker.run_digest_only()

            tracker._resolve_privacy_contact_after_search_choice.assert_not_called()
            self.assertNotIn(
                ("WUR-AIP-24", "privacy_contact_email", "recruiter@acme.example"),
                [call.args for call in tracker.sheets.set_field.call_args_list],
            )
            tracker.gmail.create_draft.assert_called_once_with(
                "recruiter@acme.example",
                "Delete data",
                "Please delete my data",
                from_addr="omer@example.com",
                from_name="Omer",
            )
            self.assertEqual(app["contact_email"], "recruiter@acme.example")
            self.assertEqual(app["privacy_contact_email"], "")

    def test_privacy_search_skip_can_explicitly_fallback_to_contact_email(self) -> None:
        tracker = Tracker.__new__(Tracker)
        tracker._resolve_company_contact_email = Mock(return_value="")
        app = {
            "company": "Acme",
            "role": "Engineer",
            "privacy_contact_email": "",
            "contact_email": "recruiter@acme.example",
            "recruiter_email": "",
            "ats_email": "",
        }

        with patch("tracker.console.print"), patch("tracker.Prompt.ask", return_value=""):
            contact, should_persist = tracker._resolve_privacy_contact_after_search_choice(app, "withdraw")

        self.assertEqual((contact, should_persist), ("recruiter@acme.example", False))
        tracker._resolve_company_contact_email.assert_called_once_with(
            app,
            "withdraw",
            log_progress=True,
            allow_stored_recipient=False,
            privacy_only=True,
        )

    def test_digest_displays_ask_when_due_manual_review_without_creating_drafts(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            pending_path = Path(temp_dir) / "pending_actions.json"
            tracker = self._digest_tracker_with_pending_path(pending_path)
            app = {
                "appl_id": "WUR-AIP-17",
                "company": "Acme",
                "role": "Engineer",
            }
            candidate = {
                "mode": "manual_review",
                "type": "follow_up",
                "appl_id": "WUR-AIP-17",
                "company": "Acme",
                "role": "Engineer",
                "reason": "7d inactive - follow-up #1",
                "policy": "ask_when_due",
                "follow_up_n": 1,
            }
            tracker.sheets.get_all.return_value = [app]
            tracker.engine.compute_actions.return_value = []
            tracker.engine.compute_manual_review_candidates.return_value = [candidate]

            with patch("tracker.PENDING_PATH", pending_path), patch("tracker.console.print") as mock_print:
                tracker.run_digest_only()

            tracker.engine.compute_actions.assert_called_once_with([app])
            tracker.engine.compute_manual_review_candidates.assert_called_once_with([app])
            tracker.gmail.create_draft.assert_not_called()
            self.assertEqual(json.loads(pending_path.read_text(encoding="utf-8")), [])
            printed_tables = [
                call.args[0]
                for call in mock_print.call_args_list
                if call.args and isinstance(call.args[0], Table)
            ]
            self.assertTrue(any("Manual Review" in table.title for table in printed_tables))

    def test_digest_automatic_actions_still_create_drafts_and_pending_records(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            pending_path = Path(temp_dir) / "pending_actions.json"
            tracker = self._digest_tracker_with_pending_path(pending_path)
            app = {
                "appl_id": "WUR-AIP-21",
                "company": "Acme",
                "role": "Engineer",
            }
            action = {
                "type": "follow_up",
                "app": app,
                "reason": "7d inactive - follow-up #1",
                "follow_up_n": 1,
            }
            tracker.sheets.get_all.return_value = [app]
            tracker.engine.compute_actions.return_value = [action]
            tracker.engine.compute_manual_review_candidates.return_value = []
            tracker._choose_existing_or_manual_privacy_contact = Mock(
                side_effect=AssertionError("privacy chooser should not run for follow-up")
            )
            tracker._resolve_company_contact_email = Mock(return_value="recruiter@example.com")
            tracker._persist_resolved_action_email = Mock()
            tracker._resolve_missing_email_action = Mock(side_effect=AssertionError("target email should exist"))
            tracker.ai.generate_follow_up.return_value = ("Checking in", "Hello")
            tracker.gmail.create_draft.return_value = "draft-1"

            with patch("tracker.PENDING_PATH", pending_path), patch("tracker.console.print"):
                tracker.run_digest_only()

            tracker.gmail.create_draft.assert_called_once_with(
                "recruiter@example.com",
                "Checking in",
                "Hello",
                from_addr="omer@example.com",
                from_name="Omer",
            )
            pending = json.loads(pending_path.read_text(encoding="utf-8"))
            self.assertEqual(len(pending), 1)
            self.assertEqual(pending[0]["type"], "follow_up")
            self.assertEqual(pending[0]["appl_id"], "WUR-AIP-21")
            self.assertEqual(pending[0]["draft_id"], "draft-1")

    def test_digest_mixed_actions_persist_only_automatic_pending_records(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            pending_path = Path(temp_dir) / "pending_actions.json"
            tracker = self._digest_tracker_with_pending_path(pending_path)
            auto_app = {
                "appl_id": "WUR-AIP-21",
                "company": "AutoCo",
                "role": "Engineer",
            }
            manual_app = {
                "appl_id": "WUR-AIP-22",
                "company": "ManualCo",
                "role": "Designer",
            }
            action = {
                "type": "follow_up",
                "app": auto_app,
                "reason": "7d inactive - follow-up #1",
                "follow_up_n": 1,
            }
            candidate = {
                "mode": "manual_review",
                "type": "withdraw",
                "appl_id": "WUR-AIP-22",
                "company": "ManualCo",
                "role": "Designer",
                "reason": "Ghosted - 14d since last activity",
                "policy": "ask_when_due",
            }
            tracker.sheets.get_all.return_value = [auto_app, manual_app]
            tracker.engine.compute_actions.return_value = [action]
            tracker.engine.compute_manual_review_candidates.return_value = [candidate]
            tracker._choose_existing_or_manual_privacy_contact = Mock(
                side_effect=AssertionError("privacy chooser should not run for follow-up")
            )
            tracker._resolve_company_contact_email = Mock(return_value="recruiter@example.com")
            tracker._persist_resolved_action_email = Mock()
            tracker._resolve_missing_email_action = Mock(side_effect=AssertionError("target email should exist"))
            tracker.ai.generate_follow_up.return_value = ("Checking in", "Hello")
            tracker.gmail.create_draft.return_value = "draft-1"

            with patch("tracker.PENDING_PATH", pending_path), patch("tracker.console.print") as mock_print:
                tracker.run_digest_only()

            tracker.gmail.create_draft.assert_called_once()
            pending = json.loads(pending_path.read_text(encoding="utf-8"))
            self.assertEqual([record["appl_id"] for record in pending], ["WUR-AIP-21"])
            self.assertNotIn("WUR-AIP-22", json.dumps(pending))
            printed_tables = [
                call.args[0]
                for call in mock_print.call_args_list
                if call.args and isinstance(call.args[0], Table)
            ]
            self.assertTrue(any("Manual Review" in table.title for table in printed_tables))

    def test_confirm_skips_stale_withdraw_when_current_status_is_terminal(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            pending_path = Path(temp_dir) / "pending_actions.json"
            pending_path.write_text(
                json.dumps(
                    [
                        {
                            "type": "withdraw",
                            "appl_id": "WUR-AIP-30",
                            "company": "Acme",
                            "role": "Engineer",
                            "target_email": "privacy@acme.example",
                            "subject": "Withdrawal",
                            "draft_id": "draft-30",
                        }
                    ]
                ),
                encoding="utf-8",
            )
            tracker = self._digest_tracker_with_pending_path(pending_path)
            tracker.sheets.get_all.return_value = [
                {
                    "appl_id": "WUR-AIP-30",
                    "status": "Rejected",
                }
            ]

            with patch("tracker.PENDING_PATH", pending_path), patch("tracker.Confirm.ask", return_value=True), patch(
                "tracker.console.print"
            ):
                tracker.confirm()

            tracker.gmail.send_draft.assert_not_called()
            tracker.sheets.set_field.assert_not_called()
            self.assertEqual(json.loads(pending_path.read_text(encoding="utf-8")), [])


if __name__ == "__main__":
    unittest.main()
