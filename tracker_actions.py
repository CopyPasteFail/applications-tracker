#!/usr/bin/env python3
"""Pure action-planning helpers for applications-tracker."""

from datetime import date, datetime, timezone
from typing import Any, Callable, Literal, Optional, TypeAlias

ConfigDict: TypeAlias = dict[str, Any]
ApplicationRecord: TypeAlias = dict[str, Any]
PendingActionRecord: TypeAlias = dict[str, Any]
ManualReviewCandidate: TypeAlias = dict[str, Any]
ActionPolicy: TypeAlias = str
ApplicationStatus: TypeAlias = Literal["Active", "Paused", "Rejected", "Withdrawn", "Offer"]
ApplicationLifecycle: TypeAlias = ApplicationStatus

APPLICATION_STATUSES: tuple[ApplicationStatus, ...] = (
    "Active",
    "Paused",
    "Rejected",
    "Withdrawn",
    "Offer",
)
DEFAULT_APPLICATION_STATUS: ApplicationStatus = "Active"
TERMINAL_APPLICATION_STATUSES: set[ApplicationStatus] = {"Rejected", "Withdrawn", "Offer"}
PIPELINE_BLOCKING_APPLICATION_STATUSES: set[ApplicationStatus] = {"Paused", "Withdrawn", "Offer"}
DEFERRED_UNTIL_CLEARING_APPLICATION_STATUSES: set[ApplicationStatus] = {"Rejected"}
LEGACY_ACTIVE_APPLICATION_STATUSES = {
    "applied",
    "screening",
    "interview",
    "assessment",
    "ghosted",
}
APPLICATION_STATUS_BY_NORMALIZED_VALUE: dict[str, ApplicationStatus] = {
    status.lower(): status for status in APPLICATION_STATUSES
}


# Status and lifecycle helpers


def normalize_application_status(raw_status: Any) -> ApplicationStatus:
    """Return the lifecycle status stored and used by current tracker logic."""
    status = str(raw_status or "").strip()
    normalized_status = status.lower()
    if not normalized_status or normalized_status in LEGACY_ACTIVE_APPLICATION_STATUSES:
        return DEFAULT_APPLICATION_STATUS
    return APPLICATION_STATUS_BY_NORMALIZED_VALUE.get(normalized_status, DEFAULT_APPLICATION_STATUS)


def application_lifecycle_from_status(status: Any) -> ApplicationLifecycle:
    """Return the normalized lifecycle status for compatibility with older imports."""
    return normalize_application_status(status)


def is_active_application_lifecycle(lifecycle: Any) -> bool:
    """Return whether a status belongs to the active pipeline group."""
    return normalize_application_status(lifecycle) == "Active"


def is_terminal_application_status(status: Any) -> bool:
    """Return whether a status is a completed lifecycle outcome."""
    return normalize_application_status(status) in TERMINAL_APPLICATION_STATUSES


def is_paused_application_status(status: Any) -> bool:
    """Return whether a status represents a manually paused application."""
    return normalize_application_status(status) == "Paused"


def status_blocks_pipeline_actions(status: Any) -> bool:
    """Return whether normal digest pipeline actions should be skipped for this status."""
    return normalize_application_status(status) in PIPELINE_BLOCKING_APPLICATION_STATUSES


def should_clear_deferred_until_for_status(status: Any) -> bool:
    """Return whether applying this status should clear a stored deferral."""
    return normalize_application_status(status) in DEFERRED_UNTIL_CLEARING_APPLICATION_STATUSES


# Generic date and sheet-value helpers


def parse_iso_date(raw_date: str) -> Optional[datetime]:
    """
    Parse an ISO-like date string into a datetime for comparison logic.

    Inputs:
    - raw_date: Expected `YYYY-MM-DD` style date text.

    Outputs:
    - Parsed datetime at midnight, or None when parsing fails.

    Edge cases:
    - Invalid or blank dates return None instead of raising.

    Atomicity / concurrency:
    - Pure helper with no shared mutable state.
    """
    normalized_date = str(raw_date or "").strip()
    if not normalized_date:
        return None
    try:
        return datetime.strptime(normalized_date, "%Y-%m-%d")
    except ValueError:
        return None


def is_truthy_sheet_value(raw_value: object) -> bool:
    """
    Interpret common sheet-entered truthy markers.

    Inputs:
    - raw_value: Cell value from the worksheet.

    Outputs:
    - True when the value represents an affirmative opt-in/opt-out style marker.

    Edge cases:
    - Blank values return False.
    - Accepts common user-entered variants such as `yes`, `true`, `1`, and `skip`.

    Atomicity / concurrency:
    - Pure helper with no shared mutable state.
    """
    normalized_value = str(raw_value or "").strip().lower()
    return normalized_value in {"1", "true", "yes", "y", "skip", "opt_out", "opt-out"}


# Per-action policy helpers


def normalize_action_policy(raw_value: object) -> ActionPolicy:
    """
    Normalize per-action policy values stored in the sheet.

    Blank, missing, or unrecognized values default to `enabled` so older sheets keep their
    previous behavior until a policy is explicitly set.
    """
    normalized_value = str(raw_value or "").strip().lower().replace("-", "_")
    if normalized_value in {"enabled", "disabled", "ask_when_due"}:
        return normalized_value
    return "enabled"


def _has_explicit_action_policy(raw_value: object) -> bool:
    normalized_value = str(raw_value or "").strip().lower().replace("-", "_")
    return normalized_value in {"enabled", "disabled", "ask_when_due"}


def get_effective_action_policy(app: ApplicationRecord, action_type: str) -> ActionPolicy:
    """
    Return the effective per-action policy for an application row.

    Compatibility:
    - Missing new policy fields default to `enabled`.
    - Legacy opt-out fields still disable matching actions unless a valid new policy is set.
    """
    policy_field_by_action_type = {
        "follow_up": "follow_up_policy",
        "withdraw": "withdraw_policy",
        "deletion_request": "deletion_request_policy",
    }
    policy_field = policy_field_by_action_type.get(action_type)
    if policy_field:
        raw_policy = app.get(policy_field, "")
        if _has_explicit_action_policy(raw_policy):
            return normalize_action_policy(raw_policy)

    legacy_opt_out_field_by_action_type = {
        "follow_up": "follow_up_opt_out",
        "deletion_request": "deletion_request_opt_out",
    }
    legacy_opt_out_field = legacy_opt_out_field_by_action_type.get(action_type)
    if legacy_opt_out_field and is_truthy_sheet_value(app.get(legacy_opt_out_field)):
        return "disabled"

    return "enabled"


def action_blocks_automatic_digest(app: ApplicationRecord, action_type: str) -> bool:
    """
    Return True when the action should not be auto-created by the digest.

    `ask_when_due` is intentionally non-automatic in this compatibility stage because there is
    no separate pending/manual representation for an ask prompt yet.
    """
    return get_effective_action_policy(app, action_type) != "enabled"


def _default_today_utc() -> date:
    return datetime.now(timezone.utc).date()


# Follow-up action planning


class FollowUpEngine:
    def __init__(self, config: ConfigDict, today_provider: Optional[Callable[[], date]] = None):
        t = config["thresholds"]
        self.follow_up_days = t["follow_up_days"]
        self.withdraw_days = t["withdraw_days"]
        self.follow_up_repeat_days = t.get("follow_up_repeat_days", t["follow_up_days"])
        self.deletion_request_days = t.get("deletion_request_days", 0)
        self._today_provider = today_provider or _default_today_utc

    def _get_withdraw_reference_date(self, app: ApplicationRecord) -> Optional[date]:
        """
        Choose the date that should start the withdrawal countdown for one application.

        Inputs:
        - app: Application row containing activity and follow-up timestamps.

        Outputs:
        - The most recent relevant date for withdrawal timing, or None when no usable date exists.

        Edge cases:
        - Invalid dates are ignored instead of raising.
        - A sent follow-up resets the withdrawal clock only when it is newer than the last activity.

        Atomicity / concurrency:
        - Pure helper with no shared mutable state.
        """
        candidate_datetimes = [
            parsed_datetime
            for parsed_datetime in (
                parse_iso_date(str(app.get("last_activity_date", "") or "").strip()),
                parse_iso_date(str(app.get("applied_date", "") or "").strip()),
                parse_iso_date(str(app.get("follow_up_sent_date", "") or "").strip()),
            )
            if parsed_datetime is not None
        ]
        if not candidate_datetimes:
            return None
        return max(candidate_datetimes).date()

    def _manual_review_candidate(
        self,
        app: ApplicationRecord,
        action_type: str,
        reason: str,
        follow_up_n: Optional[int] = None,
    ) -> ManualReviewCandidate:
        candidate: ManualReviewCandidate = {
            "mode": "manual_review",
            "type": action_type,
            "appl_id": app.get("appl_id", ""),
            "company": app.get("company", ""),
            "role": app.get("role", ""),
            "reason": reason,
            "policy": "ask_when_due",
        }
        if follow_up_n is not None:
            candidate["follow_up_n"] = follow_up_n
        return candidate

    def compute_manual_review_candidates(
        self,
        applications: list[ApplicationRecord],
    ) -> list[ManualReviewCandidate]:
        today = self._today_provider()
        candidates: list[ManualReviewCandidate] = []

        for app in applications:
            status = normalize_application_status(app.get("status"))
            manual_withdraw_requested = is_truthy_sheet_value(app.get("withdraw_in_next_digest"))

            if status_blocks_pipeline_actions(status):
                continue

            withdrawal_already_sent = bool(app.get("withdrawal_sent_date"))
            if withdrawal_already_sent:
                continue

            if manual_withdraw_requested and get_effective_action_policy(app, "withdraw") != "disabled":
                continue

            deferred = str(app.get("deferred_until", "") or "").strip()
            if deferred:
                try:
                    defer_date = datetime.fromisoformat(deferred[:10]).date()
                    if defer_date > today:
                        continue
                except ValueError:
                    pass

            ref_date = self._get_withdraw_reference_date(app)
            if ref_date is None:
                continue

            days = (today - ref_date).days
            deletion_request_already_sent = bool(app.get("deletion_request_sent_date"))

            if status == "Rejected":
                if deletion_request_already_sent:
                    continue
                if (
                    days >= self.deletion_request_days
                    and get_effective_action_policy(app, "deletion_request") == "ask_when_due"
                ):
                    candidates.append(
                        self._manual_review_candidate(
                            app,
                            "deletion_request",
                            f"Rejected — {days}d since last activity",
                        )
                    )
                continue

            if (
                days >= self.withdraw_days
                and get_effective_action_policy(app, "withdraw") == "ask_when_due"
            ):
                candidates.append(
                    self._manual_review_candidate(
                        app,
                        "withdraw",
                        f"Ghosted — {days}d since last activity",
                    )
                )
                continue

            if days >= self.follow_up_days and get_effective_action_policy(app, "follow_up") == "ask_when_due":
                last_fu = app.get("follow_up_sent_date")
                if last_fu:
                    try:
                        last_fu_date = datetime.fromisoformat(last_fu[:10]).date()
                        if (today - last_fu_date).days < self.follow_up_repeat_days:
                            continue
                    except ValueError:
                        pass
                follow_up_n = int(app.get("follow_up_count") or 0) + 1
                candidates.append(
                    self._manual_review_candidate(
                        app,
                        "follow_up",
                        f"{days}d inactive — follow-up #{follow_up_n}",
                        follow_up_n,
                    )
                )

        return candidates

    def compute_actions(
        self,
        applications: list[ApplicationRecord],
    ) -> list[PendingActionRecord]:
        today = self._today_provider()
        actions: list[PendingActionRecord] = []

        for app in applications:
            status = normalize_application_status(app.get("status"))
            manual_withdraw_requested = is_truthy_sheet_value(app.get("withdraw_in_next_digest"))

            if status_blocks_pipeline_actions(status):
                continue

            withdrawal_already_sent = bool(app.get("withdrawal_sent_date"))
            if withdrawal_already_sent:
                continue

            if manual_withdraw_requested and get_effective_action_policy(app, "withdraw") != "disabled":
                actions.append({
                    "type": "withdraw",
                    "app": dict(app),
                    "reason": "Manual withdrawal requested for next digest",
                })
                continue

            # Skip deferred entries
            deferred = app.get("deferred_until", "").strip()
            if deferred:
                try:
                    defer_date = datetime.fromisoformat(deferred[:10]).date()
                    if defer_date > today:
                        continue
                except ValueError:
                    pass

            ref_date = self._get_withdraw_reference_date(app)
            if ref_date is None:
                continue

            days = (today - ref_date).days
            app["_days"] = days  # attach for email generation

            deletion_request_already_sent = bool(app.get("deletion_request_sent_date"))

            if status == "Rejected":
                if deletion_request_already_sent or action_blocks_automatic_digest(
                    app,
                    "deletion_request",
                ):
                    continue
                if days >= self.deletion_request_days:
                    actions.append({
                        "type": "deletion_request",
                        "app": dict(app),
                        "reason": f"Rejected — {days}d since last activity",
                    })
                continue

            if (
                days >= self.withdraw_days
                and not action_blocks_automatic_digest(app, "withdraw")
            ):
                actions.append({
                    "type": "withdraw",
                    "app": dict(app),
                    "reason": f"Ghosted — {days}d since last activity",
                })
                continue

            # Follow-up threshold
            if days >= self.follow_up_days and not action_blocks_automatic_digest(app, "follow_up"):
                # Don't re-follow-up if we already sent one recently
                last_fu = app.get("follow_up_sent_date")
                if last_fu:
                    try:
                        last_fu_date = datetime.fromisoformat(last_fu[:10]).date()
                        if (today - last_fu_date).days < self.follow_up_repeat_days:
                            continue
                    except ValueError:
                        pass
                follow_up_n = int(app.get("follow_up_count") or 0) + 1
                actions.append({
                    "type": "follow_up",
                    "app": dict(app),
                    "reason": f"{days}d inactive — follow-up #{follow_up_n}",
                    "follow_up_n": follow_up_n,
                })

        return actions
