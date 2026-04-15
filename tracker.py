#!/usr/bin/env python3
# pyright: reportMissingTypeStubs=false
"""
Job Application Tracker
Syncs Gmail → Google Sheets via OpenAI grouping.
Handles follow-ups and withdrawals with daily digest + manual confirmation.

Usage:
  python tracker.py --sync            # fetch emails, update Sheets (run hourly via cron)
  python tracker.py --digest          # syncs first, then prepares today's actions + drafts
  python tracker.py --confirm         # review & send all pending drafts
  python tracker.py --add-linkedin    # add a LinkedIn application manually
  python tracker.py --delete-app ID   # delete one or more application rows without syncing
  python tracker.py --debug-app-emails ID # inspect AI grouping for one existing app row
  python tracker.py --debug-message-ids ID [ID ...] # inspect AI grouping for specific Gmail messages
  python tracker.py --resync-app ID   # force one email-backed application to be reprocessed
  python tracker.py --resync-all      # force all email-backed applications to be reprocessed
  python tracker.py --resume-run ID   # resume a saved AI grouping run after a fail-closed abort
"""

__version__ = "0.2.0"

import json
import uuid
import base64
import re
import sys
import argparse
import time
import html
import io
import calendar
import copy
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.utils import parseaddr, parsedate_to_datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any, Callable, Match, Optional, TypeAlias, TypeVar, cast
from urllib.parse import quote, urlparse, urljoin
from urllib.request import Request as UrlRequest, urlopen
import yaml
from rich.console import Console
from rich.table import Table
from rich.prompt import Confirm, Prompt
from rich.panel import Panel
import google.genai as genai
from google.genai import types

from google.auth.credentials import Credentials as GoogleAuthCredentials
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient import discovery as googleapiclient_discovery
from googleapiclient.errors import HttpError
import gspread
from pypdf import PdfReader

CredentialsLoader = cast(Any, Credentials)
InstalledAppFlowLoader = cast(Any, InstalledAppFlow)
google_api_build: Any = getattr(googleapiclient_discovery, "build")

ConfigDict: TypeAlias = dict[str, Any]
ApplicationRecord: TypeAlias = dict[str, Any]
GmailMessage: TypeAlias = dict[str, Any]
PendingActionRecord: TypeAlias = dict[str, Any]
JsonObject: TypeAlias = dict[str, Any]
ContactEmailCandidate: TypeAlias = dict[str, Any]

OperationResultT = TypeVar("OperationResultT")

# ── Paths ──────────────────────────────────────────────────────────────────────
BASE_DIR       = Path(__file__).parent
CONFIG_PATH    = BASE_DIR / "config.yaml"
TOKEN_PATH     = BASE_DIR / "credentials" / "token.json"
CREDS_PATH     = BASE_DIR / "credentials" / "google_credentials.json"
PENDING_PATH   = BASE_DIR / "pending_actions.json"
STATE_DIR      = BASE_DIR / "state"
PROCESSED_GMAIL_STATE_PATH = STATE_DIR / "processed_gmail_message_ids.json"
TEMPLATES_DIR  = BASE_DIR / "templates"
GROUPING_RUNS_DIR = STATE_DIR / "grouping_runs"

def _resolve_template(config: ConfigDict, key: str) -> Path:
    """Resolve a template path from config, falling back to the default location."""
    configured = config.get("templates", {}).get(key)
    if configured:
        p = Path(configured)
        return p if p.is_absolute() else BASE_DIR / p
    return TEMPLATES_DIR / f"{key}.txt"


def _resolve_follow_up_template(config: ConfigDict, follow_up_n: int) -> Path:
    """Resolve the follow-up template path for the requested follow-up number."""
    if follow_up_n >= 2:
        return _resolve_template(config, "follow_up_second")
    return _resolve_template(config, "follow_up")

SCOPES = [
    "https://www.googleapis.com/auth/gmail.modify",
    "https://www.googleapis.com/auth/gmail.labels",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file",
]

# Known ATS email domains — used to identify automated vs human senders
ATS_DOMAINS = {
    "greenhouse.io", "greenhouse-mail.io", "lever.co", "workday.com",
    "myworkdayjobs.com", "icims.com", "jobvite.com", "bamboohr.com",
    "smartrecruiters.com", "taleo.net", "successfactors.com", "ashbyhq.com",
    "rippling.com", "comeet.co", "recruitee.com", "workable.com",
    "breezy.hr", "jazz.co", "jazzhr.com", "pinpointhq.com", "teamtailor.com",
    "dover.com", "hired.com", "wellfound.com",
}
ATS_NOTIFICATION_DOMAIN_SUFFIXES = (
    "comeet-notifications.com",
)
TRUSTED_CONTACT_SOURCE_DOMAINS = ATS_DOMAINS.union({
    "teamtailor-mail.com",
    "greenhouse-mail.io",
    "smartrecruiters.com",
    "workday.com",
    "myworkdayjobs.com",
    "ashbyhq.com",
    "join.com",
    "lever.co",
    "workablemail.com",
})
SOCIAL_RELAY_DOMAINS = {
    "linkedin.com",
}
LINKEDIN_NOISE_DOMAINS = {
    "linkedin.com",
    "licdn.com",
    "w3.org",
}
LINKEDIN_NOISE_COMPANY_NAME_HINTS = {
    "linkedin",
    "linkedin login",
    "linkedin login sign in",
    "login",
    "sign in",
}

STATUS_RANK = {s: i for i, s in enumerate(
    ["Applied", "Screening", "Interview", "Assessment", "Offer", "Rejected", "Withdrawn", "Ghosted"]
)}

SHEETS_WRITE_CHUNK_SIZE = 200
SHEETS_RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
SHEETS_MAX_WRITE_RETRIES = 5
SHEETS_INITIAL_RETRY_DELAY_SECONDS = 2.0
DEFAULT_AI_GROUPING_BATCH_SIZE = 25
DEFAULT_AI_GROUPING_CLUSTER_SIZE = 10
DEFAULT_AI_GROUPING_CLUSTER_EMERGENCY_MARGIN = 2
DEFAULT_AI_RATE_LIMIT_MAX_WAIT_SECONDS = 60
DEFAULT_AI_RATE_LIMIT_MAX_RETRIES_PER_BATCH = 2
DEFAULT_AI_TRANSIENT_MODEL_RETRIES = 1
DEFAULT_AI_TRANSIENT_MODEL_INITIAL_RETRY_DELAY_SECONDS = 1.0
DEFAULT_AI_TRANSIENT_MODEL_BACKOFF_MULTIPLIER = 2.0
DEFAULT_GOOGLE_API_TRANSIENT_RETRIES = 1
DEFAULT_GOOGLE_API_TRANSIENT_INITIAL_RETRY_DELAY_SECONDS = 1.0
DEFAULT_GOOGLE_API_TRANSIENT_BACKOFF_MULTIPLIER = 2.0
FREE_TIER_GOOGLE_SEARCH_GROUNDED_MODEL_NAMES = (
    "gemini-2.5-flash",
    "gemini-2.5-flash-lite",
)
GMAIL_BATCH_MODIFY_MESSAGE_LIMIT = 1000
APPLICATION_ROLELESS_MERGE_WINDOW_DAYS = 60
ROLE_BACKFILL_LOOKBACK_DAYS = 365
ROLE_BACKFILL_MAX_RELATED_MESSAGES = 25
RESYNC_APP_MAX_RELATED_MESSAGES = 100
DEFAULT_PROCESSING_ROOT_LABEL = "application-tracker"
DEFAULT_PROCESSING_STAGE_NAMES = {
    "processed": "processed",
}
GMAIL_WEB_BASE_URL = "https://mail.google.com/mail/u/0"

console = Console()


class TrackerError(Exception):
    """Raised when the tracker hits a user-facing operational error."""


def is_transient_transport_disconnect(error_text: str) -> bool:
    """
    Detect transient transport-level disconnects before an HTTP API returns a response.

    Inputs:
    - error_text: Raw exception text returned by an SDK or HTTP client.

    Outputs:
    - True when the failure looks like a temporary socket/protocol disconnect.

    Edge cases:
    - Includes Windows socket abort wording (`WinError 10053`) in addition to the
      existing httpcore-style disconnect messages.

    Atomicity / concurrency:
    - Pure helper with no side effects.
    """
    normalized_error_text = str(error_text or "").lower()
    return (
        "server disconnected without sending a response" in normalized_error_text
        or "remoteprotocolerror" in normalized_error_text
        or "winerror 10053" in normalized_error_text
        or "an established connection was aborted by the software in your host machine" in normalized_error_text
        or "connection aborted" in normalized_error_text
    )


def is_json_parse_error_text(error_text: str) -> bool:
    """
    Detect common JSON parsing failures from Gemini structured-output responses.

    Inputs:
    - error_text: Raw exception text bubbled up from JSON parsing or validation.

    Outputs:
    - True when the text looks like malformed JSON returned by the model.

    Edge cases:
    - Supports both direct `json.JSONDecodeError` wording and simplified stringified
      error messages used in tests and wrapped exceptions.

    Atomicity / concurrency:
    - Pure helper with no side effects.
    """
    normalized_error_text = str(error_text or "").lower()
    return any(
        token in normalized_error_text
        for token in (
            "unterminated string",
            "expecting value",
            "expecting property name",
            "extra data",
            "invalid control character",
            "invalid \\escape",
            "delimiter",
            "jsondecodeerror",
        )
    )


def _persist_google_token(credentials: GoogleAuthCredentials) -> None:
    """
    Persist Google OAuth credentials to the local token file.

    Inputs:
    - credentials: The OAuth credentials object to serialize to `TOKEN_PATH`.

    Outputs:
    - None. Writes the serialized credentials to disk.

    Edge cases:
    - Raises `TrackerError` when the token directory cannot be created or the
      token file cannot be written.

    Atomicity / concurrency:
    - Performs one local write per call. It does not coordinate across
      processes, so concurrent writers may overwrite each other.
    """
    try:
        TOKEN_PATH.parent.mkdir(parents=True, exist_ok=True)
        TOKEN_PATH.write_text(credentials.to_json(), encoding="utf-8")
    except OSError as error:
        raise TrackerError(f"Could not write Google token to {TOKEN_PATH}: {error}") from error


def _run_google_oauth_flow() -> GoogleAuthCredentials:
    """
    Start the browser-based Google OAuth flow and return fresh credentials.

    Inputs:
    - None. Uses `CREDS_PATH` and the configured OAuth scopes from disk.

    Outputs:
    - A newly authorized Google OAuth credentials object.

    Edge cases:
    - Raises `TrackerError` when the client credentials file is missing or when
      the OAuth browser flow fails.

    Atomicity / concurrency:
    - Performs one interactive OAuth flow for the current process. It does not
      provide any cross-process locking.
    """
    if not CREDS_PATH.exists():
        raise TrackerError(f"Google credentials not found at {CREDS_PATH}")

    try:
        flow = InstalledAppFlowLoader.from_client_secrets_file(str(CREDS_PATH), SCOPES)
        # WSL: if browser doesn't open, set open_browser=False and paste URL manually
        return flow.run_local_server(port=0, open_browser=True)
    except Exception as error:
        raise TrackerError(f"Google OAuth flow failed using {CREDS_PATH}: {error}") from error


def _refresh_or_reauthorize_google_credentials(
    credentials: GoogleAuthCredentials,
    token_path_for_errors: Path = TOKEN_PATH,
) -> GoogleAuthCredentials:
    """
    Refresh Google OAuth credentials and fall back to reauthorization when needed.

    Inputs:
    - credentials: The previously stored Google OAuth credentials loaded from
      disk.
    - token_path_for_errors: Token path used only to provide precise error
      context in exception messages.

    Outputs:
    - A valid Google OAuth credentials object, either refreshed in place or
      recreated through a new OAuth browser flow.

    Edge cases:
    - When refresh fails with `invalid_grant`, this function assumes the refresh
      token was revoked or expired and runs a fresh OAuth flow.
    - Other refresh failures raise `TrackerError` with explicit context.

    Atomicity / concurrency:
    - Performs at most one refresh attempt followed by at most one interactive
      OAuth flow in the current process.
    """
    try:
        credentials.refresh(Request())
        return credentials
    except Exception as error:
        error_message = str(error)
        if "invalid_grant" not in error_message:
            raise TrackerError(f"Could not refresh Google token at {token_path_for_errors}: {error}") from error

    return _run_google_oauth_flow()


class GeminiRateLimitError(TrackerError):
    """
    Raised when Gemini rejects a request due to rate limiting or quota exhaustion.

    Inputs:
    - message: Human-readable error summary.
    - retry_delay_seconds: Suggested backoff time from the provider, if present.
    - is_daily_quota_exhausted: Whether the request failed because the model's daily quota
      is depleted and immediate retries are unlikely to succeed.

    Outputs:
    - Exception instance carrying retry metadata for the sync loop.

    Edge cases:
    - retry_delay_seconds may be None when the provider does not return a retry hint.

    Atomicity / concurrency:
    - Immutable exception object with no shared mutable state.
    """

    def __init__(
        self,
        message: str,
        retry_delay_seconds: Optional[int] = None,
        is_daily_quota_exhausted: bool = False,
    ):
        super().__init__(message)
        self.retry_delay_seconds = retry_delay_seconds
        self.is_daily_quota_exhausted = is_daily_quota_exhausted


class GmailLabelConflictError(TrackerError):
    """
    Raised when Gmail rejects a label name because it conflicts with an existing hierarchy.

    Inputs:
    - message: Human-readable summary of the conflicting Gmail label state.

    Outputs:
    - Exception instance that callers can catch when a label is optional.

    Edge cases:
    - Used only for 409 Gmail label-creation conflicts that remain unresolved after refresh.

    Atomicity / concurrency:
    - Immutable exception object with no shared mutable state.
    """


@dataclass(frozen=True)
class EmailGroupingResult:
    """
    Structured result of grouping a Gmail batch into application updates.

    Inputs:
    - updates: Unique application rows that should be inserted or updated in Sheets.
    - ignored_email_count: Number of emails the model classified as irrelevant.
    - handled_message_ids: Gmail message IDs that received a definitive action in this batch.
    - matched_existing_email_count: Number of emails attached to existing applications.
    - new_application_email_count: Number of emails classified as belonging to newly created
      applications in this batch.
    - updated_existing_application_count: Number of distinct existing applications updated.
    - created_application_count: Number of distinct new applications created.

    Outputs:
    - Immutable batch summary that callers can aggregate for sync reporting.

    Edge cases:
    - Multiple emails may collapse into one update when they map to the same application.
    - Counts reflect the model response after filtering invalid or missing matches.

    Atomicity / concurrency:
    - Immutable value object with no shared mutable state.
    """

    updates: list[ApplicationRecord]
    ignored_email_count: int
    handled_message_ids: list[str]
    matched_existing_email_count: int
    new_application_email_count: int
    updated_existing_application_count: int
    created_application_count: int


@dataclass(frozen=True)
class GmailFetchResult:
    """
    Structured result of scanning Gmail for matching messages and parsing unseen ones.

    Inputs:
    - total_matching_email_count: Number of Gmail messages that matched the query within the
      requested lookback window.
    - unseen_emails: Parsed Gmail messages whose IDs were not already recorded in Sheets.

    Outputs:
    - Immutable fetch summary that lets the sync flow report both total matches and newly
      discovered emails.

    Edge cases:
    - When all matching message IDs are already known, unseen_emails is empty.
    - Matching messages are counted even when parsing later fails for some unseen IDs.

    Atomicity / concurrency:
    - Immutable value object with no shared mutable state.
    """

    total_matching_email_count: int
    unseen_emails: list[GmailMessage]


@dataclass(frozen=True)
class GmailProcessingLabelConfig:
    """
    Describe the Gmail labels used to track pipeline progress for processed messages.

    Inputs:
    - root_label_name: Top-level Gmail label applied to all tracker-managed messages.
    - stage_label_names: Mapping of pipeline stage keys to full Gmail label names.

    Outputs:
    - Immutable label configuration with helpers to resolve stage names.

    Edge cases:
    - Stage labels may be configured as short names and are expanded under the root label.
    - The required `processed` stage must always be present.

    Atomicity / concurrency:
    - Immutable value object with no shared mutable state.
    """

    root_label_name: str
    stage_label_names: dict[str, str]

    @classmethod
    def from_config(cls, config: ConfigDict) -> "GmailProcessingLabelConfig":
        """
        Build Gmail processing label configuration from config.yaml.

        Inputs:
        - config: Parsed application configuration dictionary.

        Outputs:
        - GmailProcessingLabelConfig populated with a root label and stage labels.

        Edge cases:
        - Missing configuration falls back to sensible defaults.
        - Fully qualified stage label names are preserved as-is.

        Atomicity / concurrency:
        - Pure helper with no shared mutable state.
        """
        gmail_config = config.get("gmail", {})
        processing_label_config = gmail_config.get("processing_labels", {})
        root_label_name = str(
            processing_label_config.get("root", DEFAULT_PROCESSING_ROOT_LABEL)
        ).strip() or DEFAULT_PROCESSING_ROOT_LABEL

        configured_stage_names = processing_label_config.get("stages", {})
        if configured_stage_names and not isinstance(configured_stage_names, dict):
            raise TrackerError("config.yaml field gmail.processing_labels.stages must be a YAML object")
        if isinstance(configured_stage_names, dict):
            configured_stage_names = cast(dict[str, str], configured_stage_names)
        else:
            configured_stage_names = {}

        stage_label_names: dict[str, str] = {}
        merged_stage_names = dict(DEFAULT_PROCESSING_STAGE_NAMES)
        merged_stage_names.update(configured_stage_names or {})

        for stage_key, configured_stage_name in merged_stage_names.items():
            normalized_stage_key = str(stage_key).strip()
            normalized_stage_name = str(configured_stage_name).strip()
            if not normalized_stage_key or not normalized_stage_name:
                continue
            if normalized_stage_name.startswith(f"{root_label_name}/"):
                full_label_name = normalized_stage_name
            elif "/" in normalized_stage_name:
                full_label_name = normalized_stage_name
            else:
                full_label_name = f"{root_label_name}/{normalized_stage_name}"
            stage_label_names[normalized_stage_key] = full_label_name

        if "processed" not in stage_label_names:
            raise TrackerError("config.yaml must define gmail.processing_labels.stages.processed")

        return cls(
            root_label_name=root_label_name,
            stage_label_names=stage_label_names,
        )

    def get_stage_label_name(self, stage_key: str) -> str:
        """
        Resolve the full Gmail label name for a configured pipeline stage.

        Inputs:
        - stage_key: Stable stage identifier such as `processed`.

        Outputs:
        - Full Gmail label name for that stage.

        Edge cases:
        - Missing stage keys raise TrackerError with actionable context.

        Atomicity / concurrency:
        - Pure helper with no shared mutable state.
        """
        try:
            return self.stage_label_names[stage_key]
        except KeyError as error:
            raise TrackerError(
                f"Gmail processing label stage '{stage_key}' is not configured"
            ) from error

    def all_label_names(self) -> list[str]:
        """
        Return every Gmail label name the tracker needs for its processing pipeline.

        Inputs:
        - None.

        Outputs:
        - Deduplicated label names containing the root label and every stage label.

        Edge cases:
        - Duplicate label names are collapsed while preserving order.

        Atomicity / concurrency:
        - Pure helper with no shared mutable state.
        """
        return deduplicate_preserving_order(
            [self.root_label_name, *self.stage_label_names.values()]
        )


@dataclass(frozen=True)
class ProcessedMessageStateSnapshot:
    """
    Represent the local processed-message backstop state loaded from disk.

    Inputs:
    - processed_at_by_message_id: Mapping of Gmail message IDs to ISO timestamps describing
      when the tracker finished handling them.

    Outputs:
    - Immutable snapshot that callers can query and update.

    Edge cases:
    - Invalid or missing timestamps are tolerated during pruning and overwritten on save.

    Atomicity / concurrency:
    - Immutable value object with no shared mutable state.
    """

    processed_at_by_message_id: dict[str, str]


def deduplicate_preserving_order(values: list[str]) -> list[str]:
    """
    Remove duplicate strings while preserving their original order.

    Inputs:
    - values: List of strings that may contain duplicates.

    Outputs:
    - New list containing the first occurrence of each non-empty string.

    Edge cases:
    - Empty strings are ignored.

    Atomicity / concurrency:
    - Pure helper with no shared mutable state.
    """
    deduplicated_values: list[str] = []
    seen_values: set[str] = set()
    for value in values:
        normalized_value = value.strip()
        if not normalized_value or normalized_value in seen_values:
            continue
        deduplicated_values.append(normalized_value)
        seen_values.add(normalized_value)
    return deduplicated_values


def fail_with_message(message: str) -> None:
    """Print a human-readable error and exit the program."""
    console.print(f"[red]{message}[/red]")
    sys.exit(1)


def safe_json_loads(
    raw_text: str,
    context: str,
    default: object,
) -> Any:
    """
    Parse JSON text and fall back to a provided default on malformed data.

    Inputs:
    - raw_text: JSON string to parse.
    - context: Human-readable description included in warnings.
    - default: Value returned when parsing fails.

    Outputs:
    - Parsed JSON value, or default when the text is invalid JSON.

    Edge cases:
    - Empty strings and malformed content are treated as invalid JSON.
    - The function never raises JSONDecodeError.

    Atomicity / concurrency:
    - Pure helper with no shared mutable state.
    """
    try:
        return json.loads(raw_text)
    except json.JSONDecodeError as error:
        console.print(f"[yellow]Invalid JSON in {context}: {error}. Using fallback value.[/yellow]")
        return default


def extract_first_json_object(raw_text: str) -> Optional[JsonObject]:
    """
    Extract the first JSON object embedded in a model response.

    Inputs:
    - raw_text: Arbitrary model output that may contain markdown fences or surrounding prose.

    Outputs:
    - Parsed JSON object, or None when no JSON object can be decoded.

    Edge cases:
    - Supports fenced ```json blocks and inline JSON objects.
    - Ignores leading explanatory text before the first `{`.

    Atomicity / concurrency:
    - Pure helper with no shared mutable state.
    """
    normalized_text = str(raw_text or "").strip()
    if not normalized_text:
        return None

    fence_match = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", normalized_text, flags=re.IGNORECASE | re.DOTALL)
    if fence_match:
        normalized_text = fence_match.group(1).strip()

    try:
        parsed_value = json.loads(normalized_text)
        if isinstance(parsed_value, dict):
            return cast(JsonObject, parsed_value)
    except json.JSONDecodeError:
        pass

    decoder = json.JSONDecoder()
    for character_index, character in enumerate(normalized_text):
        if character != "{":
            continue
        try:
            parsed_value, _end_index = decoder.raw_decode(normalized_text[character_index:])
        except json.JSONDecodeError:
            continue
        if isinstance(parsed_value, dict):
            return cast(JsonObject, parsed_value)
    return None


def build_gmail_label_query_name(label_name: str) -> str:
    """
    Convert a Gmail label display name into the search token used in Gmail queries.

    Inputs:
    - label_name: Gmail label display name such as `Job Search/Processed`.

    Outputs:
    - Search token fragment such as `Job-Search-Processed`.

    Edge cases:
    - Repeated spaces or slashes collapse into a single hyphen.

    Atomicity / concurrency:
    - Pure helper with no shared mutable state.
    """
    normalized_label_name = re.sub(r"[\s/]+", "-", label_name.strip())
    return normalized_label_name.strip("-")


def normalize_matching_text(value: str) -> str:
    """
    Normalize free-form text into a comparison-friendly token sequence.

    Inputs:
    - value: Raw text such as a company name, role title, or email-derived label.

    Outputs:
    - Lowercased text with punctuation collapsed to spaces.

    Edge cases:
    - Empty or whitespace-only values normalize to an empty string.

    Atomicity / concurrency:
    - Pure helper with no shared mutable state.
    """
    normalized_value = re.sub(r"[^a-z0-9]+", " ", str(value or "").lower())
    return " ".join(normalized_value.split())


def extract_email_address(raw_email_header: str) -> str:
    """
    Parse a plain email address from a mailbox header value.

    Inputs:
    - raw_email_header: Header-like value such as `Name <person@example.com>`.

    Outputs:
    - Lowercased email address, or an empty string when no address can be parsed.

    Edge cases:
    - Blank strings and malformed header values return an empty string.
    - Surrounding whitespace is ignored.

    Atomicity / concurrency:
    - Pure helper with no shared mutable state.
    """
    _, parsed_email_address = parseaddr(str(raw_email_header or "").strip())
    return parsed_email_address.strip().lower()


def extract_email_addresses_from_text(raw_text: str) -> list[str]:
    """
    Extract normalized email addresses from free-form text.

    Inputs:
    - raw_text: Arbitrary text that may contain one or more email addresses.

    Outputs:
    - Deduplicated normalized email addresses in first-seen order.

    Edge cases:
    - Malformed addresses are ignored.
    - Trailing punctuation such as `.` or `)` is stripped before validation.

    Atomicity / concurrency:
    - Pure helper with no shared mutable state.
    """
    extracted_email_addresses: list[str] = []
    for match in re.finditer(
        r"\b[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}\b",
        str(raw_text or ""),
        flags=re.IGNORECASE,
    ):
        email_address = match.group(0).strip(" \t\r\n<>\"'`.,;:()[]{}").lower()
        if email_address and email_address not in extracted_email_addresses:
            extracted_email_addresses.append(email_address)
    return extracted_email_addresses


def detect_text_language(raw_text: str) -> str:
    """
    Detect whether text looks primarily English or German.

    Inputs:
    - raw_text: Arbitrary free-form text.

    Outputs:
    - `"en"`, `"de"`, or `"unknown"`.

    Edge cases:
    - Very short text returns `"unknown"`.
    - Mixed-language text returns the stronger heuristic match.

    Atomicity / concurrency:
    - Pure helper with no shared mutable state.
    """
    normalized_text = str(raw_text or "").strip().lower()
    if len(normalized_text) < 20:
        return "unknown"

    english_markers = {
        "the", "and", "your", "application", "interview", "unfortunately",
        "regret", "position", "role", "candidate", "moving forward",
    }
    german_markers = {
        "und", "der", "die", "das", "ihre", "bewerbung", "gespraech",
        "gespräch", "leider", "bedauern", "stelle", "position", "absage",
        "abgelehnt", "datenschutz", "wir", "weiteren", "prozess",
    }

    english_score = sum(normalized_text.count(marker) for marker in english_markers)
    german_score = sum(normalized_text.count(marker) for marker in german_markers)
    if any(character in normalized_text for character in "äöüß"):
        german_score += 3

    if german_score >= english_score + 2 and german_score > 0:
        return "de"
    if english_score >= german_score + 2 and english_score > 0:
        return "en"
    return "unknown"


def get_message_text_sources(email_message: GmailMessage) -> list[str]:
    """
    Collect the message text fields that should be inspected for extraction logic.
    """
    return [
        str(email_message.get("subject", "") or ""),
        str(email_message.get("snippet", "") or ""),
        str(email_message.get("body", "") or ""),
        str(email_message.get("attachment_text", "") or ""),
    ]


def extract_http_urls_from_text(raw_text: str) -> list[str]:
    """
    Extract HTTP(S) URLs from free-form text.

    Inputs:
    - raw_text: Arbitrary text that may contain URLs.

    Outputs:
    - Deduplicated URLs in first-seen order.

    Edge cases:
    - Trailing punctuation is stripped.

    Atomicity / concurrency:
    - Pure helper with no shared mutable state.
    """
    extracted_urls: list[str] = []
    for match in re.finditer(r"https?://[^\s<>\"]+", str(raw_text or ""), flags=re.IGNORECASE):
        url = match.group(0).strip(" \t\r\n<>\"'`.,;:()[]{}")
        if url and url not in extracted_urls:
            extracted_urls.append(url)
    return extracted_urls


def extract_linkedin_job_urls_from_text(raw_text: str) -> list[str]:
    """
    Extract LinkedIn job-page URLs from free-form text.

    Inputs:
    - raw_text: Arbitrary text that may contain LinkedIn job links.

    Outputs:
    - Deduplicated LinkedIn job URLs in first-seen order.

    Edge cases:
    - Supports both `linkedin.com/jobs/view/...` and `linkedin.com/comm/jobs/view/...`.

    Atomicity / concurrency:
    - Pure helper with no shared mutable state.
    """
    linkedin_job_urls: list[str] = []
    for candidate_url in extract_http_urls_from_text(raw_text):
        parsed_url = urlparse(candidate_url)
        hostname = str(parsed_url.hostname or "").lower()
        normalized_path = str(parsed_url.path or "").lower()
        if "linkedin.com" not in hostname:
            continue
        if "/jobs/view/" not in normalized_path and "/comm/jobs/view/" not in normalized_path:
            continue
        if candidate_url not in linkedin_job_urls:
            linkedin_job_urls.append(candidate_url)
    return linkedin_job_urls


def extract_linkedin_company_urls_from_text(raw_text: str) -> list[str]:
    """
    Extract LinkedIn company-page URLs from free-form text.

    Inputs:
    - raw_text: Arbitrary text that may contain LinkedIn company links.

    Outputs:
    - Deduplicated LinkedIn company URLs in first-seen order.

    Edge cases:
    - Supports `linkedin.com/company/...` and `linkedin.com/comm/company/...`.

    Atomicity / concurrency:
    - Pure helper with no shared mutable state.
    """
    linkedin_company_urls: list[str] = []
    for candidate_url in extract_http_urls_from_text(raw_text):
        parsed_url = urlparse(candidate_url)
        hostname = str(parsed_url.hostname or "").lower()
        normalized_path = str(parsed_url.path or "").lower()
        if "linkedin.com" not in hostname:
            continue
        if "/company/" not in normalized_path and "/comm/company/" not in normalized_path:
            continue
        if candidate_url not in linkedin_company_urls:
            linkedin_company_urls.append(candidate_url)
    return linkedin_company_urls


def get_base_domain(hostname: str) -> str:
    """
    Reduce a hostname to a coarse registrable-domain approximation.

    Inputs:
    - hostname: Hostname or domain string.

    Outputs:
    - Lowercased base domain such as `example.com`, or an empty string when unavailable.

    Edge cases:
    - Handles common two-level public suffixes such as `co.uk`.

    Atomicity / concurrency:
    - Pure helper with no shared mutable state.
    """
    normalized_hostname = str(hostname or "").strip().lower().strip(".")
    if not normalized_hostname:
        return ""
    hostname_parts = [part for part in normalized_hostname.split(".") if part]
    if len(hostname_parts) <= 2:
        return normalized_hostname
    common_two_level_suffixes = {
        "co.uk",
        "org.uk",
        "ac.uk",
        "com.au",
        "com.br",
        "co.il",
    }
    last_two_parts = ".".join(hostname_parts[-2:])
    last_three_parts = ".".join(hostname_parts[-3:])
    if last_two_parts in common_two_level_suffixes and len(hostname_parts) >= 3:
        return last_three_parts
    return last_two_parts


def get_primary_domain_label(hostname: str) -> str:
    """
    Extract the normalized left-most label from a base domain.
    """
    base_domain = get_base_domain(hostname)
    if not base_domain:
        return ""
    return re.sub(r"[^a-z0-9]+", "", base_domain.split(".", 1)[0].lower())


def is_trusted_contact_source_domain(email_domain: str) -> bool:
    """
    Determine whether a domain is an allowed non-official source for contact discovery.

    Inputs:
    - email_domain: Lowercased page or email domain.

    Outputs:
    - True when the domain belongs to a trusted ATS/careers host.

    Edge cases:
    - Blank domains return False.

    Atomicity / concurrency:
    - Pure helper with no shared mutable state.
    """
    normalized_email_domain = str(email_domain or "").strip().lower()
    if not normalized_email_domain:
        return False
    return any(
        normalized_email_domain == trusted_domain
        or normalized_email_domain.endswith(f".{trusted_domain}")
        for trusted_domain in TRUSTED_CONTACT_SOURCE_DOMAINS
    )


def is_linkedin_noise_domain(email_domain: str) -> bool:
    """
    Detect LinkedIn-related or generic infrastructure domains that should not be treated as
    employer domains during LinkedIn hint discovery.
    """
    normalized_email_domain = get_base_domain(str(email_domain or "").strip().lower())
    if not normalized_email_domain:
        return False
    return normalized_email_domain in LINKEDIN_NOISE_DOMAINS


def is_linkedin_noise_company_name_hint(company_name: str) -> bool:
    """
    Detect low-value company-name hints extracted from LinkedIn login/chrome pages.
    """
    normalized_company_name = normalize_matching_text(company_name)
    if not normalized_company_name:
        return True
    if normalized_company_name in LINKEDIN_NOISE_COMPANY_NAME_HINTS:
        return True
    return normalized_company_name.startswith("linkedin ")


def is_unusable_recruiter_name(candidate_name: str) -> bool:
    """
    Detect recruiter/contact names that are really platform or placeholder labels.
    """
    normalized_candidate_name = normalize_matching_text(candidate_name)
    if not normalized_candidate_name:
        return True
    if is_linkedin_noise_company_name_hint(candidate_name):
        return True
    return normalized_candidate_name in {
        "hiring team",
        "talent acquisition team",
        "recruiting team",
        "careers",
        "jobs",
        "team",
        "company",
    }


def build_company_search_tokens(company_name: str) -> list[str]:
    """
    Build normalized company tokens useful for relevance checks against URLs/domains.
    """
    normalized_company_name = normalize_matching_text(company_name)
    if not normalized_company_name:
        return []
    ignored_tokens = {
        "group",
        "company",
        "companies",
        "solutions",
        "technologies",
        "technology",
        "llc",
        "ltd",
        "inc",
        "corp",
        "corporation",
        "gmbh",
        "limited",
        "plc",
        "ag",
        "sa",
        "bv",
    }
    normalized_company_name_parts = [
        normalized_company_name_part
        for normalized_company_name_part in normalized_company_name.split()
        if normalized_company_name_part and normalized_company_name_part not in ignored_tokens
    ]
    token_candidates = [
        token
        for token in normalized_company_name_parts
        if len(token) >= 4 and token not in ignored_tokens
    ]
    core_joined_token = "".join(normalized_company_name_parts)
    if len(core_joined_token) >= 4:
        token_candidates.append(core_joined_token)
    joined_token = "".join(normalized_company_name.split())
    if len(joined_token) >= 4:
        token_candidates.append(joined_token)
    return deduplicate_preserving_order(token_candidates)


def is_search_result_likely_related_to_company(
    candidate_url: str,
    company_name_hints: list[str],
    known_domains: set[str],
) -> bool:
    """
    Decide whether a search result URL is plausibly related to the target company.
    """
    parsed_url = urlparse(candidate_url)
    page_hostname = str(parsed_url.hostname or "").lower()
    page_base_domain = get_base_domain(page_hostname)
    if not page_base_domain:
        return False
    if page_base_domain in known_domains or is_trusted_contact_source_domain(page_hostname):
        return True

    normalized_url_text = normalize_matching_text(candidate_url)
    for company_name_hint in company_name_hints:
        for company_search_token in build_company_search_tokens(company_name_hint):
            if company_search_token in normalize_matching_text(page_base_domain):
                return True
            if company_search_token in normalized_url_text:
                return True
    return False


def infer_company_hint_from_sender_header(raw_email_header: str) -> str:
    """
    Infer a company name from the display-name portion of an email header.

    Inputs:
    - raw_email_header: Header-like value such as `Varonis Recruiting Team <no-reply@jobvite.com>`.

    Outputs:
    - Best-effort company hint such as `Varonis`, or an empty string when the display name
      looks generic or unusable.

    Edge cases:
    - Recruiting/team boilerplate is stripped before returning the hint.
    - Generic names such as `notifications` are rejected to avoid false positives.

    Atomicity / concurrency:
    - Pure helper with no shared mutable state.
    """
    display_name, _ = parseaddr(str(raw_email_header or "").strip())
    cleaned_display_name = re.sub(
        r"\b(recruiting team|recruiting|talent acquisition|careers?|jobs?|job alerts|hr|people ops|hiring team|team|notifications?)\b",
        "",
        display_name,
        flags=re.IGNORECASE,
    )
    cleaned_display_name = re.sub(r"[^A-Za-z0-9&.\- ]+", " ", cleaned_display_name)
    cleaned_display_name = " ".join(cleaned_display_name.split()).strip(" -_.")
    if not cleaned_display_name:
        return ""

    normalized_cleaned_name = normalize_matching_text(cleaned_display_name)
    if normalized_cleaned_name in {"notification", "notifications", "no reply", "noreply", "scheduler"}:
        return ""
    if len(cleaned_display_name) > 60:
        return ""
    return cleaned_display_name


def is_low_confidence_company_name(company_name: str) -> bool:
    """
    Detect company names that are too weak for role-less merge decisions.

    Inputs:
    - company_name: Candidate company string.

    Outputs:
    - True when the company signal is too short or generic to safely drive weak merges.

    Edge cases:
    - Two-letter names such as `Vi` are treated as low-confidence because they are highly
      ambiguous in email text and AI matching.

    Atomicity / concurrency:
    - Pure helper with no shared mutable state.
    """
    normalized_company_name = normalize_matching_text(company_name)
    if not normalized_company_name:
        return True
    if len(normalized_company_name) < 3:
        return True
    return normalized_company_name in {
        "team",
        "careers",
        "jobs",
        "recruiting",
        "recruitment",
        "the",
        "company",
        "our company",
        "open",
        "position",
        "role",
    }


def build_deterministic_appl_id(company_name: str, role_title: str, applied_date: str) -> str:
    """
    Build a short stable fallback app ID from deterministic application fields.

    Inputs:
    - company_name: Employer name for the application.
    - role_title: Job title for the application.
    - applied_date: ISO-like application date when available.

    Outputs:
    - Short uppercase app ID suitable for sheet storage.

    Edge cases:
    - Missing fields fall back to generic placeholders so the result is never blank.

    Atomicity / concurrency:
    - Pure helper with no shared mutable state.
    """
    normalized_company_name = normalize_matching_text(company_name) or "app"
    normalized_role_title = normalize_matching_text(role_title) or "role"
    normalized_applied_date = str(applied_date or "").strip() or datetime.now().strftime("%Y-%m-%d")

    company_token = "".join(ch for ch in normalized_company_name.upper() if ch.isalnum())[:3] or "APP"
    role_token = "".join(ch for ch in normalized_role_title.upper() if ch.isalnum())[:3] or "ROL"
    date_token = normalized_applied_date.replace("-", "")[-4:]
    return f"{company_token}-{role_token}-{date_token}"


def extract_email_domain(raw_email_header: str) -> str:
    """
    Extract the domain part from an email or mailbox header value.

    Inputs:
    - raw_email_header: Raw email address or header string.

    Outputs:
    - Lowercased domain portion after `@`, or an empty string when unavailable.

    Edge cases:
    - Malformed addresses without `@` return an empty string.

    Atomicity / concurrency:
    - Pure helper with no shared mutable state.
    """
    normalized_email_address = extract_email_address(raw_email_header)
    if "@" not in normalized_email_address:
        return ""
    return normalized_email_address.rsplit("@", 1)[1]


def is_ats_sender_domain(email_domain: str) -> bool:
    """
    Determine whether a sender domain belongs to an ATS or ATS notification host.

    Inputs:
    - email_domain: Lowercased sender domain such as `jobs.lever.co`.

    Outputs:
    - True when the domain matches a known ATS vendor directly or as a subdomain.

    Edge cases:
    - Blank domains return False.
    - Subdomains such as `tikalk.comeet-notifications.com` are treated as ATS hosts.

    Atomicity / concurrency:
    - Pure helper with no shared mutable state.
    """
    normalized_email_domain = str(email_domain or "").strip().lower()
    if not normalized_email_domain:
        return False

    all_known_ats_domains = ATS_DOMAINS.union(ATS_NOTIFICATION_DOMAIN_SUFFIXES)
    return any(
        normalized_email_domain == ats_domain
        or normalized_email_domain.endswith(f".{ats_domain}")
        for ats_domain in all_known_ats_domains
    )


def is_generic_ats_sender_address(sender_address: str) -> bool:
    """
    Detect ATS sender addresses that are too generic to use for related-message backfills.

    Inputs:
    - sender_address: Lowercased sender email address.

    Outputs:
    - True when the local part is a generic mailbox such as `scheduler` or `no-reply`.

    Edge cases:
    - Blank or malformed addresses return False.

    Atomicity / concurrency:
    - Pure helper with no shared mutable state.
    """
    normalized_sender_address = str(sender_address or "").strip().lower()
    if "@" not in normalized_sender_address:
        return False
    local_part, sender_domain = normalized_sender_address.split("@", 1)
    generic_local_parts = {"scheduler", "notification", "notifications", "no-reply", "noreply"}
    return local_part in generic_local_parts and is_ats_sender_domain(sender_domain)


def is_company_scoped_comeet_reply_address(email_address: str) -> bool:
    """
    Detect reply-capable Comeet notification addresses scoped to a specific employer.

    Inputs:
    - email_address: Raw or normalized email address candidate.

    Outputs:
    - True when the address looks like a company-scoped Comeet reply mailbox such as
      `person-notifications+reply-...@xmcyber.comeet-notifications.com`.

    Edge cases:
    - Generic or malformed Comeet addresses return False.

    Atomicity / concurrency:
    - Pure helper with no shared mutable state.
    """
    normalized_email_address = extract_email_address(email_address).strip().lower()
    if "@" not in normalized_email_address:
        return False

    local_part, sender_domain = normalized_email_address.split("@", 1)
    if not sender_domain.endswith(".comeet-notifications.com"):
        return False
    if sender_domain == "comeet-notifications.com":
        return False

    normalized_local_part = re.sub(r"[^a-z0-9]+", "", local_part)
    if "reply" not in normalized_local_part:
        return False
    if normalized_local_part in {"notificationreply", "notificationsreply", "noreply"}:
        return False
    return True


def is_unusable_outbound_email(email_address: str) -> bool:
    """
    Detect mailbox addresses that should not be used as outbound recipients.

    Inputs:
    - email_address: Raw or normalized email address candidate.

    Outputs:
    - True when the address is blank, malformed, or clearly non-replyable.

    Edge cases:
    - Display-name headers are normalized through `extract_email_address`.
    - Generic ATS sender addresses are treated as unusable even when they do not literally
      contain `no-reply` in the original raw value.

    Atomicity / concurrency:
    - Pure helper with no shared mutable state.
    """
    normalized_email_address = extract_email_address(email_address).strip().lower()
    if "@" not in normalized_email_address:
        return True

    if is_company_scoped_comeet_reply_address(normalized_email_address):
        return False

    local_part, _sender_domain = normalized_email_address.split("@", 1)
    normalized_local_part = re.sub(r"[^a-z0-9]+", "", local_part)
    blocked_local_parts = {
        "noreply",
        "donotreply",
        "mailerdaemon",
        "postmaster",
        "notifications",
        "notification",
    }
    if normalized_local_part in blocked_local_parts:
        return True

    blocked_local_part_markers = (
        "noreply",
        "donotreply",
        "mailerdaemon",
        "postmaster",
        "notification",
        "notifications",
        "jobsnoreply",
        "inmail",
    )
    if any(marker in normalized_local_part for marker in blocked_local_part_markers):
        return True

    return is_generic_ats_sender_address(normalized_email_address)


def choose_preferred_replyable_email(*email_candidates: str) -> str:
    """
    Choose the first usable replyable email from one or more candidates.

    Inputs:
    - email_candidates: Raw email or mailbox-header values ordered by preference.

    Outputs:
    - Normalized email address, or an empty string when every candidate is unusable.

    Edge cases:
    - Mailbox headers are normalized through `extract_email_address`.
    - Generic ATS no-reply mailboxes are skipped in favor of later candidates.

    Atomicity / concurrency:
    - Pure helper with no shared mutable state.
    """
    for email_candidate in email_candidates:
        normalized_email_candidate = extract_email_address(str(email_candidate or ""))
        if normalized_email_candidate and not is_unusable_outbound_email(normalized_email_candidate):
            return normalized_email_candidate
    return ""


def score_contact_email_for_action(email_address: str, action_type: str) -> int:
    """
    Score how suitable an email address is for a specific outbound action.

    Inputs:
    - email_address: Candidate email address.
    - action_type: Pending action type such as `follow_up`, `withdraw`, or `deletion_request`.

    Outputs:
    - Integer suitability score where higher is better.

    Edge cases:
    - Unusable or malformed addresses receive a large negative score.
    - Privacy/legal addresses score highly for deletion-style actions and poorly for follow-ups.

    Atomicity / concurrency:
    - Pure helper with no shared mutable state.
    """
    normalized_email_address = extract_email_address(email_address).strip().lower()
    if not normalized_email_address or is_unusable_outbound_email(normalized_email_address):
        return -10_000

    local_part, sender_domain = normalized_email_address.split("@", 1)
    normalized_local_part = re.sub(r"[^a-z0-9]+", "", local_part)
    normalized_sender_domain = sender_domain.strip().lower()
    score = 0

    if is_company_scoped_comeet_reply_address(normalized_email_address):
        score += 120 if action_type == "follow_up" else 20
    if not is_ats_sender_domain(normalized_sender_domain):
        score += 35

    privacy_markers = PRIVACY_CONTACT_LOCAL_PART_MARKERS
    hiring_team_markers = ("recruit", "recruiting", "talent", "careers", "career", "jobs", "hiring", "hr", "people")
    generic_markers = ("info", "hello", "contact", "support", "office", "team")
    person_like_markers = ("firstname.lastname",)

    if any(marker in normalized_local_part for marker in privacy_markers):
        score += 140 if action_type in {"withdraw", "deletion_request"} else -120
    if any(marker in normalized_local_part for marker in hiring_team_markers):
        score += 140 if action_type == "follow_up" else 35
    if any(marker in normalized_local_part for marker in generic_markers):
        score += 30 if action_type in {"withdraw", "deletion_request"} else 15
    if "." in local_part and not any(marker in normalized_local_part for marker in privacy_markers + hiring_team_markers):
        score += 90 if action_type == "follow_up" else 20
    if normalized_local_part.startswith(("hr", "talent", "recruit", "career", "privacy", "gdpr", "dpo")):
        score += 20
    if any(marker in normalized_local_part for marker in ("ceo", "founder")) and action_type == "follow_up":
        score -= 20
    if any(marker in normalized_local_part for marker in ("ceo", "founder")) and action_type in {"withdraw", "deletion_request"}:
        score -= 60
    if any(marker in normalized_local_part for marker in person_like_markers):
        score += 0

    return score


PRIVACY_CONTACT_LOCAL_PART_MARKERS = (
    "privacy",
    "gdpr",
    "dpo",
    "dataprotection",
    "dataprivacy",
    "legal",
    "compliance",
    "datenschutz",
)


def is_privacy_style_contact_email(email_address: str) -> bool:
    """
    Check whether an email address clearly looks like a privacy/GDPR/legal contact.

    Inputs:
    - email_address: Candidate email address to evaluate.

    Outputs:
    - True when the local part contains common privacy mailbox markers.

    Edge cases:
    - Malformed or unusable email addresses return False.

    Atomicity / concurrency:
    - Pure helper with no shared mutable state.
    """
    normalized_email_address = extract_email_address(email_address).strip().lower()
    if not normalized_email_address or is_unusable_outbound_email(normalized_email_address):
        return False
    local_part = normalized_email_address.split("@", 1)[0]
    normalized_local_part = re.sub(r"[^a-z0-9]+", "", local_part)
    return any(marker in normalized_local_part for marker in PRIVACY_CONTACT_LOCAL_PART_MARKERS)


def choose_best_contact_email_for_action(
    email_candidates: list[str],
    action_type: str,
) -> str:
    """
    Choose the best usable email candidate for a specific outbound action.

    Inputs:
    - email_candidates: Candidate email addresses ordered by discovery order.
    - action_type: Pending action type such as `follow_up`, `withdraw`, or `deletion_request`.

    Outputs:
    - Best candidate email address, or an empty string when none are usable.

    Edge cases:
    - Ties are broken by original discovery order.

    Atomicity / concurrency:
    - Pure helper with no shared mutable state.
    """
    normalized_candidates = deduplicate_preserving_order([
        extract_email_address(str(email_candidate or "")) or str(email_candidate or "").strip().lower()
        for email_candidate in email_candidates
        if str(email_candidate or "").strip()
    ])
    usable_candidates = [
        normalized_candidate
        for normalized_candidate in normalized_candidates
        if not is_unusable_outbound_email(normalized_candidate)
    ]
    if not usable_candidates:
        return ""
    best_candidate_email = max(
        usable_candidates,
        key=lambda candidate_email: score_contact_email_for_action(candidate_email, action_type),
    )
    if score_contact_email_for_action(best_candidate_email, action_type) < 0:
        return ""
    return best_candidate_email


def resolve_message_sender_email(message_record: GmailMessage) -> str:
    """
    Pick the best replyable sender address from a parsed Gmail message.

    Inputs:
    - message_record: Parsed Gmail message containing `reply_to` and `from` headers.

    Outputs:
    - Best-effort replyable sender email, or a fallback normalized `From` address when no
      better candidate exists.

    Edge cases:
    - `Reply-To` is preferred over `From` when it is usable.
    - Falls back to `From` so ATS/company inference still has a deterministic sender value.

    Atomicity / concurrency:
    - Pure helper with no shared mutable state.
    """
    reply_to_header = str(message_record.get("reply_to", "") or "")
    from_header = str(message_record.get("from", "") or "")
    return (
        choose_preferred_replyable_email(reply_to_header, from_header)
        or extract_email_address(from_header)
    )


def resolve_message_reply_to_email(message_record: GmailMessage) -> str:
    """
    Extract a usable `Reply-To` email from a parsed Gmail message when available.

    Inputs:
    - message_record: Parsed Gmail message containing a `reply_to` header.

    Outputs:
    - Normalized replyable `Reply-To` email, or an empty string when unavailable or unusable.

    Edge cases:
    - Generic no-reply and notification-style mailboxes are excluded.

    Atomicity / concurrency:
    - Pure helper with no shared mutable state.
    """
    return choose_preferred_replyable_email(str(message_record.get("reply_to", "") or ""))


def resolve_outbound_target_email(app: ApplicationRecord, action_type: str) -> str:
    """
    Choose the most appropriate recipient address for an outbound action.

    Inputs:
    - app: Application row containing recruiter/contact/ATS email fields.
    - action_type: Pending action type such as `follow_up`, `withdraw`, or
      `deletion_request`.

    Outputs:
    - Best-effort recipient email address, or an empty string when no usable candidate exists.

    Edge cases:
    - `contact_email` and `recruiter_email` are preferred over ATS addresses.
    - Follow-ups never fall back to unusable no-reply mailboxes.
    - Withdrawals and deletion requests may still fall back to a usable ATS mailbox when no
      person-specific contact is available.

    Atomicity / concurrency:
    - Pure helper with no shared mutable state.
    """
    preferred_contacts = [
        extract_email_address(str(app.get("contact_email", "") or "")),
        extract_email_address(str(app.get("recruiter_email", "") or "")),
    ]
    resolved_preferred_contact = choose_best_contact_email_for_action(preferred_contacts, action_type)
    if resolved_preferred_contact:
        return resolved_preferred_contact

    ats_email = extract_email_address(str(app.get("ats_email", "") or ""))
    resolved_ats_contact = choose_best_contact_email_for_action([ats_email], action_type)
    if resolved_ats_contact:
        return resolved_ats_contact
    return ""


def infer_company_hint_from_sender_domain(email_domain: str) -> str:
    """
    Infer an employer hint from ATS notification subdomains when one is embedded in the domain.

    Inputs:
    - email_domain: Lowercased sender domain.

    Outputs:
    - Best-effort company hint such as `tikalk`, or an empty string when no signal exists.

    Edge cases:
    - Only vendor notification suffixes with a prefixed tenant label are used.
    - Multi-label prefixes use the last label because it is usually the tenant/company slug.

    Atomicity / concurrency:
    - Pure helper with no shared mutable state.
    """
    normalized_email_domain = str(email_domain or "").strip().lower()
    if not normalized_email_domain:
        return ""

    for notification_domain_suffix in ATS_NOTIFICATION_DOMAIN_SUFFIXES:
        suffix_with_separator = f".{notification_domain_suffix}"
        if not normalized_email_domain.endswith(suffix_with_separator):
            continue

        company_slug = normalized_email_domain.removesuffix(suffix_with_separator).split(".")[-1]
        normalized_company_slug = re.sub(r"[^a-z0-9]+", " ", company_slug).strip()
        return " ".join(normalized_company_slug.split())

    return ""


def is_unknown_role_title(role_title: str) -> bool:
    """
    Determine whether a role title is effectively missing or unusable for matching.

    Inputs:
    - role_title: Extracted or stored role title.

    Outputs:
    - True when the role is blank or a placeholder such as `Unknown`.

    Edge cases:
    - Whitespace and punctuation are ignored before comparison.

    Atomicity / concurrency:
    - Pure helper with no shared mutable state.
    """
    normalized_role_title = normalize_matching_text(role_title)
    return normalized_role_title in {"", "unknown", "not specified", "n a", "na", "none", "null"}


def is_stage_like_role_title(role_title: str) -> bool:
    """
    Detect whether a stored role looks like a process stage instead of a real job title.

    Inputs:
    - role_title: Extracted or stored role title text.

    Outputs:
    - True when the role resembles interview-stage wording such as `tech interview`.

    Edge cases:
    - Blank or placeholder roles are treated as stage-like because they are not useful
      discriminators for separate applications.

    Atomicity / concurrency:
    - Pure helper with no shared mutable state.
    """
    normalized_role_title = normalize_matching_text(role_title)
    if is_unknown_role_title(normalized_role_title):
        return True

    stage_markers = (
        "interview",
        "screen",
        "screening",
        "assessment",
        "take home",
        "takehome",
        "challenge",
        "onsite",
        "on site",
        "phone call",
        "phone screen",
        "recruiter call",
        "hiring manager",
        "final round",
        "ceo",
        "founder",
    )
    return any(stage_marker in normalized_role_title for stage_marker in stage_markers)


def is_contact_like_role_title(role_title: str) -> bool:
    """
    Detect recruiter/contact job titles that should not outrank the candidate's actual applied role.

    Inputs:
    - role_title: Extracted or stored role title text.

    Outputs:
    - True when the title looks like an HR / recruiting contact title such as `Talent Partner`.

    Edge cases:
    - Blank or placeholder values return False here because those are already handled by
      `is_stage_like_role_title`.

    Atomicity / concurrency:
    - Pure helper with no shared mutable state.
    """
    normalized_role_title = normalize_matching_text(role_title)
    if not normalized_role_title or is_unknown_role_title(normalized_role_title):
        return False

    contact_title_markers = (
        "recruiter",
        "recruiting",
        "talent acquisition",
        "talent partner",
        "talent lead",
        "talent specialist",
        "talent sourcer",
        "sourcer",
        "people operations",
        "people ops",
        "people partner",
        "human resources",
        "hr business partner",
        "hrbp",
    )
    return any(contact_title_marker in normalized_role_title for contact_title_marker in contact_title_markers)


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


def are_dates_within_merge_window(first_date: str, second_date: str) -> bool:
    """
    Check whether two application dates are close enough for role-less merge fallback logic.

    Inputs:
    - first_date: First ISO date string.
    - second_date: Second ISO date string.

    Outputs:
    - True when both dates are present and within the configured merge window.

    Edge cases:
    - Missing or invalid dates return False so the caller does not over-merge.

    Atomicity / concurrency:
    - Pure helper with no shared mutable state.
    """
    first_datetime = parse_iso_date(first_date)
    second_datetime = parse_iso_date(second_date)
    if first_datetime is None or second_datetime is None:
        return False
    return abs((first_datetime - second_datetime).days) <= APPLICATION_ROLELESS_MERGE_WINDOW_DAYS


def parse_int_with_default(raw_value: object, default: int = 0) -> int:
    """
    Parse an integer-like value while falling back safely on malformed inputs.

    Inputs:
    - raw_value: Value expected to represent an integer count.
    - default: Fallback integer used when parsing fails.

    Outputs:
    - Parsed integer value, or the provided default.

    Edge cases:
    - Blank strings, None, and non-numeric content return the fallback.

    Atomicity / concurrency:
    - Pure helper with no shared mutable state.
    """
    try:
        return int(str(raw_value or default))
    except (TypeError, ValueError):
        return default


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


def get_application_merge_strength(
    first_app: ApplicationRecord,
    second_app: ApplicationRecord,
) -> int:
    """
    Score how confidently two application records should be treated as one process.

    Inputs:
    - first_app: First application-like dictionary.
    - second_app: Second application-like dictionary.

    Outputs:
    - Integer score where 0 means do not merge and larger numbers mean stronger evidence.

    Edge cases:
    - Exact company and role matches win even when statuses differ substantially.
    - Same-company records with stage-like or missing roles only merge when their dates are
      close enough to avoid collapsing clearly separate applications.

    Atomicity / concurrency:
    - Pure helper with no shared mutable state.
    """
    first_company = normalize_matching_text(first_app.get("company", ""))
    second_company = normalize_matching_text(second_app.get("company", ""))
    if not first_company or first_company != second_company:
        return 0

    first_role = normalize_matching_text(first_app.get("role", ""))
    second_role = normalize_matching_text(second_app.get("role", ""))
    if first_role and second_role and first_role == second_role:
        return 3

    first_role_is_low_signal = (
        is_stage_like_role_title(first_app.get("role", ""))
        or is_contact_like_role_title(first_app.get("role", ""))
    )
    second_role_is_low_signal = (
        is_stage_like_role_title(second_app.get("role", ""))
        or is_contact_like_role_title(second_app.get("role", ""))
    )
    if is_low_confidence_company_name(first_company) and (
        first_role_is_low_signal or second_role_is_low_signal
    ):
        return 0
    dates_are_close = are_dates_within_merge_window(
        first_app.get("applied_date", "") or first_app.get("last_activity_date", ""),
        second_app.get("applied_date", "") or second_app.get("last_activity_date", ""),
    )
    if not dates_are_close:
        return 0

    if first_role_is_low_signal and second_role_is_low_signal:
        return 2
    if first_role_is_low_signal != second_role_is_low_signal:
        return 1
    return 0


def choose_preferred_role_title(*role_titles: str) -> str:
    """
    Pick the most useful role title from one or more competing application records.

    Inputs:
    - role_titles: Candidate role strings ordered by preference.

    Outputs:
    - The most descriptive non-stage-like role when available, otherwise the first non-empty
      role value.

    Edge cases:
    - Returns an empty string when every candidate is blank.

    Atomicity / concurrency:
    - Pure helper with no shared mutable state.
    """
    preferred_roles = [
        role_title
        for role_title in role_titles
        if role_title
        and not is_stage_like_role_title(role_title)
        and not is_contact_like_role_title(role_title)
        and not is_low_quality_role_title(role_title)
    ]
    if preferred_roles:
        return max(preferred_roles, key=lambda role_title: len(role_title.strip()))

    non_empty_roles = [role_title for role_title in role_titles if str(role_title or "").strip()]
    if non_empty_roles:
        return max(non_empty_roles, key=lambda role_title: len(role_title.strip()))
    return ""


def extract_company_name_from_email_message(email_message: GmailMessage) -> str:
    """
    Extract a likely employer name from one email message using deterministic text patterns.

    Inputs:
    - email_message: Parsed Gmail message dictionary containing subject, snippet, and body text.

    Outputs:
    - Cleaned company name string, or an empty string when no reliable company phrase is found.

    Edge cases:
    - Prioritizes ATS-style confirmation subjects such as `Taboola! We've received your application`.
    - Rejects overly long fragments and phrases that still look like full sentences.

    Atomicity / concurrency:
    - Pure helper with no shared mutable state.
    """
    message_text_sources = [
        str(email_message.get("subject", "") or ""),
        str(email_message.get("snippet", "") or ""),
        str(email_message.get("body", "") or ""),
    ]
    company_patterns = (
        re.compile(
            r"^\s*(?P<company>[A-Za-z0-9][A-Za-z0-9&.,'()\/\- ]{1,80}?)!\s+we(?:'|’)ve received your application\b",
            re.IGNORECASE,
        ),
        re.compile(
            r"\bthank you for applying to (?P<company>[A-Za-z0-9][A-Za-z0-9&.,'()\/\- ]{1,80}?)\b",
            re.IGNORECASE,
        ),
        re.compile(
            r"\byour application to (?P<company>[A-Za-z0-9][A-Za-z0-9&.,'()\/\- ]{1,80}?)\b",
            re.IGNORECASE,
        ),
        re.compile(
            r"\bapplication (?:for|to) (?P<company>[A-Za-z0-9][A-Za-z0-9&.,'()\/\- ]{1,80}?)(?:\s+(?:has been|was|is)\b|[.!,:;]|\n)",
            re.IGNORECASE,
        ),
    )

    for message_text in message_text_sources:
        normalized_message_text = html.unescape(str(message_text or ""))
        for company_pattern in company_patterns:
            for match in company_pattern.finditer(normalized_message_text):
                company_name = re.sub(r"\s+", " ", match.group("company")).strip(" \t\r\n\"'`.,:;[]{}!()-")
                if not company_name:
                    continue
                if len(company_name) > 80:
                    continue
                if "@" in company_name or "http" in company_name.lower():
                    continue
                normalized_company_name = normalize_matching_text(company_name)
                if not normalized_company_name:
                    continue
                if is_low_confidence_company_name(company_name):
                    continue
                sentence_like_markers = (
                    "we have",
                    "thank you",
                    "received your application",
                    "application has been",
                    "job alert",
                )
                if any(marker in normalized_company_name for marker in sentence_like_markers):
                    continue
                return company_name
    return ""


def is_low_quality_role_title(role_title: str) -> bool:
    """
    Detect role values that look like email subjects or other non-title boilerplate.

    Inputs:
    - role_title: Candidate role string persisted on an application row.

    Outputs:
    - True when the value is weak enough that Gmail backfill should try to replace it.

    Edge cases:
    - Treats stage-like and unknown roles as low quality.
    - ATS confirmation subjects such as `Taboola! We've received your application...` are
      flagged even though they are non-empty.

    Atomicity / concurrency:
    - Pure helper with no shared mutable state.
    """
    normalized_role_title = normalize_matching_text(role_title)
    if is_unknown_role_title(normalized_role_title):
        return True
    if is_stage_like_role_title(role_title) or is_contact_like_role_title(role_title):
        return True
    low_quality_markers = (
        "we ve received your application",
        "thank you for applying",
        "application received",
        "your application for",
        "received your application for our",
    )
    return any(marker in normalized_role_title for marker in low_quality_markers)


def normalize_role_candidate_text(role_candidate_text: str) -> str:
    """
    Normalize an extracted role-title fragment before it is compared or persisted.

    Inputs:
    - role_candidate_text: Raw title fragment captured from an email subject, snippet, or body.

    Outputs:
    - Cleaned role title string, or an empty string when the fragment is not usable.

    Edge cases:
    - HTML entities and repeated whitespace are collapsed.
    - Boilerplate punctuation around the title is removed.
    - Overly long, email-like, or URL-like fragments are rejected.

    Atomicity / concurrency:
    - Pure helper with no shared mutable state.
    """
    normalized_role_candidate_text = html.unescape(str(role_candidate_text or ""))
    normalized_role_candidate_text = re.sub(r"\s+", " ", normalized_role_candidate_text).strip()
    normalized_role_candidate_text = normalized_role_candidate_text.strip(" \t\r\n\"'`.,:;[]{}")
    normalized_role_candidate_text = re.sub(
        r"\s+(?:position|role)\s*$",
        "",
        normalized_role_candidate_text,
        flags=re.IGNORECASE,
    ).strip()
    if not normalized_role_candidate_text:
        return ""
    if len(normalized_role_candidate_text) > 120:
        return ""
    if len(normalized_role_candidate_text.split()) > 8:
        return ""
    if "@" in normalized_role_candidate_text or "http" in normalized_role_candidate_text.lower():
        return ""
    lowercased_role_candidate_text = normalized_role_candidate_text.lower()
    sentence_like_markers = (
        "we have",
        "thank you",
        "joining",
        "team",
        "received your cv",
        "received your resume",
        "application has been",
    )
    if any(sentence_like_marker in lowercased_role_candidate_text for sentence_like_marker in sentence_like_markers):
        return ""
    if (
        is_stage_like_role_title(normalized_role_candidate_text)
        or is_contact_like_role_title(normalized_role_candidate_text)
    ):
        return ""
    generic_role_placeholders = {
        "the",
        "open",
        "position",
        "positions",
        "role",
        "job",
        "our position",
        "our positions",
        "open role",
        "open position",
        "open positions",
    }
    if lowercased_role_candidate_text in generic_role_placeholders:
        return ""
    return normalized_role_candidate_text


def extract_role_titles_from_email_message(email_message: GmailMessage) -> list[str]:
    """
    Extract likely job titles from one email message using deterministic text patterns.

    Inputs:
    - email_message: Parsed Gmail message dictionary containing subject, snippet, and body text.

    Outputs:
    - Ordered list of candidate role titles found in the message.

    Edge cases:
    - Searches multiple text sources because ATS emails often place the role in only one field.
    - Returns an empty list when no reliable title pattern is present.

    Atomicity / concurrency:
    - Pure helper with no shared mutable state.
    """
    message_text_sources = [
        str(email_message.get("subject", "") or ""),
        str(email_message.get("snippet", "") or ""),
        str(email_message.get("body", "") or ""),
    ]
    role_patterns = (
        re.compile(r"\binterest in (?:the )?(?P<role>[^.\n]{3,120}?) position\b", re.IGNORECASE),
        re.compile(r"\bapplication for (?:the )?(?P<role>[^.\n]{3,120}?) position\b", re.IGNORECASE),
        re.compile(r"\bapplication for (?:the )?(?P<role>[^.\n]{3,120}?) role\b", re.IGNORECASE),
        re.compile(r"\breferred (?:you )?(?:to us )?for (?:the )?(?P<role>[^.\n]{3,120}?) job at\b", re.IGNORECASE),
        re.compile(r"\bfor (?:the )?(?P<role>[^.\n]{3,120}?) job at\b", re.IGNORECASE),
        re.compile(r"\bfor considering .*? for (?:the )?(?P<role>[^.\n]{3,120}?) position\b", re.IGNORECASE),
        re.compile(r"\bregarding (?:my |your )?application for (?:the )?(?P<role>[^.\n]{3,120}?)(?: position| role| at)\b", re.IGNORECASE),
    )

    extracted_role_titles: list[str] = []
    for message_text in message_text_sources:
        normalized_message_text = html.unescape(message_text)
        for role_pattern in role_patterns:
            for match in role_pattern.finditer(normalized_message_text):
                normalized_role_title = normalize_role_candidate_text(match.group("role"))
                if normalized_role_title and normalized_role_title not in extracted_role_titles:
                    extracted_role_titles.append(normalized_role_title)
    return extracted_role_titles


def extract_status_from_email_message(email_message: GmailMessage) -> Optional[str]:
    """
    Extract a strong application status signal from one email message using text patterns.

    Inputs:
    - email_message: Parsed Gmail message dictionary containing subject, snippet, and body text.

    Outputs:
    - Canonical status string when a reliable phrase is found, otherwise None.

    Edge cases:
    - Prefers terminal outcomes such as `Rejected` over weaker progress states when multiple
      signals appear in the same message.
    - Uses subject, snippet, and body because ATS notifications often place the key status in
      only one of those fields.

    Atomicity / concurrency:
    - Pure helper with no shared mutable state.
    """
    message_text_sources = get_message_text_sources(email_message)
    normalized_message_text = "\n".join(html.unescape(text) for text in message_text_sources)
    detected_languages = {
        detect_text_language(text)
        for text in message_text_sources
        if str(text or "").strip()
    }

    english_status_patterns: tuple[tuple[str, tuple[str, ...]], ...] = (
        (
            "Rejected",
            (
                r"\bnot moving forward\b",
                r"\bwill not be moving forward\b",
                r"\bno longer under consideration\b",
                r"\bmove forward with other candidates\b",
                r"\bmoving forward with other candidates\b",
                r"\bdecided to move forward with other candidates\b",
                r"\bdecided to pursue other candidates\b",
                r"\bunfortunately\b.{0,120}\b(?:not selected|not moving forward|other candidates|no longer under consideration)\b",
                r"\bwe regret to inform you\b",
                r"\bposition has been filled\b",
                r"\bclosed the position\b",
                r"\bclosed the role\b",
                r"\boffer has been extended\b",
            ),
        ),
        (
            "Offer",
            (
                r"\bexcited to offer you\b",
                r"\bpleased to offer you\b",
                r"\boffer letter\b",
            ),
        ),
        (
            "Interview",
            (
                r"\binterview\b",
                r"\binterviews?\b",
                r"\bhiring manager\b",
                r"\bonsite\b",
            ),
        ),
        (
            "Assessment",
            (
                r"\bassessment\b",
                r"\bcoding challenge\b",
                r"\btake[- ]?home\b",
            ),
        ),
        (
            "Screening",
            (
                r"\bphone screen\b",
                r"\brecruiter call\b",
                r"\bscreening call\b",
            ),
        ),
    )

    german_status_patterns: tuple[tuple[str, tuple[str, ...]], ...] = (
        (
            "Rejected",
            (
                r"\bleider\b.{0,160}\b(?:absage|abgelehnt|nicht ber[üu]cksichtigen|nicht weiter|anderen kandidaten|anderen bewerber)\b",
                r"\bwir bedauern\b",
                r"\bwir m[üu]ssen ihnen leider mitteilen\b",
                r"\bhaben uns f[üu]r andere(?:n)? bewerber\b",
                r"\bnicht in die engere auswahl\b",
                r"\bposition(?: ist)? bereits besetzt\b",
                r"\bstelle(?: ist)? bereits besetzt\b",
                r"\babsage\b",
                r"\babgelehnt\b",
            ),
        ),
        (
            "Offer",
            (
                r"\bfreuen uns ihnen\b.{0,80}\banzubieten\b",
                r"\bangebot\b",
            ),
        ),
        (
            "Interview",
            (
                r"\binterview\b",
                r"\bvorstellungsgespr[äa]ch\b",
                r"\bgespr[äa]chstermin\b",
            ),
        ),
        (
            "Assessment",
            (
                r"\baufgabe\b",
                r"\btestaufgabe\b",
                r"\bcoding[- ]?challenge\b",
            ),
        ),
        (
            "Screening",
            (
                r"\btelefoninterview\b",
                r"\btelefonat\b",
                r"\berstgespr[äa]ch\b",
            ),
        ),
    )

    status_patterns = list(english_status_patterns)
    if "de" in detected_languages:
        status_patterns = list(german_status_patterns) + status_patterns

    for status_name, patterns in status_patterns:
        if any(re.search(pattern, normalized_message_text, flags=re.IGNORECASE | re.DOTALL) for pattern in patterns):
            return status_name
    return None


def extract_sent_date_from_email_message(email_message: GmailMessage) -> str:
    """
    Parse the Gmail message's sent date into `YYYY-MM-DD` form when possible.

    Inputs:
    - email_message: Parsed Gmail message dictionary containing `date` and optional `timestamp`.

    Outputs:
    - ISO-like date string, or an empty string when no reliable sent date is available.

    Edge cases:
    - Falls back from RFC 2822 `Date` headers to the parsed Unix timestamp when present.
    - Invalid or missing date fields return an empty string.

    Atomicity / concurrency:
    - Pure helper with no shared mutable state.
    """
    raw_date_header = str(email_message.get("date", "") or "").strip()
    if raw_date_header:
        try:
            return parsedate_to_datetime(raw_date_header).date().isoformat()
        except (TypeError, ValueError, IndexError, OverflowError):
            pass

    raw_timestamp = email_message.get("timestamp")
    try:
        if raw_timestamp:
            return datetime.fromtimestamp(int(raw_timestamp), tz=timezone.utc).date().isoformat()
    except (TypeError, ValueError, OSError, OverflowError):
        pass
    return ""


def extract_scheduled_interview_date_from_email_message(email_message: GmailMessage) -> str:
    """
    Extract a scheduled interview/meeting date from invite-style email content.

    Inputs:
    - email_message: Parsed Gmail message dictionary containing subject, snippet, and body text.

    Outputs:
    - Scheduled meeting date in `YYYY-MM-DD` form, or an empty string when no strong invite-style
      date is found.

    Edge cases:
    - Only activates on interview/meeting/invite-style wording to avoid treating arbitrary dates
      in outreach emails as activity dates.
    - Returns the first strong ISO-like meeting date found in the message text.

    Atomicity / concurrency:
    - Pure helper with no shared mutable state.
    """
    message_text_sources = get_message_text_sources(email_message)
    normalized_message_text = "\n".join(html.unescape(text) for text in message_text_sources)
    normalized_message_text_lower = normalized_message_text.lower()

    invite_markers = (
        "interview invite",
        "meeting invite",
        "meeting reminder",
        "time confirmed",
        "thank you for choosing a time",
        "date/time",
        "google meet",
        "video call",
        "choose a time",
        "teams.microsoft.com",
        "microsoft teams",
        "calendar",
        "organizer",
        "rescheduled",
        "updated invitation",
        "interview - ",
    )
    if not any(invite_marker in normalized_message_text_lower for invite_marker in invite_markers):
        return ""

    iso_date_match = re.search(
        r"\b(20\d{2}-\d{2}-\d{2})\b(?:\s+\d{1,2}:\d{2}(?:\s*-\s*\d{1,2}:\d{2})?)?",
        normalized_message_text,
        flags=re.IGNORECASE,
    )
    if iso_date_match:
        return iso_date_match.group(1)

    sent_date = parse_iso_date(extract_sent_date_from_email_message(email_message))
    anchor_date = sent_date.date() if sent_date is not None else datetime.now(timezone.utc).date()
    month_name_pattern = (
        r"\b(?:mon|monday|tue|tues|tuesday|wed|wednesday|thu|thurs|thursday|fri|friday|"
        r"sat|saturday|sun|sunday),?\s+"
        r"(?P<month>jan|january|feb|february|mar|march|apr|april|may|jun|june|jul|july|"
        r"aug|august|sep|sept|september|oct|october|nov|november|dec|december)"
        r"\s+"
        r"(?P<day>\d{1,2})"
        r"(?:st|nd|rd|th)?"
        r"(?:,\s*(?P<year>20\d{2}))?"
        r"\b"
    )
    compact_month_name_pattern = (
        r"\b(?P<month>jan|january|feb|february|mar|march|apr|april|may|jun|june|jul|july|"
        r"aug|august|sep|sept|september|oct|october|nov|november|dec|december)"
        r"\s+"
        r"(?P<day>\d{1,2})"
        r"(?:st|nd|rd|th)?"
        r"(?:,\s*(?P<year>20\d{2}))?"
        r"\b"
    )

    for pattern in (month_name_pattern, compact_month_name_pattern):
        for date_match in re.finditer(pattern, normalized_message_text, flags=re.IGNORECASE):
            raw_month = str(date_match.group("month") or "").strip().lower()
            month_lookup = raw_month[:3]
            month_number = {
                str(name).lower(): index
                for index, name in enumerate(calendar.month_abbr)
                if index
            }.get(month_lookup)
            if month_number is None:
                continue
            try:
                candidate_year = int(date_match.group("year") or anchor_date.year)
                candidate_date = datetime(
                    year=candidate_year,
                    month=month_number,
                    day=int(date_match.group("day")),
                ).date()
            except (TypeError, ValueError):
                continue
            if not date_match.group("year") and candidate_date < anchor_date - timedelta(days=30):
                try:
                    candidate_date = candidate_date.replace(year=candidate_date.year + 1)
                except ValueError:
                    continue
            return candidate_date.isoformat()
    return ""


def has_interview_schedule_signal(email_message: GmailMessage) -> bool:
    """
    Detect whether a message reflects interview scheduling state such as an invite or update.

    Inputs:
    - email_message: Parsed Gmail message dictionary containing free-form text and metadata.

    Outputs:
    - True when the message looks like a scheduling event, reschedule, or cancellation.

    Edge cases:
    - A parseable scheduled interview date counts as a scheduling signal even without explicit
      invite keywords.
    - Generic interview chatter without invite/update language returns False.

    Atomicity / concurrency:
    - Pure helper with no shared mutable state.
    """
    if extract_scheduled_interview_date_from_email_message(email_message):
        return True

    normalized_message_text = "\n".join(
        html.unescape(text) for text in get_message_text_sources(email_message)
    ).lower()
    schedule_markers = (
        "meeting invite",
        "interview invite",
        "meeting reminder",
        "updated invitation",
        "rescheduled",
        "reschedule",
        "canceled",
        "cancelled",
        "calendar",
        "organizer",
        "microsoft teams",
        "teams.microsoft.com",
        "google meet",
        "zoom.us",
        "webex",
    )
    return any(marker in normalized_message_text for marker in schedule_markers)


def choose_latest_activity_date(*date_values: str) -> str:
    """
    Pick the latest valid ISO-like date from one or more candidates.

    Inputs:
    - date_values: Candidate `YYYY-MM-DD` strings.

    Outputs:
    - Latest valid date string, or an empty string when no candidates are valid.

    Edge cases:
    - Invalid date strings are ignored.

    Atomicity / concurrency:
    - Pure helper with no shared mutable state.
    """
    valid_date_values = [
        normalized_date
        for raw_date in date_values
        for normalized_date in [str(raw_date or "").strip()]
        if parse_iso_date(normalized_date) is not None
    ]
    if not valid_date_values:
        return ""
    return max(valid_date_values)


def has_strong_application_signal(email_message: GmailMessage) -> bool:
    """
    Determine whether an email clearly refers to a submitted application or active pipeline.

    Inputs:
    - email_message: Parsed Gmail message dictionary containing subject, snippet, and body text.

    Outputs:
    - True when the message includes strong evidence that the candidate already applied or is
      actively in a hiring process.

    Edge cases:
    - ATS senders count as strong signals only when paired with application-like wording.
    - Generic recruiter outreach that merely mentions a role or company does not count.

    Atomicity / concurrency:
    - Pure helper with no shared mutable state.
    """
    message_text_sources = [
        str(email_message.get("subject", "") or ""),
        str(email_message.get("snippet", "") or ""),
        str(email_message.get("body", "") or ""),
    ]
    normalized_message_text = "\n".join(html.unescape(text) for text in message_text_sources)

    strong_signal_patterns = (
        r"\bwe(?:'|’)ve received your application\b",
        r"\bthank you for applying\b",
        r"\byour application (?:for|to)\b",
        r"\bapplication received\b",
        r"\bapplication has been\b",
        r"\bunder consideration\b",
        r"\bmove forward with your application\b",
        r"\bnext step(?:s)? in the interview process\b",
        r"\binterview(?: process)?\b",
        r"\bschedule (?:an )?interview\b",
        r"\bphone screen\b",
        r"\bassessment\b",
        r"\bcoding challenge\b",
        r"\btake[- ]?home\b",
        r"\bnot moving forward\b",
        r"\bwill not be moving forward\b",
        r"\bother candidates\b",
        r"\bno longer under consideration\b",
        r"\bdecided to move forward with other candidates\b",
        r"\bdecided to pursue other candidates\b",
        r"\bwe regret to inform you\b",
        r"\bposition has been filled\b",
        r"\bclosed the position\b",
        r"\bclosed the role\b",
        r"\boffer letter\b",
        r"\bexcited to offer you\b",
        r"\boffer has been extended\b",
    )
    return any(
        re.search(pattern, normalized_message_text, flags=re.IGNORECASE | re.DOTALL)
        for pattern in strong_signal_patterns
    )


def is_unsolicited_recruiter_outreach_email(email_message: GmailMessage) -> bool:
    """
    Detect inbound recruiter sourcing or job-offer outreach that should not create applications.

    Inputs:
    - email_message: Parsed Gmail message dictionary containing subject, snippet, body, and sender.

    Outputs:
    - True when the email looks like unsolicited outreach rather than a response to an existing
      application.

    Edge cases:
    - Messages with strong application signals are never treated as unsolicited outreach even if
      they contain generic recruiting phrases.
    - ATS senders are excluded because automated application workflow messages are handled
      separately and often use different wording.

    Atomicity / concurrency:
    - Pure helper with no shared mutable state.
    """
    sender_email_address = resolve_message_sender_email(email_message)
    sender_email_domain = extract_email_domain(sender_email_address)
    if is_ats_sender_domain(sender_email_domain):
        return False

    if has_strong_application_signal(email_message):
        return False

    message_text_sources = [
        str(email_message.get("subject", "") or ""),
        str(email_message.get("snippet", "") or ""),
        str(email_message.get("body", "") or ""),
    ]
    normalized_message_text = "\n".join(html.unescape(text) for text in message_text_sources)

    outreach_patterns = (
        r"\breaching out\b",
        r"\breach out\b",
        r"\bcame across your profile\b",
        r"\bfound your profile\b",
        r"\bsaw your profile\b",
        r"\byour linkedin profile\b",
        r"\bon linkedin\b",
        r"\bopen to new opportunities\b",
        r"\bwould you be interested\b",
        r"\bif you(?:'|’)re interested\b",
        r"\bpotential fit\b",
        r"\bgreat fit\b",
        r"\bgood fit\b",
        r"\bjob opportunity\b",
        r"\bexciting opportunity\b",
        r"\bshare an opportunity\b",
        r"\bdiscuss an opportunity\b",
        r"\bmy client .*?\blooking for\b",
        r"\bclient of mine\b",
        r"\bhiring for\b",
        r"\bwe(?:'|’)re hiring\b",
        r"\bwe are hiring\b",
        r"\blooking for (?:a|an)\b",
        r"\blooking for\b.{0,80}\b(?:engineer|developer|manager|designer|analyst|contractor|freelance)\b",
        r"\blooking to hire\b",
        r"\bseeking (?:a|an)\b",
        r"\bopening on (?:my|our) team\b",
        r"\brole might interest you\b",
        r"\bthought you might be interested\b",
        r"\bplease apply\b",
        r"\bencourage you to apply\b",
        r"\bif you wish to be considered\b",
        r"\bto be considered for the position\b",
        r"\bjob description and application link\b",
        r"\bapplication link\b",
        r"\bsend (?:me )?(?:your|an) up[- ]to[- ]date cv\b",
        r"\be-?mail an up[- ]to[- ]date cv\b",
        r"\bsend .*?\bresume\b",
        r"\bpass this advert\b",
        r"\bcontract\b",
        r"\b12[- ]month\b",
        r"\bstart asap\b",
        r"\bremote role\b",
    )
    outreach_match_count = sum(
        1
        for pattern in outreach_patterns
        if re.search(pattern, normalized_message_text, flags=re.IGNORECASE | re.DOTALL)
    )
    return outreach_match_count >= 1


def build_related_gmail_search_terms(application_row: ApplicationRecord) -> list[str]:
    """
    Build Gmail search clauses that can surface related emails for one application.

    Inputs:
    - application_row: Application record containing company and contact metadata.

    Outputs:
    - Ordered Gmail search clauses such as company-name terms and sender-address filters.

    Edge cases:
    - Blank values are ignored.
    - Sender domains are included because ATS and recruiter messages may come from multiple
      addresses under one employer domain.

    Atomicity / concurrency:
    - Pure helper with no shared mutable state.
    """
    search_terms: list[str] = []
    company_name = str(application_row.get("company", "") or "").strip()
    if company_name and not is_low_confidence_company_name(company_name):
        search_terms.append(f"\"{company_name}\"")

    sender_addresses: list[str] = []
    for field_name in ("recruiter_email", "ats_email", "contact_email"):
        sender_address = extract_email_address(str(application_row.get(field_name, "") or ""))
        if not sender_address:
            continue
        if (
            field_name != "ats_email"
            and score_contact_email_for_action(sender_address, "follow_up") < 0
        ):
            continue
        sender_addresses.append(sender_address)
    sender_addresses = deduplicate_preserving_order(sender_addresses)
    for sender_address in sender_addresses:
        if is_generic_ats_sender_address(sender_address):
            continue
        search_terms.append(f"from:{sender_address}")

    sender_domains = deduplicate_preserving_order([
        extract_email_domain(sender_address)
        for sender_address in sender_addresses
        if extract_email_domain(sender_address)
    ])
    for sender_domain in sender_domains:
        if is_ats_sender_domain(sender_domain):
            continue
        search_terms.append(f"from:{sender_domain}")

    return deduplicate_preserving_order(search_terms)


def has_only_calendar_message_ids(application_row: ApplicationRecord) -> bool:
    """
    Check whether an application row is linked only to Google Calendar invite messages.

    Inputs:
    - application_row: Application record containing stored RFC Message-IDs.

    Outputs:
    - True when every stored RFC Message-ID looks like a Google Calendar invite ID.

    Edge cases:
    - Rows with no RFC Message-IDs return False so callers can use other heuristics.

    Atomicity / concurrency:
    - Pure helper with no shared mutable state.
    """
    internet_message_ids = cast(list[str], safe_json_loads(
        application_row.get("internet_message_ids") or "[]",
        f"internet_message_ids for appl_id {application_row.get('appl_id', 'unknown')}",
        [],
    ))
    normalized_internet_message_ids = [
        str(internet_message_id or "").strip().lower()
        for internet_message_id in internet_message_ids
        if str(internet_message_id or "").strip()
    ]
    if not normalized_internet_message_ids:
        return False
    return all(
        normalized_internet_message_id.startswith("<calendar-")
        and normalized_internet_message_id.endswith("@google.com>")
        for normalized_internet_message_id in normalized_internet_message_ids
    )


def find_existing_application_id_by_thread_id(
    existing_apps_by_id: dict[str, ApplicationRecord],
    message_thread_id: str,
) -> str:
    """
    Find the existing application row that already owns a Gmail thread.

    Inputs:
    - existing_apps_by_id: Existing application rows keyed by `appl_id`.
    - message_thread_id: Gmail internal thread ID from an inbound message.

    Outputs:
    - Matching `appl_id`, or an empty string when no stored row contains the thread.

    Edge cases:
    - Blank thread IDs and malformed stored JSON return no match.

    Atomicity / concurrency:
    - Pure helper with no shared mutable state.
    """
    normalized_message_thread_id = str(message_thread_id or "").strip()
    if not normalized_message_thread_id:
        return ""

    for existing_appl_id, existing_app in existing_apps_by_id.items():
        stored_thread_ids = cast(list[str], safe_json_loads(
            existing_app.get("thread_ids") or "[]",
            f"thread_ids for appl_id {existing_appl_id}",
            [],
        ))
        if normalized_message_thread_id in stored_thread_ids:
            return existing_appl_id
    return ""


def merge_application_records(
    primary_app: ApplicationRecord,
    secondary_app: ApplicationRecord,
) -> ApplicationRecord:
    """
    Merge two application records into a single canonical row.

    Inputs:
    - primary_app: Existing application row that keeps its `appl_id`.
    - secondary_app: Additional record that should collapse into the primary row.

    Outputs:
    - Merged application dictionary containing the richest combined data.

    Edge cases:
    - Status only ever moves forward according to STATUS_RANK.
    - `email_ids`, `thread_ids`, and `internet_message_ids` are deduplicated even when
      malformed JSON appears in one record.

    Atomicity / concurrency:
    - Pure in-memory transformation with no shared mutable state.
    """
    merged_app = dict(primary_app)

    preferred_role_title = choose_preferred_role_title(
        primary_app.get("role", ""),
        secondary_app.get("role", ""),
    )
    if preferred_role_title:
        merged_app["role"] = preferred_role_title

    for field_name in ("company", "source", "recruiter_name", "recruiter_email", "ats_email", "contact_email", "draft_id", "linkedin_contact"):
        if not merged_app.get(field_name) and secondary_app.get(field_name):
            merged_app[field_name] = secondary_app[field_name]

    primary_status = primary_app.get("status", "Applied")
    secondary_status = secondary_app.get("status", "Applied")
    if STATUS_RANK.get(secondary_status, 0) > STATUS_RANK.get(primary_status, 0):
        merged_app["status"] = secondary_status

    candidate_applied_dates = [
        value
        for value in (primary_app.get("applied_date", ""), secondary_app.get("applied_date", ""))
        if parse_iso_date(value) is not None
    ]
    if candidate_applied_dates:
        merged_app["applied_date"] = min(candidate_applied_dates)

    candidate_last_activity_dates = [
        value
        for value in (
            primary_app.get("last_activity_date", ""),
            secondary_app.get("last_activity_date", ""),
        )
        if parse_iso_date(value) is not None
    ]
    if candidate_last_activity_dates:
        merged_app["last_activity_date"] = max(candidate_last_activity_dates)

    primary_follow_up_count = parse_int_with_default(primary_app.get("follow_up_count", "0"))
    secondary_follow_up_count = parse_int_with_default(secondary_app.get("follow_up_count", "0"))
    merged_app["follow_up_count"] = str(max(primary_follow_up_count, secondary_follow_up_count))

    for field_name in ("follow_up_sent_date", "withdrawal_sent_date", "deferred_until"):
        candidate_dates = [
            value
            for value in (primary_app.get(field_name, ""), secondary_app.get(field_name, ""))
            if parse_iso_date(value) is not None
        ]
        if candidate_dates:
            merged_app[field_name] = max(candidate_dates)

    if is_truthy_sheet_value(primary_app.get("withdraw_in_next_digest")) or is_truthy_sheet_value(
        secondary_app.get("withdraw_in_next_digest")
    ):
        merged_app["withdraw_in_next_digest"] = "TRUE"

    primary_notes = str(primary_app.get("notes", "") or "").strip()
    secondary_notes = str(secondary_app.get("notes", "") or "").strip()
    if primary_notes and secondary_notes and primary_notes != secondary_notes:
        merged_app["notes"] = f"{primary_notes}\n{secondary_notes}"
    elif not primary_notes and secondary_notes:
        merged_app["notes"] = secondary_notes

    merged_email_ids = deduplicate_preserving_order(
        cast(list[str], safe_json_loads(
            primary_app.get("email_ids") or "[]",
            f"email_ids for appl_id {primary_app.get('appl_id', 'unknown')}",
            [],
        ))
        + cast(list[str], safe_json_loads(
            secondary_app.get("email_ids") or "[]",
            f"email_ids for appl_id {secondary_app.get('appl_id', 'unknown')}",
            [],
        ))
    )
    merged_app["email_ids"] = json.dumps(merged_email_ids)

    merged_thread_ids = deduplicate_preserving_order(
        cast(list[str], safe_json_loads(
            primary_app.get("thread_ids") or "[]",
            f"thread_ids for appl_id {primary_app.get('appl_id', 'unknown')}",
            [],
        ))
        + cast(list[str], safe_json_loads(
            secondary_app.get("thread_ids") or "[]",
            f"thread_ids for appl_id {secondary_app.get('appl_id', 'unknown')}",
            [],
        ))
    )
    merged_app["thread_ids"] = json.dumps(merged_thread_ids)

    merged_internet_message_ids = deduplicate_preserving_order(
        cast(list[str], safe_json_loads(
            primary_app.get("internet_message_ids") or "[]",
            f"internet_message_ids for appl_id {primary_app.get('appl_id', 'unknown')}",
            [],
        ))
        + cast(list[str], safe_json_loads(
            secondary_app.get("internet_message_ids") or "[]",
            f"internet_message_ids for appl_id {secondary_app.get('appl_id', 'unknown')}",
            [],
        ))
    )
    merged_app["internet_message_ids"] = json.dumps(merged_internet_message_ids)

    return merged_app


def build_gmail_message_url(message_id: str) -> str:
    """
    Build a Gmail web URL that opens one specific message or thread by internal Gmail ID.

    Inputs:
    - message_id: Gmail internal message or thread ID.

    Outputs:
    - Gmail web URL string, or an empty string when the ID is blank.

    Edge cases:
    - Blank IDs return an empty string so callers can fall back to other options.

    Atomicity / concurrency:
    - Pure helper with no shared mutable state.
    """
    normalized_message_id = str(message_id).strip()
    if not normalized_message_id:
        return ""
    return f"{GMAIL_WEB_BASE_URL}/#all/{normalized_message_id}"


def build_gmail_search_url_from_query(gmail_search_query: str) -> str:
    """
    Build a Gmail web URL that opens a free-form Gmail search query.

    Inputs:
    - gmail_search_query: Raw Gmail search string.

    Outputs:
    - Gmail web URL string, or an empty string when the query is blank.

    Edge cases:
    - Whitespace-only queries return an empty string.

    Atomicity / concurrency:
    - Pure helper with no shared mutable state.
    """
    normalized_gmail_search_query = str(gmail_search_query or "").strip()
    if not normalized_gmail_search_query:
        return ""
    return f"{GMAIL_WEB_BASE_URL}/#search/{quote(normalized_gmail_search_query, safe='')}"


def build_gmail_search_url(internet_message_ids: list[str]) -> str:
    """
    Build a Gmail search URL that shows the provided RFC Message-IDs together.

    Inputs:
    - internet_message_ids: RFC `Message-ID` header values, usually including angle brackets.

    Outputs:
    - Gmail search URL string, or an empty string when there are no usable IDs.

    Edge cases:
    - Blank and duplicate IDs are removed before building the query.
    - The query uses Gmail's `rfc822msgid:` operator so one click can show multiple messages.

    Atomicity / concurrency:
    - Pure helper with no shared mutable state.
    """
    normalized_internet_message_ids = deduplicate_preserving_order([
        str(internet_message_id).strip()
        for internet_message_id in internet_message_ids
        if str(internet_message_id).strip()
    ])
    if not normalized_internet_message_ids:
        return ""

    search_terms = [
        f"rfc822msgid:{internet_message_id}"
        for internet_message_id in normalized_internet_message_ids
    ]
    gmail_search_query = " OR ".join(search_terms)
    return build_gmail_search_url_from_query(gmail_search_query)


def build_gmail_review_url(application_row: ApplicationRecord) -> str:
    """
    Build the best available Gmail review URL for one application row.

    Inputs:
    - application_row: Application record that may contain `internet_message_ids`,
      `email_ids`, and `thread_ids`.

    Outputs:
    - Preferred Gmail URL for manual review, or an empty string when no email identity data
      is available.

    Edge cases:
    - Prefers a Gmail search URL that shows all RFC Message-IDs together.
    - Falls back to the first Gmail message ID when legacy rows do not have RFC Message-IDs.
    - Falls back again to the first thread ID for partial data recovered from Gmail.

    Atomicity / concurrency:
    - Pure helper with no shared mutable state.
    """
    internet_message_ids = cast(list[str], safe_json_loads(
        application_row.get("internet_message_ids") or "[]",
        f"internet_message_ids for appl_id {application_row.get('appl_id', 'unknown')}",
        [],
    ))
    exact_message_search_terms = [
        f"rfc822msgid:{internet_message_id}"
        for internet_message_id in deduplicate_preserving_order([
            str(internet_message_id).strip()
            for internet_message_id in internet_message_ids
            if str(internet_message_id).strip()
        ])
    ]
    related_search_terms = build_related_gmail_search_terms(application_row)
    should_expand_review_query = (
        is_stage_like_role_title(str(application_row.get("role", "") or ""))
        or has_only_calendar_message_ids(application_row)
    )
    if should_expand_review_query:
        expanded_gmail_search_url = build_gmail_search_url_from_query(
            " OR ".join(deduplicate_preserving_order(exact_message_search_terms + related_search_terms))
        )
        if expanded_gmail_search_url:
            return expanded_gmail_search_url

    gmail_search_url = build_gmail_search_url(internet_message_ids)
    if gmail_search_url:
        return gmail_search_url

    email_ids = cast(list[str], safe_json_loads(
        application_row.get("email_ids") or "[]",
        f"email_ids for appl_id {application_row.get('appl_id', 'unknown')}",
        [],
    ))
    if email_ids:
        return build_gmail_message_url(email_ids[0])

    thread_ids = cast(list[str], safe_json_loads(
        application_row.get("thread_ids") or "[]",
        f"thread_ids for appl_id {application_row.get('appl_id', 'unknown')}",
        [],
    ))
    if thread_ids:
        return build_gmail_message_url(thread_ids[0])

    return ""


class ProcessedMessageStateStore:
    """
    Persist a small local ledger of successfully handled Gmail message IDs.

    Inputs:
    - state_path: File path used to read and write the local state snapshot.

    Outputs:
    - Helper object that loads, prunes, and updates processed-message state on disk.

    Edge cases:
    - Missing files initialize to an empty state.
    - Malformed files log a warning and fall back to an empty state.

    Atomicity / concurrency:
    - Writes the full state file in one local filesystem operation per save. Concurrent
      writers are not coordinated and can still race.
    """

    def __init__(self, state_path: Path):
        self.state_path = state_path

    def load(self, retention_days: int) -> ProcessedMessageStateSnapshot:
        """
        Load and prune the local processed-message state file.

        Inputs:
        - retention_days: Maximum age of state entries to keep.

        Outputs:
        - ProcessedMessageStateSnapshot containing only retained message IDs.

        Edge cases:
        - Missing state files return an empty snapshot.
        - Invalid timestamps are discarded during pruning.

        Atomicity / concurrency:
        - Reads the file once and may rewrite a pruned snapshot back to disk.
        """
        if not self.state_path.exists():
            return ProcessedMessageStateSnapshot(processed_at_by_message_id={})

        try:
            raw_state = self.state_path.read_text(encoding="utf-8")
        except OSError as error:
            raise TrackerError(
                f"Failed to read processed Gmail state file at {self.state_path}: {error}"
            ) from error

        parsed_state = cast(object, safe_json_loads(
            raw_state,
            f"processed Gmail state file {self.state_path}",
            {},
        ))
        processed_at_by_message_id = self._extract_processed_entries(parsed_state)
        pruned_processed_at_by_message_id = self._prune_entries(
            processed_at_by_message_id,
            retention_days=retention_days,
        )

        if pruned_processed_at_by_message_id != processed_at_by_message_id:
            self.save(pruned_processed_at_by_message_id)

        return ProcessedMessageStateSnapshot(
            processed_at_by_message_id=pruned_processed_at_by_message_id
        )

    def save(self, processed_at_by_message_id: dict[str, str]) -> None:
        """
        Persist the local processed-message snapshot to disk.

        Inputs:
        - processed_at_by_message_id: Mapping of Gmail message IDs to ISO timestamps.

        Outputs:
        - None. The state file on disk is replaced with the provided snapshot.

        Edge cases:
        - Parent directories are created automatically when missing.

        Atomicity / concurrency:
        - Writes the file contents in one local filesystem operation with no inter-process
          locking.
        """
        payload: JsonObject = {
            "schema_version": 1,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "processed_at_by_message_id": processed_at_by_message_id,
        }
        try:
            self.state_path.parent.mkdir(parents=True, exist_ok=True)
            self.state_path.write_text(
                json.dumps(payload, indent=2, sort_keys=True),
                encoding="utf-8",
            )
        except OSError as error:
            raise TrackerError(
                f"Failed to write processed Gmail state file at {self.state_path}: {error}"
            ) from error

    def delete(self) -> None:
        """
        Delete the local processed-message state file.

        Inputs:
        - None.

        Outputs:
        - None. The on-disk state file is removed when it exists.

        Edge cases:
        - Missing files are ignored because there is nothing to reset.

        Atomicity / concurrency:
        - Performs at most one filesystem delete. Concurrent writers are not coordinated and
          may recreate the file after deletion.
        """
        if not self.state_path.exists():
            return

        try:
            self.state_path.unlink()
        except OSError as error:
            raise TrackerError(
                f"Failed to delete processed Gmail state file at {self.state_path}: {error}"
            ) from error

    def record_processed_message_ids(
        self,
        existing_snapshot: ProcessedMessageStateSnapshot,
        message_ids: list[str],
        retention_days: int,
    ) -> ProcessedMessageStateSnapshot:
        """
        Add successfully handled Gmail message IDs to the local backstop state file.

        Inputs:
        - existing_snapshot: Current in-memory state snapshot.
        - message_ids: Gmail message IDs that were fully handled by the sync flow.
        - retention_days: Maximum age of state entries to keep after the update.

        Outputs:
        - Updated ProcessedMessageStateSnapshot after the new IDs are saved.

        Edge cases:
        - Duplicate or blank message IDs are ignored.

        Atomicity / concurrency:
        - Reads the provided snapshot in memory, writes one replacement file, and returns the
          resulting in-memory snapshot.
        """
        if not message_ids:
            return existing_snapshot

        processed_at_by_message_id = dict(existing_snapshot.processed_at_by_message_id)
        processed_at_timestamp = datetime.now(timezone.utc).isoformat()
        for message_id in message_ids:
            normalized_message_id = message_id.strip()
            if not normalized_message_id:
                continue
            processed_at_by_message_id[normalized_message_id] = processed_at_timestamp

        pruned_processed_at_by_message_id = self._prune_entries(
            processed_at_by_message_id,
            retention_days=retention_days,
        )
        self.save(pruned_processed_at_by_message_id)
        return ProcessedMessageStateSnapshot(
            processed_at_by_message_id=pruned_processed_at_by_message_id
        )

    def remove_processed_message_ids(
        self,
        existing_snapshot: ProcessedMessageStateSnapshot,
        message_ids: list[str],
        retention_days: int,
    ) -> ProcessedMessageStateSnapshot:
        """
        Remove specific Gmail message IDs from the local processed-message ledger.

        Inputs:
        - existing_snapshot: Current in-memory processed-message snapshot.
        - message_ids: Gmail message IDs that should be forgotten so they can be reprocessed.
        - retention_days: Maximum entry age to keep after the removal is applied.

        Outputs:
        - Updated ProcessedMessageStateSnapshot after the filtered mapping is saved.

        Edge cases:
        - Blank or unknown message IDs are ignored.
        - The state file is still pruned and normalized even when no IDs match.

        Atomicity / concurrency:
        - Reads the provided snapshot in memory, writes one replacement file, and returns the
          resulting in-memory snapshot.
        """
        normalized_message_ids_to_remove = {
            str(message_id).strip()
            for message_id in message_ids
            if str(message_id).strip()
        }
        filtered_processed_at_by_message_id = {
            message_id: processed_at
            for message_id, processed_at in existing_snapshot.processed_at_by_message_id.items()
            if message_id not in normalized_message_ids_to_remove
        }
        pruned_processed_at_by_message_id = self._prune_entries(
            filtered_processed_at_by_message_id,
            retention_days=retention_days,
        )
        self.save(pruned_processed_at_by_message_id)
        return ProcessedMessageStateSnapshot(
            processed_at_by_message_id=pruned_processed_at_by_message_id
        )

    @staticmethod
    def _extract_processed_entries(parsed_state: object) -> dict[str, str]:
        """
        Normalize raw JSON state into a message-ID-to-timestamp mapping.

        Inputs:
        - parsed_state: Parsed JSON value loaded from disk.

        Outputs:
        - Mapping of Gmail message IDs to ISO timestamps.

        Edge cases:
        - Legacy list-based state files are accepted and upgraded in memory.
        - Invalid shapes produce an empty mapping.

        Atomicity / concurrency:
        - Pure helper with no shared mutable state.
        """
        if isinstance(parsed_state, dict):
            parsed_state_dict = cast(dict[str, object], parsed_state)
            raw_mapping = parsed_state_dict.get("processed_at_by_message_id", {})
            if isinstance(raw_mapping, dict):
                normalized_mapping: dict[str, str] = {}
                for raw_message_id, raw_timestamp in cast(
                    dict[object, object],
                    raw_mapping,
                ).items():
                    message_id = str(raw_message_id).strip()
                    timestamp = str(raw_timestamp).strip()
                    if not message_id or not timestamp:
                        continue
                    normalized_mapping[message_id] = timestamp
                return normalized_mapping

        if isinstance(parsed_state, list):
            now_timestamp = datetime.now(timezone.utc).isoformat()
            return {
                str(raw_message_id).strip(): now_timestamp
                for raw_message_id in cast(list[object], parsed_state)
                if str(raw_message_id).strip()
            }

        return {}

    @staticmethod
    def _prune_entries(
        processed_at_by_message_id: dict[str, str],
        retention_days: int,
    ) -> dict[str, str]:
        """
        Drop local state entries that are older than the configured retention window.

        Inputs:
        - processed_at_by_message_id: Mapping of Gmail message IDs to ISO timestamps.
        - retention_days: Maximum entry age to retain.

        Outputs:
        - New mapping containing only retained entries.

        Edge cases:
        - Invalid timestamps are discarded.
        - Retention is bounded to at least one day.

        Atomicity / concurrency:
        - Pure helper with no shared mutable state.
        """
        retention_days = max(1, retention_days)
        retention_cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
        retained_entries: dict[str, str] = {}
        for message_id, processed_at_text in processed_at_by_message_id.items():
            try:
                processed_at = datetime.fromisoformat(processed_at_text)
            except ValueError:
                continue

            if processed_at.tzinfo is None:
                processed_at = processed_at.replace(tzinfo=timezone.utc)

            if processed_at >= retention_cutoff:
                retained_entries[message_id] = processed_at.isoformat()
        return retained_entries

# ── Config ─────────────────────────────────────────────────────────────────────

def load_config() -> ConfigDict:
    """
    Load the YAML configuration file used by the tracker.

    Inputs:
    - None. Reads from the configured config.yaml path.

    Outputs:
    - Parsed configuration dictionary.

    Edge cases:
    - Missing file, invalid YAML, empty files, or missing required top-level sections
      raise a TrackerError with actionable context.

    Atomicity / concurrency:
    - Reads config from disk once with no shared mutable state.
    """
    if not CONFIG_PATH.exists():
        raise TrackerError(f"config.yaml not found at {CONFIG_PATH}")

    try:
        with open(CONFIG_PATH, encoding="utf-8") as config_file:
            config = yaml.safe_load(config_file)
    except yaml.YAMLError as error:
        raise TrackerError(f"config.yaml is not valid YAML: {error}") from error
    except OSError as error:
        raise TrackerError(f"Could not read config.yaml at {CONFIG_PATH}: {error}") from error

    if not isinstance(config, dict):
        raise TrackerError("config.yaml must contain a YAML object at the top level")

    config = cast(ConfigDict, config)
    required_sections = ["gmail", "google_sheets", "gemini", "thresholds", "user"]
    missing_sections = [section for section in required_sections if section not in config]
    if missing_sections:
        raise TrackerError(
            "config.yaml is missing required sections: " + ", ".join(missing_sections)
        )

    return config


# ── Template renderer ──────────────────────────────────────────────────────────

def render_template(
    template_path: Path,
    variables: dict[str, str],
) -> Optional[tuple[str, str]]:
    """
    Load a template file and substitute {{var}} placeholders.
    Handles {{#if var}}...{{/if}} blocks (block removed if var is empty).
    Returns (subject, body) tuple parsed from the first 'Subject:' line,
    or None if the file doesn't exist.
    """
    if not template_path.exists():
        return None

    try:
        text = template_path.read_text(encoding="utf-8")
    except OSError as error:
        raise TrackerError(f"Could not read template at {template_path}: {error}") from error

    # Process {{#if var}}...{{else}}...{{/if}} blocks
    def process_if(match: Match[str]) -> str:
        key = match.group(1).strip()
        truthy_content = match.group(2)
        falsey_content = match.group(3) or ""
        return truthy_content if variables.get(key) else falsey_content

    text = re.sub(r"\{\{#if\s+(\w+)\}\}(.*?)(?:\{\{else\}\}(.*?))?\{\{/if\}\}", process_if,
                  text, flags=re.DOTALL)

    # Substitute {{var}} placeholders
    for key, value in variables.items():
        text = text.replace(f"{{{{{key}}}}}", str(value or ""))

    # Split subject line from body
    lines   = text.strip().splitlines()
    subject = ""
    body_start = 0
    if lines and lines[0].lower().startswith("subject:"):
        subject    = lines[0][len("subject:"):].strip()
        body_start = 1
        # Skip blank line after subject if present
        if len(lines) > 1 and lines[1].strip() == "":
            body_start = 2

    body = "\n".join(lines[body_start:]).strip()
    return subject, body




class GmailClient:
    def __init__(self, config: ConfigDict):
        self.config = config
        self._creds = self._authenticate()
        try:
            self.service: Any = google_api_build("gmail", "v1", credentials=self._creds)
        except Exception as error:
            raise TrackerError(f"Failed to initialize Gmail client: {error}") from error

    def _authenticate(self) -> GoogleAuthCredentials:
        """
        Authenticate against Gmail and persist refreshed OAuth credentials.

        Inputs:
        - None. Uses token and Google client credential files from disk.

        Outputs:
        - Valid Google OAuth credentials object.

        Edge cases:
        - Missing credential files, invalid token files, failed refreshes, and failed
          browser-based OAuth flows raise TrackerError with explicit context.

        Atomicity / concurrency:
        - Refreshes and rewrites the token file as one local operation per process.
        """
        creds = None
        if TOKEN_PATH.exists():
            try:
                creds = CredentialsLoader.from_authorized_user_file(str(TOKEN_PATH), SCOPES)
            except Exception as error:
                raise TrackerError(f"Could not load Google token from {TOKEN_PATH}: {error}") from error
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds = _refresh_or_reauthorize_google_credentials(creds, token_path_for_errors=TOKEN_PATH)
            else:
                creds = _run_google_oauth_flow()
            _persist_google_token(creds)
        return creds

    def get_emails(
        self,
        query: str,
        since_days: int = 90,
        seen_message_ids: Optional[set[str]] = None,
    ) -> GmailFetchResult:
        """
        Scan Gmail for all messages matching the configured query and parse only unseen ones.

        Inputs:
        - query: Gmail search query configured by the user.
        - since_days: Rolling lookback window applied via Gmail search syntax.
        - seen_message_ids: Persistent Gmail message IDs already known to the tracker from
          local state or previously synced application records.

        Outputs:
        - GmailFetchResult containing the total match count and the parsed unseen emails.

        Edge cases:
        - When seen_message_ids is empty, the method parses every matching email in the window.
        - When all matching IDs are already known, no message bodies are fetched.
        - Gmail pagination continues past 500 results so older unseen emails are not starved.

        Atomicity / concurrency:
        - Performs sequential Gmail API reads with no shared mutable state beyond the client.
        """
        after = (datetime.now() - timedelta(days=since_days)).strftime("%Y/%m/%d")
        full_query = f"({query}) after:{after}"
        known_message_ids = seen_message_ids or set()
        unseen_message_ids: list[str] = []
        total_matching_email_count = 0
        page_token = None
        while True:
            kwargs: dict[str, str | int] = {"userId": "me", "q": full_query, "maxResults": 500}
            if page_token:
                kwargs["pageToken"] = page_token
            try:
                res = self.service.users().messages().list(**kwargs).execute()
            except Exception as error:
                raise TrackerError(f"Failed to list Gmail messages for query '{full_query}': {error}") from error
            page_messages = res.get("messages", [])
            total_matching_email_count += len(page_messages)
            for message in page_messages:
                message_id = message.get("id")
                if message_id and message_id not in known_message_ids:
                    unseen_message_ids.append(message_id)
            page_token = res.get("nextPageToken")
            if not page_token:
                break
        emails: list[GmailMessage] = []
        total_unseen_message_count = len(unseen_message_ids)
        for message_index, message_id in enumerate(unseen_message_ids, start=1):
            console.print(
                f"  Fetching new Gmail messages: [cyan]{message_index}[/cyan] / "
                f"[cyan]{total_unseen_message_count}[/cyan]",
                end="\r",
            )
            try:
                emails.append(self._parse_message(message_id))
            except Exception as error:
                console.print(f"[yellow]  ↳ Warning: could not parse {message_id}: {error}[/yellow]")
        return GmailFetchResult(
            total_matching_email_count=total_matching_email_count,
            unseen_emails=emails,
        )

    def search_messages(
        self,
        query: str,
        since_days: int,
        max_results: int,
    ) -> list[GmailMessage]:
        """
        Search Gmail and return parsed messages for a bounded query window.

        Inputs:
        - query: Gmail search query that identifies related messages.
        - since_days: Rolling lookback window added to the Gmail query.
        - max_results: Maximum number of matching messages to parse.

        Outputs:
        - Parsed Gmail messages ordered by Gmail's search results.

        Edge cases:
        - Blank queries and non-positive limits return an empty list.
        - Duplicate Gmail message IDs are deduplicated before fetching full message bodies.

        Atomicity / concurrency:
        - Performs bounded Gmail API reads with no local side effects.
        """
        normalized_query = str(query or "").strip()
        if not normalized_query or max_results <= 0:
            return []

        after = (datetime.now() - timedelta(days=since_days)).strftime("%Y/%m/%d")
        full_query = f"({normalized_query}) after:{after}"
        matched_message_ids: list[str] = []
        page_token = None
        while len(matched_message_ids) < max_results:
            remaining_result_count = max_results - len(matched_message_ids)
            kwargs: dict[str, str | int] = {
                "userId": "me",
                "q": full_query,
                "maxResults": min(remaining_result_count, 100),
            }
            if page_token:
                kwargs["pageToken"] = page_token
            try:
                response = self.service.users().messages().list(**kwargs).execute()
            except Exception as error:
                raise TrackerError(
                    f"Failed to search Gmail messages for query '{full_query}': {error}"
                ) from error

            matched_message_ids.extend([
                str(message.get("id", "")).strip()
                for message in response.get("messages", [])
                if str(message.get("id", "")).strip()
            ])
            page_token = response.get("nextPageToken")
            if not page_token:
                break

        return self.get_messages_by_ids(
            deduplicate_preserving_order(matched_message_ids)[:max_results]
        )

    def _parse_message(self, msg_id: str) -> GmailMessage:
        try:
            msg = cast(
                JsonObject,
                self.service.users().messages().get(
                    userId="me",
                    id=msg_id,
                    format="full",
                ).execute(),
            )
        except Exception as error:
            raise TrackerError(f"Failed to fetch Gmail message {msg_id}: {error}") from error
        payload = cast(JsonObject, msg.get("payload", {}))
        headers = cast(list[dict[str, Any]], payload.get("headers", []))
        hdrs = {
            str(header.get("name", "")).lower(): str(header.get("value", ""))
            for header in headers
        }
        body_text = self._extract_body(payload)[:3000]
        attachment_text = self._extract_attachment_text(msg_id, payload)[:6000]
        detected_languages = deduplicate_preserving_order([
            detect_text_language(text_source)
            for text_source in (
                hdrs.get("subject", ""),
                msg.get("snippet", ""),
                body_text,
                attachment_text,
            )
            if str(text_source or "").strip() and detect_text_language(str(text_source or "").strip()) != "unknown"
        ])
        return {
            "id": msg_id,
            "thread_id": msg.get("threadId"),
            "internet_message_id": hdrs.get("message-id", ""),
            "from":     hdrs.get("from", ""),
            "reply_to": hdrs.get("reply-to", ""),
            "to":       hdrs.get("to", ""),
            "subject":  hdrs.get("subject", ""),
            "date":     hdrs.get("date", ""),
            "timestamp": int(msg.get("internalDate", 0)) // 1000,
            "snippet":  msg.get("snippet", ""),
            "body":     body_text,
            "attachment_text": attachment_text,
            "detected_languages": detected_languages,
        }

    def get_messages_by_ids(self, message_ids: list[str]) -> list[GmailMessage]:
        """
        Fetch and parse a specific set of Gmail messages by internal message ID.

        Inputs:
        - message_ids: Gmail internal message IDs to load.

        Outputs:
        - Parsed Gmail message dictionaries in the provided order with duplicates removed.

        Edge cases:
        - Blank and duplicate message IDs are ignored.

        Atomicity / concurrency:
        - Performs independent Gmail message fetches with no local side effects.
        """
        normalized_message_ids = deduplicate_preserving_order([
            str(message_id or "").strip()
            for message_id in message_ids
            if str(message_id or "").strip()
        ])
        parsed_messages: list[GmailMessage] = []
        for message_id in normalized_message_ids:
            parsed_messages.append(self._parse_message(message_id))
        return parsed_messages

    def get_messages_by_thread_ids(self, thread_ids: list[str]) -> list[GmailMessage]:
        """
        Fetch and parse every Gmail message contained in one or more thread IDs.

        Inputs:
        - thread_ids: Gmail internal thread IDs.

        Outputs:
        - Parsed Gmail messages for every message found in the referenced threads.

        Edge cases:
        - Blank and duplicate thread IDs are ignored.
        - Missing or inaccessible threads are tolerated with a warning so one bad thread does
          not block the broader resync flow.

        Atomicity / concurrency:
        - Performs read-only Gmail API calls with no local side effects.
        """
        normalized_thread_ids = deduplicate_preserving_order([
            str(thread_id or "").strip()
            for thread_id in thread_ids
            if str(thread_id or "").strip()
        ])
        if not normalized_thread_ids:
            return []

        thread_message_ids: list[str] = []
        for thread_id in normalized_thread_ids:
            try:
                thread_payload = cast(
                    JsonObject,
                    self.service.users().threads().get(
                        userId="me",
                        id=thread_id,
                        format="minimal",
                    ).execute(),
                )
            except Exception as error:
                console.print(
                    f"[yellow]  ↳ Warning: could not fetch Gmail thread {thread_id}: {error}[/yellow]"
                )
                continue

            thread_message_ids.extend([
                str(message.get("id", "")).strip()
                for message in cast(list[JsonObject], thread_payload.get("messages", []) or [])
                if str(message.get("id", "")).strip()
            ])

        return self.get_messages_by_ids(deduplicate_preserving_order(thread_message_ids))

    def _extract_body(self, payload: JsonObject) -> str:
        mime = str(payload.get("mimeType", ""))
        if mime == "text/plain":
            body = cast(JsonObject, payload.get("body", {}))
            data = str(body.get("data", ""))
            if data:
                return base64.urlsafe_b64decode(data + "==").decode("utf-8", errors="replace")
        if mime == "text/html":
            body = cast(JsonObject, payload.get("body", {}))
            data = str(body.get("data", ""))
            if data:
                html = base64.urlsafe_b64decode(data + "==").decode("utf-8", errors="replace")
                return re.sub(r"<[^>]+>", " ", html)
        parts = cast(list[JsonObject], payload.get("parts", []))
        for part in parts:
            result = self._extract_body(part)
            if result:
                return result
        return ""

    def _iter_payload_parts(self, payload: JsonObject) -> list[JsonObject]:
        """
        Flatten a Gmail payload tree into a list of parts, including the root payload.
        """
        parts: list[JsonObject] = [payload]
        for part in cast(list[JsonObject], payload.get("parts", []) or []):
            parts.extend(self._iter_payload_parts(part))
        return parts

    def _fetch_attachment_bytes(self, message_id: str, attachment_id: str) -> bytes:
        """
        Fetch raw Gmail attachment bytes for one attachment ID.
        """
        attachment_payload = cast(
            JsonObject,
            self.service.users().messages().attachments().get(
                userId="me",
                messageId=message_id,
                id=attachment_id,
            ).execute(),
        )
        raw_data = str(attachment_payload.get("data", "") or "")
        if not raw_data:
            return b""
        return base64.urlsafe_b64decode(raw_data + "==")

    def _log_pdf_attachment_issue(
        self,
        message_id: str,
        filename: str,
        error: Exception,
    ) -> None:
        """
        Emit a searchable warning when a PDF attachment cannot be read.
        """
        attachment_label = filename or "(unnamed attachment)"
        console.print(
            f"[yellow]  PDF attachment issue: message_id={message_id} "
            f"filename={attachment_label} error={error}[/yellow]"
        )

    def _extract_attachment_text(self, message_id: str, payload: JsonObject) -> str:
        """
        Extract readable text from Gmail attachments, prioritizing PDFs.
        """
        extracted_text_chunks: list[str] = []
        for part in self._iter_payload_parts(payload):
            mime = str(part.get("mimeType", "") or "").lower()
            raw_filename = str(part.get("filename", "") or "").strip()
            filename = raw_filename.lower()
            body = cast(JsonObject, part.get("body", {}) or {})
            attachment_id = str(body.get("attachmentId", "") or "").strip()
            inline_data = str(body.get("data", "") or "")
            is_pdf_attachment = mime == "application/pdf" or filename.endswith(".pdf")

            if mime not in {"application/pdf", "text/plain", "text/html"} and not filename.endswith(".pdf"):
                continue

            attachment_bytes = b""
            if attachment_id:
                try:
                    attachment_bytes = self._fetch_attachment_bytes(message_id, attachment_id)
                except Exception as error:
                    if is_pdf_attachment:
                        self._log_pdf_attachment_issue(message_id, raw_filename, error)
                    continue
            elif inline_data:
                try:
                    attachment_bytes = base64.urlsafe_b64decode(inline_data + "==")
                except Exception as error:
                    if is_pdf_attachment:
                        self._log_pdf_attachment_issue(message_id, raw_filename, error)
                    continue
            if not attachment_bytes:
                continue

            try:
                if is_pdf_attachment:
                    pdf_reader = PdfReader(io.BytesIO(attachment_bytes))
                    pdf_text = "\n".join(
                        (page.extract_text() or "").strip()
                        for page in pdf_reader.pages[:10]
                    ).strip()
                    if pdf_text:
                        extracted_text_chunks.append(pdf_text)
                elif mime == "text/plain":
                    extracted_text_chunks.append(attachment_bytes.decode("utf-8", errors="replace"))
                elif mime == "text/html":
                    html_text = attachment_bytes.decode("utf-8", errors="replace")
                    extracted_text_chunks.append(re.sub(r"<[^>]+>", " ", html_text))
            except Exception as error:
                if is_pdf_attachment:
                    self._log_pdf_attachment_issue(message_id, raw_filename, error)
                continue

        normalized_attachment_text = "\n\n".join(
            text_chunk.strip()
            for text_chunk in extracted_text_chunks
            if text_chunk.strip()
        )
        return normalized_attachment_text

    def create_draft(self, to: str, subject: str, body: str,
                     from_addr: str = "") -> str:
        if re.search(r"<[a-zA-Z][^>]*>", body):
            # Body contains HTML — send as multipart/alternative so the footer renders
            msg = MIMEMultipart("alternative")
            plain_text = re.sub(r"<[^>]+>", "", body).strip()
            html_body = re.sub(
                r"(.*?)(<[a-zA-Z])",
                lambda m: m.group(1).replace("\n\n", "<br><br>").replace("\n", "<br>") + m.group(2),
                body,
                count=1,
                flags=re.DOTALL,
            )
            msg.attach(MIMEText(plain_text, "plain"))
            msg.attach(MIMEText(html_body, "html"))
        else:
            msg = MIMEText(body, "plain")
        if str(to or "").strip():
            msg["To"] = to
        msg["Subject"] = subject
        if from_addr:
            msg["From"] = from_addr
        raw   = base64.urlsafe_b64encode(msg.as_bytes()).decode()
        retry_delay_seconds = DEFAULT_GOOGLE_API_TRANSIENT_INITIAL_RETRY_DELAY_SECONDS
        for attempt_number in range(DEFAULT_GOOGLE_API_TRANSIENT_RETRIES + 1):
            try:
                draft = cast(JsonObject, self.service.users().drafts().create(
                    userId="me", body={"message": {"raw": raw}}
                ).execute())
                break
            except Exception as error:
                if (
                    is_transient_transport_disconnect(str(error))
                    and attempt_number < DEFAULT_GOOGLE_API_TRANSIENT_RETRIES
                ):
                    console.print(
                        f"[yellow]  Gmail draft retry: {to or '(manual draft)'} failed due to "
                        f"transport disconnect before response. Waiting {retry_delay_seconds:.1f}s "
                        f"before retry {attempt_number + 2} of "
                        f"{DEFAULT_GOOGLE_API_TRANSIENT_RETRIES + 1}.[/yellow]"
                    )
                    time.sleep(retry_delay_seconds)
                    retry_delay_seconds *= max(1.0, DEFAULT_GOOGLE_API_TRANSIENT_BACKOFF_MULTIPLIER)
                    continue
                raise TrackerError(f"Failed to create Gmail draft for {to}: {error}") from error
        return draft["id"]

    def send_draft(self, draft_id: str) -> None:
        try:
            self.service.users().drafts().send(userId="me", body={"id": draft_id}).execute()
        except Exception as error:
            raise TrackerError(f"Failed to send Gmail draft {draft_id}: {error}") from error

    def delete_draft(self, draft_id: str) -> None:
        """
        Delete a Gmail draft by ID.

        Inputs:
        - draft_id: Gmail draft identifier to delete.

        Outputs:
        - None. The draft is removed from the authenticated Gmail account.

        Edge cases:
        - Blank draft IDs are ignored.

        Atomicity / concurrency:
        - Performs one Gmail drafts delete request with no local side effects.
        """
        normalized_draft_id = str(draft_id).strip()
        if not normalized_draft_id:
            return

        try:
            self.service.users().drafts().delete(userId="me", id=normalized_draft_id).execute()
        except HttpError as error:
            error_response = cast(object, error.resp)
            if getattr(error_response, "status", None) == 404:
                return
            raise TrackerError(f"Failed to delete Gmail draft {normalized_draft_id}: {error}") from error
        except Exception as error:
            raise TrackerError(f"Failed to delete Gmail draft {normalized_draft_id}: {error}") from error

    def get_profile_email(self) -> str:
        try:
            profile = cast(JsonObject, self.service.users().getProfile(userId="me").execute())
            return str(profile.get("emailAddress", ""))
        except Exception as error:
            raise TrackerError(f"Failed to read Gmail profile email: {error}") from error

    def ensure_processing_labels(
        self,
        processing_label_config: GmailProcessingLabelConfig,
    ) -> dict[str, str]:
        """
        Ensure the configured Gmail processing labels exist and return their label IDs.

        Inputs:
        - processing_label_config: Root and stage labels required by the tracker.

        Outputs:
        - Mapping of Gmail label names to Gmail label IDs.

        Edge cases:
        - Missing custom labels are created automatically.
        - Existing labels are reused without modification.

        Atomicity / concurrency:
        - Reads the Gmail label list once and may perform one create request per missing label.
        """
        existing_label_ids_by_name = self._get_user_label_ids_by_name()
        ensured_label_ids_by_name: dict[str, str] = {}
        for label_name in processing_label_config.all_label_names():
            if label_name in existing_label_ids_by_name:
                ensured_label_ids_by_name[label_name] = existing_label_ids_by_name[label_name]
                continue

            try:
                created_label_id = self._create_or_get_user_label(
                    label_name=label_name,
                    existing_label_ids_by_name=existing_label_ids_by_name,
                )
            except GmailLabelConflictError as error:
                if label_name != processing_label_config.root_label_name:
                    raise
                console.print(
                    f"  [yellow]Skipping optional Gmail root label '{label_name}' because "
                    f"Gmail reported a name conflict: {error}[/yellow]"
                )
                continue
            existing_label_ids_by_name[label_name] = created_label_id
            ensured_label_ids_by_name[label_name] = created_label_id

        return ensured_label_ids_by_name

    def apply_labels_to_messages(self, message_ids: list[str], label_ids: list[str]) -> None:
        """
        Apply the provided Gmail labels to a batch of Gmail messages.

        Inputs:
        - message_ids: Gmail message IDs that should receive the labels.
        - label_ids: Gmail label IDs to add to every provided message.

        Outputs:
        - None. Gmail messages are updated in-place via batch modify calls.

        Edge cases:
        - Empty message or label lists are ignored.
        - Duplicate message IDs and label IDs are removed before the API call.

        Atomicity / concurrency:
        - Each Gmail batchModify request updates up to 1000 messages independently. A later
          chunk may fail after earlier chunks already succeeded.
        """
        deduplicated_message_ids = deduplicate_preserving_order(message_ids)
        deduplicated_label_ids = deduplicate_preserving_order(label_ids)
        if not deduplicated_message_ids or not deduplicated_label_ids:
            return

        for start_index in range(0, len(deduplicated_message_ids), GMAIL_BATCH_MODIFY_MESSAGE_LIMIT):
            message_id_chunk = deduplicated_message_ids[
                start_index : start_index + GMAIL_BATCH_MODIFY_MESSAGE_LIMIT
            ]
            try:
                self.service.users().messages().batchModify(
                    userId="me",
                    body={
                        "ids": message_id_chunk,
                        "addLabelIds": deduplicated_label_ids,
                    },
                ).execute()
            except Exception as error:
                raise TrackerError(
                    "Failed to apply Gmail processing labels to handled messages: "
                    f"{error}"
                ) from error

    def remove_labels_from_messages(self, message_ids: list[str], label_ids: list[str]) -> None:
        """
        Remove the provided Gmail labels from a batch of Gmail messages.

        Inputs:
        - message_ids: Gmail message IDs that should lose the labels.
        - label_ids: Gmail label IDs to remove from every provided message.

        Outputs:
        - None. Gmail messages are updated in-place via batch modify calls.

        Edge cases:
        - Empty message or label lists are ignored.
        - Duplicate message IDs and label IDs are removed before the API call.

        Atomicity / concurrency:
        - Each Gmail batchModify request updates up to 1000 messages independently. A later
          chunk may fail after earlier chunks already succeeded.
        """
        deduplicated_message_ids = deduplicate_preserving_order(message_ids)
        deduplicated_label_ids = deduplicate_preserving_order(label_ids)
        if not deduplicated_message_ids or not deduplicated_label_ids:
            return

        for start_index in range(0, len(deduplicated_message_ids), GMAIL_BATCH_MODIFY_MESSAGE_LIMIT):
            message_id_chunk = deduplicated_message_ids[
                start_index : start_index + GMAIL_BATCH_MODIFY_MESSAGE_LIMIT
            ]
            try:
                self.service.users().messages().batchModify(
                    userId="me",
                    body={
                        "ids": message_id_chunk,
                        "removeLabelIds": deduplicated_label_ids,
                    },
                ).execute()
            except Exception as error:
                raise TrackerError(
                    "Failed to remove Gmail processing labels from messages: "
                    f"{error}"
                ) from error

    def _get_user_label_ids_by_name(self) -> dict[str, str]:
        """
        Load all existing Gmail user labels keyed by display name.

        Inputs:
        - None.

        Outputs:
        - Mapping of Gmail user label names to Gmail label IDs.

        Edge cases:
        - System labels are excluded because the tracker manages only custom labels.

        Atomicity / concurrency:
        - Performs one Gmail labels list request with no local side effects.
        """
        try:
            response = cast(JsonObject, self.service.users().labels().list(userId="me").execute())
        except Exception as error:
            raise TrackerError(f"Failed to list Gmail labels: {error}") from error

        user_label_ids_by_name: dict[str, str] = {}
        for label in cast(list[dict[str, Any]], response.get("labels", [])):
            label_type = str(label.get("type", "")).strip().upper()
            if label_type != "USER":
                continue
            label_name = str(label.get("name", "")).strip()
            label_id = str(label.get("id", "")).strip()
            if not label_name or not label_id:
                continue
            user_label_ids_by_name[label_name] = label_id
        return user_label_ids_by_name

    def list_user_label_names(self) -> list[str]:
        """
        Return every Gmail user label name visible to the authenticated account.

        Inputs:
        - None.

        Outputs:
        - Sorted list of Gmail user label display names.

        Edge cases:
        - Labels are returned exactly as the Gmail API reports them.

        Atomicity / concurrency:
        - Pure wrapper around one Gmail labels list request with no shared mutable state.
        """
        return sorted(self._get_user_label_ids_by_name())

    def get_user_label_ids_by_name(self) -> dict[str, str]:
        """
        Return every Gmail user label ID keyed by display name.

        Inputs:
        - None.

        Outputs:
        - Mapping of Gmail user label names to Gmail label IDs.

        Edge cases:
        - Returns an empty mapping when the account has no user labels.

        Atomicity / concurrency:
        - Pure wrapper around one Gmail labels list request with no shared mutable state.
        """
        return self._get_user_label_ids_by_name()

    def list_labels(self) -> list[dict[str, str]]:
        """
        Return every Gmail label visible to the authenticated account.

        Inputs:
        - None.

        Outputs:
        - Sorted list of Gmail labels with their API name, id, and type.

        Edge cases:
        - Labels are returned exactly as the Gmail API reports them, including system labels.

        Atomicity / concurrency:
        - Performs one Gmail labels list request with no shared mutable state.
        """
        try:
            response = cast(JsonObject, self.service.users().labels().list(userId="me").execute())
        except Exception as error:
            raise TrackerError(f"Failed to list Gmail labels: {error}") from error

        labels: list[dict[str, str]] = []
        for label in cast(list[dict[str, Any]], response.get("labels", [])):
            labels.append(
                {
                    "id": str(label.get("id", "")).strip(),
                    "name": str(label.get("name", "")).strip(),
                    "type": str(label.get("type", "")).strip(),
                }
            )
        return sorted(labels, key=lambda label: (label["type"], label["name"]))

    def _create_user_label(self, label_name: str) -> str:
        """
        Create a Gmail user label and return its Gmail label ID.

        Inputs:
        - label_name: Gmail label display name to create.

        Outputs:
        - Gmail label ID assigned to the new user label.

        Edge cases:
        - Gmail API validation failures are wrapped with user-facing context.

        Atomicity / concurrency:
        - Performs one Gmail label creation request with no shared mutable state.
        """
        try:
            response = self.service.users().labels().create(
                userId="me",
                body={
                    "name": label_name,
                    "labelListVisibility": "labelShow",
                    "messageListVisibility": "show",
                },
            ).execute()
        except HttpError:
            raise
        except Exception as error:
            raise TrackerError(f"Failed to create Gmail label '{label_name}': {error}") from error

        created_label_id = str(response.get("id", "")).strip()
        if not created_label_id:
            raise TrackerError(f"Gmail label '{label_name}' was created without a label ID")
        return created_label_id

    def delete_user_labels(self, label_names: list[str]) -> int:
        """
        Delete the provided Gmail user labels when they exist.

        Inputs:
        - label_names: Gmail user label display names to delete.

        Outputs:
        - Number of labels successfully deleted.

        Edge cases:
        - Missing labels are ignored so reset flows stay idempotent.
        - Child labels are deleted before parent labels to avoid hierarchy conflicts.

        Atomicity / concurrency:
        - Performs one label-list read followed by independent delete calls per existing
          label. Later deletes may fail after earlier ones already succeeded.
        """
        existing_label_ids_by_name = self._get_user_label_ids_by_name()
        deleted_label_count = 0
        deduplicated_label_names = deduplicate_preserving_order(label_names)
        for label_name in sorted(deduplicated_label_names, key=lambda name: (-name.count("/"), name)):
            label_id = existing_label_ids_by_name.get(label_name)
            if not label_id:
                continue
            try:
                self.service.users().labels().delete(userId="me", id=label_id).execute()
            except Exception as error:
                raise TrackerError(f"Failed to delete Gmail label '{label_name}': {error}") from error
            deleted_label_count += 1
        return deleted_label_count

    def _create_or_get_user_label(
        self,
        label_name: str,
        existing_label_ids_by_name: dict[str, str],
    ) -> str:
        """
        Create a Gmail label or reuse it if Gmail reports a concurrent/existing-label conflict.

        Inputs:
        - label_name: Gmail label display name the tracker needs.
        - existing_label_ids_by_name: Mutable cache of known Gmail label IDs keyed by display name.

        Outputs:
        - Gmail label ID for the requested label.

        Edge cases:
        - A 409 response may mean the label already exists but the local cache is stale.
        - If Gmail reports a true ancestor/descendant naming conflict, the error includes the
          conflicting label name and a concrete remediation path.

        Atomicity / concurrency:
        - Performs at most one create request followed by one refresh list request when Gmail
          reports a conflict. The provided cache is updated in-place with the refreshed state.
        """
        try:
            return self._create_user_label(label_name)
        except HttpError as error:
            error_response = cast(object, error.resp)
            if getattr(error_response, "status", None) != 409:
                raise TrackerError(f"Failed to create Gmail label '{label_name}': {error}") from error

            refreshed_label_ids_by_name = self._get_user_label_ids_by_name()
            existing_label_ids_by_name.clear()
            existing_label_ids_by_name.update(refreshed_label_ids_by_name)

            existing_label_id = refreshed_label_ids_by_name.get(label_name)
            if existing_label_id:
                return existing_label_id

            conflicting_label_name = self._find_conflicting_label_name(
                requested_label_name=label_name,
                existing_label_ids_by_name=refreshed_label_ids_by_name,
            )
            if conflicting_label_name:
                raise GmailLabelConflictError(
                    f"Failed to create Gmail label '{label_name}' because it conflicts with "
                    f"existing Gmail label '{conflicting_label_name}'. Rename or delete the "
                    "conflicting Gmail label, or update gmail.processing_labels in config.yaml."
                ) from error

            raise GmailLabelConflictError(
                f"Failed to create Gmail label '{label_name}': Gmail reported a name conflict, "
                "but the label was not visible after refreshing the label list. Re-run the sync "
                "or check Gmail labels manually."
            ) from error
        except Exception as error:
            raise TrackerError(f"Failed to create Gmail label '{label_name}': {error}") from error

    def _find_conflicting_label_name(
        self,
        requested_label_name: str,
        existing_label_ids_by_name: dict[str, str],
    ) -> Optional[str]:
        """
        Find an existing Gmail label whose hierarchy conflicts with a requested label name.

        Inputs:
        - requested_label_name: Gmail label display name the tracker wants to create.
        - existing_label_ids_by_name: Current Gmail label IDs keyed by display name.

        Outputs:
        - The conflicting Gmail label name when an ancestor or descendant conflict exists.
        - None when no obvious hierarchy conflict is present.

        Edge cases:
        - Exact name matches are ignored because callers handle them separately.
        - Ancestor and descendant conflicts are both treated as collisions because Gmail label
          creation requires a non-conflicting hierarchy.

        Atomicity / concurrency:
        - Pure helper with no shared mutable state.
        """
        requested_prefix = f"{requested_label_name}/"
        for existing_label_name in existing_label_ids_by_name:
            existing_prefix = f"{existing_label_name}/"
            if existing_label_name == requested_label_name:
                continue
            if existing_label_name.startswith(requested_prefix):
                return existing_label_name
            if requested_label_name.startswith(existing_prefix):
                return existing_label_name
        return None


# ── Sheets Client ──────────────────────────────────────────────────────────────

COLUMNS = [
    "appl_id", "company", "role", "status", "source",
    "applied_date", "last_activity_date",
    "recruiter_name", "recruiter_email", "ats_email", "contact_email",
    "follow_up_sent_date", "follow_up_count", "withdrawal_sent_date", "deletion_request_sent_date",
    "follow_up_opt_out", "deletion_request_opt_out",
    "follow_up_missing_email_policy", "withdraw_missing_email_policy", "deletion_request_missing_email_policy",
    "withdraw_in_next_digest",
    "deferred_until",
    "notes", "linkedin_contact", "email_ids", "thread_ids", "internet_message_ids",
    "gmail_review_url", "draft_id",
]

class SheetsClient:
    def __init__(self, config: ConfigDict):
        try:
            creds = CredentialsLoader.from_authorized_user_file(str(TOKEN_PATH), SCOPES)
        except Exception as error:
            raise TrackerError(f"Could not load Sheets credentials from {TOKEN_PATH}: {error}") from error

        if creds.expired and creds.refresh_token:
            creds = _refresh_or_reauthorize_google_credentials(creds, token_path_for_errors=TOKEN_PATH)
            _persist_google_token(creds)

        gc = gspread.authorize(creds)
        try:
            spreadsheet = gc.open_by_key(config["google_sheets"]["spreadsheet_id"])
        except KeyError as error:
            raise TrackerError("config.yaml is missing google_sheets.spreadsheet_id") from error
        except Exception as error:
            raise TrackerError(f"Failed to open Google Sheet: {error}") from error
        self.spreadsheet = spreadsheet
        sheet_name  = config["google_sheets"].get("sheet_name", "Applications")
        try:
            self.ws = spreadsheet.worksheet(sheet_name)
        except gspread.WorksheetNotFound:
            try:
                self.ws = spreadsheet.add_worksheet(title=sheet_name, rows=2000, cols=len(COLUMNS))
                self.ws.append_row(COLUMNS)
                self._apply_header_filter(row_count=1, column_count=len(COLUMNS))
            except Exception as error:
                raise TrackerError(f"Failed to create worksheet '{sheet_name}': {error}") from error
        except Exception as error:
            raise TrackerError(f"Failed to open worksheet '{sheet_name}': {error}") from error

    def get_spreadsheet_url(self) -> str:
        """
        Return the browser URL for the configured Google Spreadsheet.

        Inputs:
        - None.

        Outputs:
        - A Google Sheets URL that opens the spreadsheet in a browser.

        Edge cases:
        - Falls back to constructing the URL from the spreadsheet ID if the gspread
          object does not expose a populated `url` attribute.

        Atomicity / concurrency:
        - Read-only helper with no side effects.
        """
        spreadsheet_url = str(getattr(self.spreadsheet, "url", "") or "").strip()
        if spreadsheet_url:
            return spreadsheet_url
        return f"https://docs.google.com/spreadsheets/d/{self.spreadsheet.id}/edit"

    def _execute_with_retry(
        self,
        operation_name: str,
        operation: Callable[[], OperationResultT],
    ) -> OperationResultT:
        """
        Execute a Google Sheets operation with bounded retries for transient quota errors.

        Inputs:
        - operation_name: Human-readable operation label used in error messages.
        - operation: Zero-argument callable that performs the Sheets request.

        Outputs:
        - The return value produced by the operation.

        Edge cases:
        - Retries on HTTP 429 and common 5xx responses using exponential backoff.
        - Raises TrackerError with context after the final failed attempt.

        Atomicity / concurrency:
        - Each Sheets API call is retried independently. The method does not coordinate
          across processes and cannot make multi-call workflows atomic.
        """
        retry_delay_seconds = SHEETS_INITIAL_RETRY_DELAY_SECONDS
        for attempt_number in range(1, SHEETS_MAX_WRITE_RETRIES + 1):
            try:
                return operation()
            except gspread.exceptions.APIError as error:
                status_code = getattr(error.response, "status_code", None)
                is_retryable_error = status_code in SHEETS_RETRYABLE_STATUS_CODES
                if not is_retryable_error or attempt_number == SHEETS_MAX_WRITE_RETRIES:
                    raise TrackerError(f"Failed to {operation_name}: {error}") from error
                console.print(
                    f"[yellow]Sheets {operation_name} hit HTTP {status_code}. "
                    f"Retrying in {retry_delay_seconds:.1f}s "
                    f"({attempt_number}/{SHEETS_MAX_WRITE_RETRIES})...[/yellow]"
                )
                time.sleep(retry_delay_seconds)
                retry_delay_seconds *= 2
            except Exception as error:
                raise TrackerError(f"Failed to {operation_name}: {error}") from error

        raise TrackerError(f"Failed to {operation_name}: retry loop exited unexpectedly")

    @staticmethod
    def _column_letter(column_number: int) -> str:
        """
        Convert a 1-based column index into A1-notation column letters.

        Inputs:
        - column_number: 1-based column index.

        Outputs:
        - Excel/Google Sheets style column letters such as A, Z, or AA.

        Edge cases:
        - Supports column numbers beyond 26.

        Atomicity / concurrency:
        - Pure helper with no shared mutable state.
        """
        letters: list[str] = []
        remaining_column_number = column_number
        while remaining_column_number > 0:
            remaining_column_number, remainder = divmod(remaining_column_number - 1, 26)
            letters.append(chr(65 + remainder))
        return "".join(reversed(letters))

    def _load_sheet_rows(self) -> list[list[str]]:
        """
        Load all worksheet rows and initialize the header row when the sheet is empty.

        Inputs:
        - None.

        Outputs:
        - Worksheet values as a list of rows, always including a header row.

        Edge cases:
        - Creates the default header row when the worksheet is empty.

        Atomicity / concurrency:
        - Reads the current sheet snapshot once and may perform one header write if the
          worksheet has not been initialized yet.
        """
        rows = cast(list[list[str]], self._execute_with_retry(
            "read worksheet values",
            lambda: self.ws.get_all_values(),
        ))
        if rows:
            normalized_rows = self._normalize_rows(rows)
            if normalized_rows != rows:
                self._write_rows(normalized_rows)
                return normalized_rows
            return rows

        self._execute_with_retry(
            "initialize sheet headers",
            lambda: self.ws.append_row(COLUMNS),
        )
        self._apply_header_filter(row_count=1, column_count=len(COLUMNS))
        return [list(COLUMNS)]

    def _apply_header_filter(self, row_count: int, column_count: int) -> None:
        """
        Freeze the header row and enable the standard Google Sheets filter controls.

        Inputs:
        - row_count: Total number of rows currently represented in the worksheet snapshot.
        - column_count: Total number of columns currently represented in the worksheet snapshot.

        Outputs:
        - None. Updates worksheet metadata so the first row stays visible and the header row
          exposes the built-in filter and sort dropdowns in Google Sheets.

        Edge cases:
        - Empty row or column counts are ignored because there is no meaningful filter range.
        - Reapplying the same metadata is harmless and keeps the filter range aligned with
          the latest worksheet dimensions after rows or columns are added.

        Atomicity / concurrency:
        - Sends one spreadsheet batch update request that changes worksheet metadata and the
          basic filter range together. Concurrent external edits can still race with this
          request, but the operation itself is applied as one Sheets API call.
        """
        if row_count < 1 or column_count < 1:
            return

        worksheet_id = self.ws.id
        request_body: JsonObject = {
            "requests": [
                {
                    "updateSheetProperties": {
                        "properties": {
                            "sheetId": worksheet_id,
                            "gridProperties": {
                                "frozenRowCount": 1,
                            },
                        },
                        "fields": "gridProperties.frozenRowCount",
                    }
                },
                {
                    "setBasicFilter": {
                        "filter": {
                            "range": {
                                "sheetId": worksheet_id,
                                "startRowIndex": 0,
                                "endRowIndex": row_count,
                                "startColumnIndex": 0,
                                "endColumnIndex": column_count,
                            }
                        }
                    }
                },
            ]
        }
        self._execute_with_retry(
            "configure worksheet header filter",
            lambda: self.spreadsheet.batch_update(request_body),
        )

    def _normalize_rows(self, rows: list[list[str]]) -> list[list[str]]:
        """
        Ensure worksheet rows contain the configured headers and aligned row widths.

        Inputs:
        - rows: Worksheet values including the header row.

        Outputs:
        - Normalized rows whose header includes every configured column and whose data
          rows are padded to the same width.

        Edge cases:
        - Adds missing configured columns without discarding any existing user columns.

        Atomicity / concurrency:
        - Pure in-memory transformation. No external side effects.
        """
        headers = list(rows[0]) if rows else list(COLUMNS)
        headers = [
            "appl_id" if str(column_name).strip() == "app_id" else column_name
            for column_name in headers
        ]
        for column_name in COLUMNS:
            if column_name not in headers:
                headers.append(column_name)

        normalized_rows = [headers]
        header_count = len(headers)
        for raw_row in rows[1:]:
            normalized_row = list(raw_row[:header_count])
            if len(normalized_row) < header_count:
                normalized_row.extend([""] * (header_count - len(normalized_row)))
            normalized_rows.append(normalized_row)
        return normalized_rows

    @staticmethod
    def _populate_derived_application_fields(application_row: ApplicationRecord) -> ApplicationRecord:
        """
        Populate sheet columns that are derived from stored Gmail identity fields.

        Inputs:
        - application_row: Application record that may already contain persisted or partially
          missing Gmail identity metadata.

        Outputs:
        - Copy of the application row with derived Gmail review fields normalized.

        Edge cases:
        - Rows without Gmail-backed emails leave `gmail_review_url` blank.
        - Existing stored values are overwritten so derived fields remain consistent after
          merges or manual edits to the underlying JSON ID columns.

        Atomicity / concurrency:
        - Pure in-memory transformation with no shared mutable state.
        """
        normalized_application_row = dict(application_row)
        normalized_application_row["gmail_review_url"] = build_gmail_review_url(
            normalized_application_row
        )
        return normalized_application_row

    def _ensure_sheet_dimensions(self, row_count: int, column_count: int) -> None:
        """
        Expand the worksheet grid when a pending write exceeds its current dimensions.

        Inputs:
        - row_count: Required number of rows.
        - column_count: Required number of columns.

        Outputs:
        - None. The worksheet is resized only when necessary.

        Edge cases:
        - Leaves the worksheet unchanged when the existing grid is already large enough.

        Atomicity / concurrency:
        - Resizes rows and columns independently. The grid may be partially expanded if a
          later resize call fails.
        """
        additional_rows_needed = row_count - self.ws.row_count
        if additional_rows_needed > 0:
            self._execute_with_retry(
                f"add {additional_rows_needed} worksheet rows",
                lambda: self.ws.add_rows(additional_rows_needed),
            )

        additional_columns_needed = column_count - self.ws.col_count
        if additional_columns_needed > 0:
            self._execute_with_retry(
                f"add {additional_columns_needed} worksheet columns",
                lambda: self.ws.add_cols(additional_columns_needed),
            )

    def _write_rows(self, rows: list[list[str]]) -> None:
        """
        Persist normalized worksheet rows using chunked range updates.

        Inputs:
        - rows: Worksheet values including the header row.

        Outputs:
        - None. Writes the provided rows back to Google Sheets.

        Edge cases:
        - Splits large writes into bounded chunks to reduce quota pressure.
        - Uses named `range_name` and `values` arguments to match current gspread API.

        Atomicity / concurrency:
        - Each chunk is written independently. A partially completed write may exist if a
          later chunk fails after earlier chunks succeed.
        """
        if not rows:
            return

        normalized_rows = self._normalize_rows(rows)
        headers = normalized_rows[0]
        derived_rows = [headers]
        for raw_row in normalized_rows[1:]:
            if not any(cell.strip() for cell in raw_row):
                derived_rows.append(raw_row)
                continue
            application_row = dict(zip(headers, raw_row))
            application_row = self._populate_derived_application_fields(application_row)
            derived_rows.append(
                [str(application_row.get(column_name, "") or "") for column_name in headers]
            )
        normalized_rows = derived_rows
        total_rows = len(normalized_rows)
        total_columns = len(normalized_rows[0])
        self._ensure_sheet_dimensions(total_rows, total_columns)

        end_column_letter = self._column_letter(total_columns)
        for start_index in range(0, total_rows, SHEETS_WRITE_CHUNK_SIZE):
            chunk_rows = normalized_rows[start_index : start_index + SHEETS_WRITE_CHUNK_SIZE]
            start_row_number = start_index + 1
            end_row_number = start_row_number + len(chunk_rows) - 1
            range_name = f"A{start_row_number}:{end_column_letter}{end_row_number}"
            def write_chunk(
                range_name: str = range_name,
                chunk_rows: list[list[str]] = chunk_rows,
            ) -> Any:
                return self.ws.update(
                    range_name=range_name,
                    values=chunk_rows,
                )
            self._execute_with_retry(
                f"write worksheet rows {start_row_number}-{end_row_number}",
                write_chunk,
            )
        self._apply_header_filter(row_count=total_rows, column_count=total_columns)

    def get_all(self) -> list[ApplicationRecord]:
        rows = self._load_sheet_rows()
        normalized_rows = self._normalize_rows(rows)
        headers = normalized_rows[0]
        records: list[ApplicationRecord] = []
        for row in normalized_rows[1:]:
            if not any(cell.strip() for cell in row):
                continue
            records.append(self._populate_derived_application_fields(dict(zip(headers, row))))
        return records

    def consolidate_similar_applications(self) -> tuple[list[ApplicationRecord], int]:
        """
        Merge obviously duplicated application rows and rewrite the worksheet snapshot.

        Inputs:
        - None. Reads the current worksheet state and applies deterministic merge heuristics.

        Outputs:
        - Tuple of `(applications, merged_duplicate_count)` where `applications` is the
          canonical post-merge record list.

        Edge cases:
        - Same-company/same-role rows always merge.
        - Same-company rows with stage-like or missing roles only merge when their dates are
          close enough to represent one process.
        - Extra worksheet columns are preserved from the first retained row.

        Atomicity / concurrency:
        - Reads the worksheet once, merges in memory, then rewrites the snapshot. Concurrent
          external edits can still race between the read and write phases.
        """
        rows = self._normalize_rows(self._load_sheet_rows())
        headers = rows[0]
        original_row_count = len(rows)
        application_rows = [
            dict(zip(headers, row))
            for row in rows[1:]
            if any(cell.strip() for cell in row)
        ]
        if not application_rows:
            return [], 0

        consolidated_applications: list[ApplicationRecord] = []
        merged_duplicate_count = 0
        for application_row in application_rows:
            strongest_match_score = 0
            strongest_match_index: Optional[int] = None
            for index, existing_row in enumerate(consolidated_applications):
                merge_strength = get_application_merge_strength(existing_row, application_row)
                if merge_strength > strongest_match_score:
                    strongest_match_score = merge_strength
                    strongest_match_index = index

            if strongest_match_index is None:
                consolidated_applications.append(dict(application_row))
                continue

            consolidated_applications[strongest_match_index] = merge_application_records(
                consolidated_applications[strongest_match_index],
                application_row,
            )
            merged_duplicate_count += 1

        if merged_duplicate_count == 0:
            return consolidated_applications, 0

        rewritten_rows = [headers]
        for application_row in consolidated_applications:
            rewritten_rows.append(
                [str(application_row.get(column_name, "") or "") for column_name in headers]
            )

        while len(rewritten_rows) < original_row_count:
            rewritten_rows.append([""] * len(headers))

        self._write_rows(rewritten_rows)
        return consolidated_applications, merged_duplicate_count

    def upsert(self, app: ApplicationRecord) -> None:
        self.upsert_many([app])

    def upsert_many(self, apps: list[ApplicationRecord]) -> None:
        """
        Insert or update multiple application records using a single in-memory sheet snapshot.

        Inputs:
        - apps: Application dictionaries keyed by the worksheet column names.

        Outputs:
        - None. The worksheet is updated so each appl_id appears once with the latest values.

        Edge cases:
        - Repeated appl_ids in the input keep the last provided version.
        - Blank rows in the worksheet are ignored when building the appl_id index.

        Atomicity / concurrency:
        - Reads the worksheet once, applies all changes in memory, then writes chunked
          updates back to the sheet. Concurrent external writers can still race with this
          process between the read and write phases.
        """
        if not apps:
            return

        rows = self._normalize_rows(self._load_sheet_rows())
        headers = rows[0]
        appl_id_to_row_index: dict[str, int] = {}
        for row_index, row in enumerate(rows[1:], start=1):
            appl_id_value = row[0].strip() if row else ""
            if appl_id_value:
                appl_id_to_row_index[appl_id_value] = row_index

        for app in apps:
            normalized_app = self._populate_derived_application_fields(app)
            values = [str(normalized_app.get(column_name, "") or "") for column_name in headers]
            appl_id = values[0]
            if appl_id in appl_id_to_row_index:
                rows[appl_id_to_row_index[appl_id]] = values
            else:
                rows.append(values)
                appl_id_to_row_index[appl_id] = len(rows) - 1

        self._write_rows(rows)

    def set_field(self, appl_id: str, field: str, value: str) -> None:
        rows = self._normalize_rows(self._load_sheet_rows())
        if not rows:
            return
        headers = rows[0]
        if field not in headers:
            console.print(f"[yellow]Sheet field '{field}' not found. Skipping update for {appl_id}.[/yellow]")
            return
        col_i = headers.index(field) + 1
        appl_ids = [r[0] for r in rows[1:]]
        if appl_id in appl_ids:
            row_number = appl_ids.index(appl_id) + 2
            cell_range = f"{self._column_letter(col_i)}{row_number}"
            self._execute_with_retry(
                f"update field '{field}' for appl_id {appl_id}",
                lambda: self.ws.update(
                    range_name=cell_range,
                    values=[[value]],
                ),
            )

    def delete_application(self, appl_id: str) -> Optional[ApplicationRecord]:
        """
        Delete one application row from the worksheet by appl_id.

        Inputs:
        - appl_id: Application identifier to remove from the sheet.

        Outputs:
        - Deleted application row as a dictionary, or None when the appl_id is not present.

        Edge cases:
        - Blank appl_ids return None without modifying the sheet.
        - Only the first matching row is deleted because appl_ids are expected to be unique.

        Atomicity / concurrency:
        - Reads the worksheet once, removes one row in memory, then rewrites the snapshot.
          Concurrent external edits can still race between the read and write phases.
        """
        normalized_appl_id = str(appl_id or "").strip()
        if not normalized_appl_id:
            return None

        rows = self._normalize_rows(self._load_sheet_rows())
        headers = rows[0]
        retained_rows = [headers]
        deleted_application: Optional[ApplicationRecord] = None

        for row in rows[1:]:
            application_row = dict(zip(headers, row))
            if deleted_application is None and application_row.get("appl_id", "").strip() == normalized_appl_id:
                deleted_application = self._populate_derived_application_fields(application_row)
                continue
            retained_rows.append(row)

        if deleted_application is None:
            return None

        while len(retained_rows) < len(rows):
            retained_rows.append([""] * len(headers))

        self._write_rows(retained_rows)
        return deleted_application

    def delete_applications(self, appl_ids: list[str]) -> list[ApplicationRecord]:
        """
        Delete multiple application rows from the worksheet by appl_id.

        Inputs:
        - appl_ids: Application identifiers to remove from the sheet.

        Outputs:
        - Deleted application rows in worksheet order.

        Edge cases:
        - Blank and duplicate appl_ids are ignored.
        - Missing appl_ids are ignored so callers can compare requested versus deleted IDs.

        Atomicity / concurrency:
        - Reads the worksheet once, removes matching rows in memory, then rewrites the
          snapshot. Concurrent external edits can still race between the read and write phases.
        """
        normalized_appl_ids = deduplicate_preserving_order([
            str(appl_id or "").strip()
            for appl_id in appl_ids
            if str(appl_id or "").strip()
        ])
        if not normalized_appl_ids:
            return []

        appl_id_set = set(normalized_appl_ids)
        rows = self._normalize_rows(self._load_sheet_rows())
        headers = rows[0]
        retained_rows = [headers]
        deleted_applications: list[ApplicationRecord] = []

        for row in rows[1:]:
            application_row = dict(zip(headers, row))
            if application_row.get("appl_id", "").strip() in appl_id_set:
                deleted_applications.append(
                    self._populate_derived_application_fields(application_row)
                )
                continue
            retained_rows.append(row)

        if not deleted_applications:
            return []

        while len(retained_rows) < len(rows):
            retained_rows.append([""] * len(headers))

        self._write_rows(retained_rows)
        return deleted_applications

    def reset_applications_sheet(self) -> None:
        """
        Destructively clear the applications worksheet and recreate the header row.

        Inputs:
        - None.

        Outputs:
        - None. The worksheet contents are replaced with only the configured header row.

        Edge cases:
        - Existing formatting and filters may be cleared by the underlying worksheet reset and
          are reapplied for the recreated header row.

        Atomicity / concurrency:
        - Performs a clear followed by a header write. External edits can race between those
          calls, so the operation is destructive but not transactionally isolated.
        """
        self._execute_with_retry(
            "clear applications worksheet",
            lambda: self.ws.clear(),
        )
        self._execute_with_retry(
            "recreate applications worksheet headers",
            lambda: self.ws.append_row(COLUMNS),
        )
        self._apply_header_filter(row_count=1, column_count=len(COLUMNS))


# ── AI Grouper ─────────────────────────────────────────────────────────────────

class AIGrouper:
    """
    Group raw Gmail messages into application records using Gemini.

    Inputs:
    - config: Application configuration dictionary containing Gemini settings.

    Outputs:
    - Instance with a configured Gemini client and generation settings.

    Edge cases:
    - Missing API configuration raises TrackerError with actionable context.
    - Gemini quota and rate-limit failures are surfaced as GeminiRateLimitError so
      callers can decide whether to wait, retry, or stop processing.

    Atomicity / concurrency:
    - Stateless across requests except for immutable configuration and the shared API client.
    """

    def _build_free_tier_grounded_model_names(
        self,
        configured_model_names: list[str],
    ) -> list[str]:
        """
        Filter configured Gemini models down to those that support Google Search grounding on
        the free tier.

        Inputs:
        - configured_model_names: Deduplicated Gemini model names from config priority order.

        Outputs:
        - Ordered subset of model names that are safe to use for free-tier grounded search.

        Edge cases:
        - Unknown, preview, or paid-only grounded-search models are excluded.
        - Returns an empty list when the config contains no free-tier grounded-search-safe
          models, allowing callers to raise a clear configuration error before attempting a
          grounded request.

        Atomicity / concurrency:
        - Pure helper with no side effects.
        """
        free_tier_grounded_model_names: list[str] = []
        allowed_model_names = set(FREE_TIER_GOOGLE_SEARCH_GROUNDED_MODEL_NAMES)
        for configured_model_name in configured_model_names:
            if configured_model_name in allowed_model_names:
                free_tier_grounded_model_names.append(configured_model_name)
        return free_tier_grounded_model_names

    def __init__(self, config: ConfigDict):
        self._config = config
        try:
            gcfg = config["gemini"]
            api_key = gcfg["api_key"]
        except KeyError as error:
            raise TrackerError("config.yaml is missing gemini.api_key") from error

        configured_model_names = gcfg.get("models", [])
        if configured_model_names and not isinstance(configured_model_names, list):
            raise TrackerError("config.yaml field gemini.models must be a YAML list of model names")
        normalized_configured_model_names = [
            str(model_name).strip()
            for model_name in cast(list[object], configured_model_names or [])
            if str(model_name).strip()
        ]

        fallback_model_name = gcfg.get("model", "gemini-2.5-flash-lite")
        self.model_names = deduplicate_preserving_order(
            normalized_configured_model_names + [str(fallback_model_name).strip()]
        )
        if not self.model_names:
            raise TrackerError("config.yaml must define at least one Gemini model")
        self.free_tier_grounded_model_names = self._build_free_tier_grounded_model_names(
            self.model_names
        )
        self.client = genai.Client(api_key=api_key)
        self.json_generation_config = types.GenerateContentConfig(
            response_mime_type="application/json",
            temperature=0.1,
        )
        self.creative_generation_config = types.GenerateContentConfig(
            temperature=0.7,
        )
        self.user_owned_email_addresses = self._build_user_owned_email_addresses()

    def _build_user_owned_email_addresses(self) -> set[str]:
        """
        Collect the configured user email addresses that should never be treated as recruiters.

        Inputs:
        - None. Reads `user.personal_email` and `user.career_email` from the loaded config.

        Outputs:
        - Lowercased set of configured user-controlled email addresses with blank values removed.

        Edge cases:
        - Missing `user` config or blank address fields produce an empty or partial set.
        - Duplicate addresses are collapsed automatically by the set.

        Atomicity / concurrency:
        - Pure helper with no side effects.
        """
        user_config = self._config.get("user", {})
        configured_user_email_addresses = {
            str(user_config.get("personal_email", "")).strip().lower(),
            str(user_config.get("career_email", "")).strip().lower(),
        }
        return {
            configured_email_address
            for configured_email_address in configured_user_email_addresses
            if configured_email_address
        }

    def _is_user_owned_sender(self, sender_email_address: str) -> bool:
        """
        Determine whether a parsed sender email belongs to the configured user.

        Inputs:
        - sender_email_address: Parsed sender email extracted from Gmail headers or Gemini output.

        Outputs:
        - True when the sender matches one of the configured user-owned email addresses.

        Edge cases:
        - Blank or malformed sender values return False after normalization.
        - Matching is case-insensitive and trims surrounding whitespace.

        Atomicity / concurrency:
        - Pure helper with no side effects.
        """
        normalized_sender_email_address = str(sender_email_address).strip().lower()
        if not normalized_sender_email_address:
            return False
        return normalized_sender_email_address in self.user_owned_email_addresses

    def _parse_retry_delay_seconds(self, error_text: str) -> Optional[int]:
        """
        Extract a retry delay hint from Gemini error text.

        Inputs:
        - error_text: Raw exception text returned by the Gemini SDK.

        Outputs:
        - Retry delay in whole seconds, or None when no hint is present.

        Edge cases:
        - Handles formats like `retry in 27.6s` and `retryDelay': '27s'`.

        Atomicity / concurrency:
        - Pure helper with no side effects.
        """
        retry_patterns = [
            r"retry in (\d+(?:\.\d+)?)s",
            r"retryDelay['\"]?\s*:\s*['\"](\d+(?:\.\d+)?)s['\"]",
        ]
        for retry_pattern in retry_patterns:
            match = re.search(retry_pattern, error_text, flags=re.IGNORECASE)
            if match:
                return max(1, int(float(match.group(1))))
        return None

    def _is_daily_quota_exhausted(self, error_text: str) -> bool:
        """
        Detect whether a Gemini error indicates daily quota exhaustion.

        Inputs:
        - error_text: Raw exception text returned by the Gemini SDK.

        Outputs:
        - True when the error appears to be a daily or free-tier quota exhaustion event.

        Edge cases:
        - Matches the current Gemini free-tier quota wording without depending on an exact
          serialized error shape.

        Atomicity / concurrency:
        - Pure helper with no side effects.
        """
        normalized_error_text = error_text.lower()
        return (
            "resource_exhausted" in normalized_error_text
            and (
                "quota exceeded" in normalized_error_text
                or "freetier" in normalized_error_text
                or "free_tier" in normalized_error_text
                or "perday" in normalized_error_text
            )
        )

    def _is_transient_model_unavailable(self, error_text: str) -> bool:
        """
        Detect temporary Gemini model unavailability that should use the retry/fallback path.

        Inputs:
        - error_text: Raw exception text returned by the Gemini SDK.

        Outputs:
        - True when the failure looks like a temporary 503/high-demand event.

        Edge cases:
        - Requires both a 503 signal and temporary-unavailability wording to avoid masking
          unrelated backend configuration failures as retryable.

        Atomicity / concurrency:
        - Pure helper with no side effects.
        """
        normalized_error_text = error_text.lower()
        has_503_signal = "503" in error_text or "'code': 503" in normalized_error_text
        has_temporary_unavailable_signal = (
            "status': 'unavailable'" in normalized_error_text
            or '"status": "unavailable"' in normalized_error_text
            or "currently experiencing high demand" in normalized_error_text
            or "spikes in demand are usually temporary" in normalized_error_text
            or "please try again later" in normalized_error_text
        )
        return has_503_signal and has_temporary_unavailable_signal

    def _is_transient_transport_disconnect(self, error_text: str) -> bool:
        """
        Detect transport-level disconnects before Gemini returns an HTTP response.

        Inputs:
        - error_text: Raw exception text returned by the Gemini SDK or HTTP client.

        Outputs:
        - True when the connection appears to have closed before response headers arrived.

        Edge cases:
        - Matches current httpcore wording while remaining tolerant to exception class names
          appearing in wrapped error strings.

        Atomicity / concurrency:
        - Pure helper with no side effects.
        """
        return is_transient_transport_disconnect(error_text)

    def _describe_retryable_model_error(self, error_text: str) -> str:
        """
        Build a concise reason string for retryable Gemini failures.

        Inputs:
        - error_text: Raw exception text returned by the Gemini SDK.

        Outputs:
        - Human-readable reason suitable for stdout logging.

        Edge cases:
        - Prefers specific provider wording like "high demand" over generic status codes.

        Atomicity / concurrency:
        - Pure helper with no side effects.
        """
        normalized_error_text = error_text.lower()
        if self._is_daily_quota_exhausted(error_text):
            return "daily quota exhausted"
        if self._is_transient_transport_disconnect(error_text):
            return "transport disconnect before response"
        if "429" in error_text or "resource_exhausted" in normalized_error_text:
            return "quota or rate limits"
        if self._is_transient_model_unavailable(error_text):
            return "temporary high demand (503 UNAVAILABLE)"
        return "temporary provider failure"

    def _generate_content(
        self,
        prompt: str,
        generation_config: types.GenerateContentConfig,
        model_names: Optional[list[str]] = None,
    ) -> Any:
        """
        Send a prompt to Gemini, trying configured models in priority order.

        Inputs:
        - prompt: Prompt text to send to Gemini.
        - generation_config: Gemini generation settings for this request.
        - model_names: Optional explicit model priority list. When omitted, uses the full
          configured model list.

        Outputs:
        - Raw Gemini SDK response from the first successful model.

        Edge cases:
        - Rate-limited or quota-exhausted models are skipped in favor of the next configured model.
        - If every configured model fails with rate limiting, the final GeminiRateLimitError is re-raised.

        Atomicity / concurrency:
        - Performs one outbound Gemini request at a time with no shared mutable state beyond the client.
        """
        last_rate_limit_error: Optional[GeminiRateLimitError] = None
        candidate_model_names = model_names or self.model_names
        if not candidate_model_names:
            raise TrackerError("No Gemini models are available for this request")

        for model_index, model_name in enumerate(candidate_model_names):
            configured_transient_model_retries = getattr(
                self,
                "transient_model_retries",
                DEFAULT_AI_TRANSIENT_MODEL_RETRIES,
            )
            transient_model_retries = max(0, int(configured_transient_model_retries))
            retry_delay_seconds = float(
                getattr(
                    self,
                    "transient_model_initial_retry_delay_seconds",
                    DEFAULT_AI_TRANSIENT_MODEL_INITIAL_RETRY_DELAY_SECONDS,
                )
            )
            backoff_multiplier = float(
                getattr(
                    self,
                    "transient_model_backoff_multiplier",
                    DEFAULT_AI_TRANSIENT_MODEL_BACKOFF_MULTIPLIER,
                )
            )

            for model_attempt in range(transient_model_retries + 1):
                try:
                    models_client = cast(Any, self.client.models)
                    resp = models_client.generate_content(
                        model=model_name,
                        contents=prompt,
                        config=generation_config,
                    )
                except Exception as error:
                    error_text = str(error)
                    normalized_error_text = error_text.lower()
                    if self._is_transient_transport_disconnect(error_text):
                        last_rate_limit_error = GeminiRateLimitError(
                            message=f"Gemini request failed for model {model_name}: {error}",
                            retry_delay_seconds=max(1, int(retry_delay_seconds)),
                            is_daily_quota_exhausted=False,
                        )
                        if model_attempt < transient_model_retries:
                            console.print(
                                f"[yellow]  Gemini model retry: {model_name} failed due to "
                                f"transport disconnect before response. Waiting "
                                f"{retry_delay_seconds:.1f}s before retry {model_attempt + 2} "
                                f"of {transient_model_retries + 1}.[/yellow]"
                            )
                            time.sleep(retry_delay_seconds)
                            retry_delay_seconds *= max(1.0, backoff_multiplier)
                            continue
                        if model_index < len(candidate_model_names) - 1:
                            console.print(
                                f"[yellow]  Gemini model fallback: {model_name} failed due to "
                                f"transport disconnect before response after "
                                f"{transient_model_retries + 1} attempts. Falling back to "
                                f"{candidate_model_names[model_index + 1]}.[/yellow]"
                            )
                            break
                        raise last_rate_limit_error from error

                    if (
                        "429" in error_text
                        or "resource_exhausted" in normalized_error_text
                        or self._is_transient_model_unavailable(error_text)
                    ):
                        last_rate_limit_error = GeminiRateLimitError(
                            message=f"Gemini request failed for model {model_name}: {error}",
                            retry_delay_seconds=self._parse_retry_delay_seconds(error_text),
                            is_daily_quota_exhausted=self._is_daily_quota_exhausted(error_text),
                        )
                        if model_index < len(candidate_model_names) - 1:
                            retryable_reason = self._describe_retryable_model_error(error_text)
                            console.print(
                                f"[yellow]  Gemini model fallback: {model_name} failed due to "
                                f"{retryable_reason}. Trying {candidate_model_names[model_index + 1]}.[/yellow]"
                            )
                            break
                        raise last_rate_limit_error from error
                    raise TrackerError(f"Gemini request failed for model {model_name}: {error}") from error

                return resp

        if last_rate_limit_error is not None:
            raise last_rate_limit_error
        raise TrackerError("Gemini request failed before any model could return a response")

    def _call(self, prompt: str, json_mode: bool = True) -> str:
        """
        Send a prompt to Gemini and return plain response text.

        Inputs:
        - prompt: Prompt text to send to Gemini.
        - json_mode: Whether to request JSON output settings.

        Outputs:
        - Response text from the first successful model.

        Edge cases:
        - Empty responses raise TrackerError with model context from `_generate_content`.

        Atomicity / concurrency:
        - Performs one outbound Gemini request at a time with no shared mutable state beyond the client.
        """
        generation_config = (
            self.json_generation_config
            if json_mode
            else self.creative_generation_config
        )
        response = self._generate_content(prompt, generation_config)
        response_text = getattr(response, "text", "") or ""
        if not response_text.strip():
            raise TrackerError("Gemini returned an empty response")
        return response_text.strip()

    def _extract_grounding_web_queries(self, response: Any) -> list[str]:
        candidates = getattr(response, "candidates", None) or []
        if not candidates:
            return []
        grounding_metadata = getattr(candidates[0], "grounding_metadata", None)
        if grounding_metadata is None:
            return []
        return [
            str(search_query).strip()
            for search_query in (getattr(grounding_metadata, "web_search_queries", None) or [])
            if str(search_query).strip()
        ]

    def _extract_grounding_web_sources(self, response: Any) -> list[JsonObject]:
        candidates = getattr(response, "candidates", None) or []
        if not candidates:
            return []
        grounding_metadata = getattr(candidates[0], "grounding_metadata", None)
        if grounding_metadata is None:
            return []

        grounding_sources: list[JsonObject] = []
        for grounding_chunk in getattr(grounding_metadata, "grounding_chunks", None) or []:
            web_chunk = getattr(grounding_chunk, "web", None)
            if web_chunk is None:
                continue
            source_title = str(getattr(web_chunk, "title", "") or "").strip()
            source_url = str(getattr(web_chunk, "uri", "") or "").strip()
            if not source_title and not source_url:
                continue
            grounding_sources.append({
                "title": source_title,
                "url": source_url,
            })
        return grounding_sources

    def find_grounded_company_contact_email(
        self,
        company: str,
        action_type: str,
        known_domains: Optional[list[str]] = None,
        company_name_hints: Optional[list[str]] = None,
    ) -> JsonObject:
        """
        Use Gemini with Google Search grounding to discover a company contact email.

        Inputs:
        - company: Canonical company name from the application row.
        - action_type: Pending action type such as `follow_up`, `withdraw`, or `deletion_request`.
        - known_domains: Optional domain hints already seen in the application context.
        - company_name_hints: Optional alternate company names discovered from LinkedIn or email content.

        Outputs:
        - JSON-like dictionary containing the model-selected email plus grounding metadata.

        Edge cases:
        - Returns empty-string fields when no grounded contact can be found.
        - Falls back to grounding metadata when the model omits source details.

        Atomicity / concurrency:
        - Performs one grounded Gemini request with no shared mutable state beyond the client.
        """
        normalized_company = str(company or "").strip()
        normalized_known_domains = [
            str(domain).strip().lower()
            for domain in (known_domains or [])
            if str(domain).strip()
        ]
        normalized_company_name_hints = [
            str(company_name_hint).strip()
            for company_name_hint in (company_name_hints or [])
            if str(company_name_hint).strip()
        ]
        if normalized_company and normalized_company not in normalized_company_name_hints:
            normalized_company_name_hints.insert(0, normalized_company)

        desired_contact_description = (
            "a recruiter, recruiting, careers, hiring, or talent acquisition contact"
            if action_type == "follow_up"
            else "a privacy, GDPR, data protection, DPO, legal, or deletion-request contact"
        )
        fallback_contact_description = (
            "If no recruiting contact is explicitly published, return an empty email."
            if action_type == "follow_up"
            else (
                "If no privacy-style contact is explicitly published, you may use an official recruiting/careers contact only when it is clearly public and replyable."
            )
        )

        prompt = "\n".join([
            f"Use Google Search to find {desired_contact_description} for the company {json.dumps(normalized_company)}.",
            "Do not guess or synthesize an email address.",
            "Only return an email if it is explicitly shown on a public web page.",
            "Prefer official company pages over third-party sources. Trusted ATS/careers pages are acceptable only when the company page does not expose a better contact.",
            "Never return no-reply, notification, or obviously non-replyable addresses.",
            "Prefer the most specific contact for the action. For privacy actions, prefer privacy/datenschutz/gdpr/dpo/legal over general info/support inboxes.",
            fallback_contact_description,
            f"Known company/domain hints from the application: {json.dumps(normalized_company_name_hints)}",
            f"Known related domains from the application context: {json.dumps(normalized_known_domains)}",
            'Return exactly one JSON object with keys "email", "source_url", "source_domain", "source_title", and "notes".',
            'If nothing reliable is found, return {"email":"","source_url":"","source_domain":"","source_title":"","notes":""}.',
        ])
        if not self.free_tier_grounded_model_names:
            supported_model_names = ", ".join(FREE_TIER_GOOGLE_SEARCH_GROUNDED_MODEL_NAMES)
            raise TrackerError(
                "config.yaml gemini.models must include at least one model that supports "
                "Google Search grounding on the free tier for web contact discovery. "
                f"Supported models: {supported_model_names}."
            )

        response = self._generate_content(
            prompt,
            types.GenerateContentConfig(
                tools=[types.Tool(google_search=types.GoogleSearch())],
                temperature=0.1,
            ),
            model_names=self.free_tier_grounded_model_names,
        )
        response_text = getattr(response, "text", "") or ""
        parsed_result = extract_first_json_object(response_text) or {}
        grounding_sources = self._extract_grounding_web_sources(response)
        grounding_queries = self._extract_grounding_web_queries(response)

        if not parsed_result:
            parsed_result = {}

        result_email = extract_email_address(str(parsed_result.get("email", "") or ""))
        result_source_url = str(parsed_result.get("source_url", "") or "").strip()
        result_source_domain = get_base_domain(str(parsed_result.get("source_domain", "") or "").strip().lower())
        result_source_title = str(parsed_result.get("source_title", "") or "").strip()
        result_notes = str(parsed_result.get("notes", "") or "").strip()

        if not result_source_url and grounding_sources:
            result_source_url = str(grounding_sources[0].get("url", "") or "").strip()
        if not result_source_title and grounding_sources:
            result_source_title = str(grounding_sources[0].get("title", "") or "").strip()
        if not result_source_domain:
            source_url_hostname = str(urlparse(result_source_url).hostname or "").strip().lower()
            if source_url_hostname and source_url_hostname != "vertexaisearch.cloud.google.com":
                result_source_domain = get_base_domain(source_url_hostname)

        return {
            "email": result_email,
            "source_url": result_source_url,
            "source_domain": result_source_domain,
            "source_title": result_source_title,
            "notes": result_notes,
            "search_queries": grounding_queries,
            "grounding_sources": grounding_sources,
            "raw_response_text": response_text.strip(),
        }

    def choose_best_company_contact_email(
        self,
        company: str,
        action_type: str,
        candidates: list[ContactEmailCandidate],
        known_domains: Optional[list[str]] = None,
        company_name_hints: Optional[list[str]] = None,
    ) -> JsonObject:
        """
        Ask Gemini to choose the best contact email from collected candidates.

        Inputs:
        - company: Canonical company name from the application row.
        - action_type: Pending action type such as `follow_up`, `withdraw`, or `deletion_request`.
        - candidates: Structured candidate emails plus evidence.
        - known_domains: Known company-related domains from Gmail or LinkedIn context.
        - company_name_hints: Alternate company names discovered from context.

        Outputs:
        - JSON-like dictionary with the selected email and a short reason.

        Edge cases:
        - Empty candidate lists return an empty selection.
        - The model is constrained to pick only from supplied emails.

        Atomicity / concurrency:
        - Performs one outbound Gemini request with no shared mutable state beyond the client.
        """
        normalized_candidates: list[ContactEmailCandidate] = []
        seen_emails: set[str] = set()
        for candidate in candidates:
            normalized_email = extract_email_address(str(candidate.get("email", "") or ""))
            if not normalized_email or normalized_email in seen_emails:
                continue
            seen_emails.add(normalized_email)
            normalized_candidates.append({
                "email": normalized_email,
                "source_type": str(candidate.get("source_type", "") or "").strip(),
                "source_url": str(candidate.get("source_url", "") or "").strip(),
                "source_domain": str(candidate.get("source_domain", "") or "").strip().lower(),
                "source_title": str(candidate.get("source_title", "") or "").strip(),
                "notes": str(candidate.get("notes", "") or "").strip(),
            })

        if not normalized_candidates:
            return {"email": "", "reason": ""}

        prompt = "\n".join([
            f"Choose the best outbound contact email for the company {json.dumps(str(company or '').strip())}.",
            f"Action type: {action_type}",
            "You must select ONLY from the provided candidate emails.",
            "Prefer the most appropriate recipient for the action:",
            "- follow_up: recruiter, hiring manager, HR, careers, talent, or a clearly replyable person involved in hiring",
            "- withdraw/deletion_request: privacy, GDPR, DPO, legal, or deletion mailbox first; otherwise use the best official replyable company contact",
            "Avoid no-reply, notification, and low-confidence third-party addresses.",
            f"Known company/domain hints: {json.dumps(company_name_hints or [])}",
            f"Known related domains: {json.dumps(known_domains or [])}",
            f"Candidates: {json.dumps(normalized_candidates, indent=2)}",
            'Return exactly one JSON object with keys "email" and "reason".',
            'If none are suitable, return {"email":"","reason":""}.',
        ])

        raw_response = self._call(prompt, json_mode=True)
        parsed_result = extract_first_json_object(raw_response) or {}
        selected_email = extract_email_address(str(parsed_result.get("email", "") or ""))
        if selected_email not in seen_emails:
            selected_email = ""
        return {
            "email": selected_email,
            "reason": str(parsed_result.get("reason", "") or "").strip(),
        }

    def _build_grouping_summaries(self, new_emails: list[GmailMessage]) -> list[JsonObject]:
        """
        Build the normalized email summaries sent to Gemini for grouping.

        Inputs:
        - new_emails: Parsed Gmail message dictionaries to classify.

        Outputs:
        - List of JSON-like summary objects containing sender metadata and email content.

        Edge cases:
        - Sender email parsing falls back safely on malformed `From` headers.

        Atomicity / concurrency:
        - Pure helper with no side effects.
        """
        summaries: list[JsonObject] = []
        for email_message in new_emails:
            sender_email_address = resolve_message_sender_email(email_message)
            sender_email_domain = extract_email_domain(sender_email_address)
            summaries.append({
                "id": email_message["id"],
                "from": email_message["from"],
                "reply_to": email_message.get("reply_to", ""),
                "sender_email": sender_email_address,
                "sender_domain": sender_email_domain,
                "sender_is_known_ats": is_ats_sender_domain(sender_email_domain),
                "sender_company_hint": infer_company_hint_from_sender_domain(sender_email_domain),
                "has_strong_application_signal": has_strong_application_signal(email_message),
                "looks_like_unsolicited_outreach": is_unsolicited_recruiter_outreach_email(email_message),
                "subject": email_message["subject"],
                "date": email_message["date"],
                "body": email_message["body"],
            })
        return summaries

    @staticmethod
    def _build_existing_application_stubs(
        existing_apps: list[ApplicationRecord],
    ) -> list[JsonObject]:
        """
        Build the reduced application context sent to Gemini for matching.

        Inputs:
        - existing_apps: Current application rows from Sheets.

        Outputs:
        - List of JSON-like stubs containing only grouping-relevant fields.

        Edge cases:
        - Missing fields are preserved as empty values.

        Atomicity / concurrency:
        - Pure helper with no side effects.
        """
        return [{
            "appl_id": application.get("appl_id"),
            "company": application.get("company"),
            "role": application.get("role"),
            "status": application.get("status"),
            "applied_date": application.get("applied_date"),
            "last_activity_date": application.get("last_activity_date"),
        } for application in existing_apps]

    def _build_grouping_prompt(
        self,
        summaries: list[JsonObject],
        app_stubs: list[JsonObject],
    ) -> str:
        """
        Build the Gemini prompt used for email-to-application grouping.

        Inputs:
        - summaries: Normalized email summaries for the current batch.
        - app_stubs: Reduced view of existing applications available for matching.

        Outputs:
        - Prompt string for the Gemini grouping request.

        Edge cases:
        - Works with an empty existing-application list.

        Atomicity / concurrency:
        - Pure helper with no side effects.
        """
        return f"""You are a job application tracker assistant.

EXISTING APPLICATIONS (may be empty):
{json.dumps(app_stubs, indent=2)}

NEW EMAILS TO PROCESS:
{json.dumps(summaries, indent=2)}

KNOWN ATS DOMAINS (automated system emails, not humans): {", ".join(sorted(ATS_DOMAINS))}

TASK: For each email, decide:
- "ignore"          → newsletters, spam, unrelated, auto-confirmations with no useful info
- "match_existing"  → belongs to an existing application (match by company + role, fuzzy OK)
- "new_application" → a new job application starts here

MATCHING RULES:
1. Company + role match → same application (case/punctuation insensitive)
2. Same company, different role → separate application
3. Same company, role unclear, last_activity_date > 60 days ago → new application
4. ATS sender emails (greenhouse.io, lever.co etc.) → extract company from subject/body
5. Multiple emails for same NEW application in this batch → all share the same generated appl_id
6. For extracted.role, prefer the actual job title. Do NOT use process-stage labels such as
   "tech interview", "CEO interview", "phone screen", or "recruiter call" as the role unless
   the real title is truly unavailable.
7. Treat ATS vendor subdomains as ATS too. Example:
   - `tikalk.comeet-notifications.com` is ATS, not a human recruiter domain
   - when a sender has a `sender_company_hint`, use it only as a company hint and still verify
     against the subject/body before finalizing the company name
8. Do not infer the company from an agency/recruiter email domain when the message content or an
   ATS notification domain points to a different employer.
9. If the subject or body contains a concrete role title such as `Senior DevOps Engineer`, use it.
   Do not leave `role` as unknown when a specific job title is present.
10. For `match_existing`, `appl_id` must be one of the exact `appl_id` values from EXISTING
    APPLICATIONS above. If no existing application matches, use `new_application` instead.
11. Unsolicited recruiter outreach, sourcing, cold outreach, or job offers are NOT applications.
    Examples: recruiter says they found the candidate on LinkedIn, wants to share an opportunity,
    asks whether the candidate would be interested, or invites the candidate to apply. These must
    be `ignore` unless the email clearly refers to an application the candidate already submitted.
12. Respect the helper flags in each summary:
    - `looks_like_unsolicited_outreach: true` strongly favors `ignore`
    - `has_strong_application_signal: true` strongly favors `match_existing` or `new_application`

For each email return one object with:
  email_id      : string
  action        : "ignore" | "match_existing" | "new_application"
  appl_id        : string — existing appl_id if matching; for new_application use a SHORT 8-char id
                  (all emails in this batch for the SAME new application must share the same id)
  extracted:
    company     : string
    role        : string
    status      : "Applied"|"Screening"|"Interview"|"Assessment"|"Offer"|"Rejected"|null
    sender_name : string
    sender_email: string (parse properly from "Name <email>" format)
    is_ats      : boolean
    date_iso    : "YYYY-MM-DD" string

Return ONLY a valid JSON object: {{"results": [...]}}. No markdown, no prose."""

    def inspect_group_emails(
        self,
        new_emails: list[GmailMessage],
        existing_apps: list[ApplicationRecord],
    ) -> JsonObject:
        """
        Run the grouping prompt and return raw diagnostic data for inspection.

        Inputs:
        - new_emails: Gmail messages to classify.
        - existing_apps: Existing application rows available for matching.

        Outputs:
        - JSON-like diagnostic object containing summaries, raw model output, parsed results,
          and email-ID coverage information.

        Edge cases:
        - Empty batches still return a structured diagnostic payload.
        - Missing, duplicate, or unexpected `email_id` values are surfaced explicitly.

        Atomicity / concurrency:
        - Performs one outbound Gemini request when emails are present and otherwise has no side effects.
        """
        summaries = self._build_grouping_summaries(new_emails)
        app_stubs = self._build_existing_application_stubs(existing_apps)
        if not new_emails:
            return {
                "summaries": summaries,
                "app_stubs": app_stubs,
                "prompt": "",
                "raw_response": "",
                "results": [],
                "missing_email_ids": [],
                "duplicate_email_ids": [],
                "unexpected_email_ids": [],
            }

        prompt = self._build_grouping_prompt(summaries, app_stubs)
        raw_response = self._call(prompt, json_mode=True)
        normalized_raw_response = re.sub(
            r"^```(?:json)?\s*|\s*```$",
            "",
            raw_response,
            flags=re.MULTILINE,
        ).strip()
        parsed_results = cast(JsonObject, json.loads(normalized_raw_response))
        results = cast(list[JsonObject], parsed_results.get("results", []))

        expected_email_ids = [str(email_message.get("id", "")).strip() for email_message in new_emails]
        returned_email_ids = [
            str(result.get("email_id", "")).strip()
            for result in results
            if str(result.get("email_id", "")).strip()
        ]
        duplicate_email_ids = sorted({
            email_id
            for email_id in returned_email_ids
            if returned_email_ids.count(email_id) > 1
        })
        missing_email_ids = [
            email_id
            for email_id in expected_email_ids
            if email_id and email_id not in returned_email_ids
        ]
        unexpected_email_ids = [
            email_id
            for email_id in returned_email_ids
            if email_id not in expected_email_ids
        ]
        return {
            "summaries": summaries,
            "app_stubs": app_stubs,
            "prompt": prompt,
            "raw_response": normalized_raw_response,
            "results": results,
            "missing_email_ids": missing_email_ids,
            "duplicate_email_ids": duplicate_email_ids,
            "unexpected_email_ids": unexpected_email_ids,
        }

    def group_emails(
        self,
        new_emails: list[GmailMessage],
        existing_apps: list[ApplicationRecord],
    ) -> EmailGroupingResult:
        """
        Map a batch of new emails onto existing or newly created application records.

        Inputs:
        - new_emails: Gmail message dictionaries that have not yet been seen in Sheets.
        - existing_apps: Current application rows from Sheets used for fuzzy matching.

        Outputs:
        - EmailGroupingResult containing the unique application updates to write plus counts
          explaining how many emails were ignored, matched, or grouped into new applications.

        Edge cases:
        - Multiple emails may map to the same existing or newly created application.
        - Invalid matches to missing appl_ids are skipped from the returned updates.
        - Emails marked as "ignore" are counted but do not produce update rows.

        Atomicity / concurrency:
        - Pure transformation for the provided inputs aside from the outbound Gemini request.
        """
        if not new_emails:
            return EmailGroupingResult(
                updates=[],
                ignored_email_count=0,
                handled_message_ids=[],
                matched_existing_email_count=0,
                new_application_email_count=0,
                updated_existing_application_count=0,
                created_application_count=0,
            )
        inspection_result = self.inspect_group_emails(new_emails, existing_apps)
        results = cast(list[JsonObject], inspection_result.get("results", []))
        missing_email_ids = cast(list[str], inspection_result.get("missing_email_ids", []))
        duplicate_email_ids = cast(list[str], inspection_result.get("duplicate_email_ids", []))
        unexpected_email_ids = cast(list[str], inspection_result.get("unexpected_email_ids", []))
        if missing_email_ids or duplicate_email_ids or unexpected_email_ids:
            validation_problems: list[str] = []
            if missing_email_ids:
                validation_problems.append(
                    f"missing email_ids: {', '.join(missing_email_ids)}"
                )
            if duplicate_email_ids:
                validation_problems.append(
                    f"duplicate email_ids: {', '.join(duplicate_email_ids)}"
                )
            if unexpected_email_ids:
                validation_problems.append(
                    f"unexpected email_ids: {', '.join(unexpected_email_ids)}"
                )
            raise TrackerError(
                "AI grouping returned an invalid result set for this batch: "
                + "; ".join(validation_problems)
            )

        email_map = {e["id"]: e for e in new_emails}
        updates: dict[str, ApplicationRecord] = {}
        existing_apps_by_id = {
            app.get("appl_id", ""): app
            for app in existing_apps
            if app.get("appl_id")
        }

        ignored_email_count = 0
        handled_message_ids: list[str] = []
        matched_existing_email_count = 0
        new_application_email_count = 0
        updated_existing_application_ids: set[str] = set()
        created_application_ids: set[str] = set()

        for r in results:
            action = r.get("action")
            email_id = str(r.get("email_id", "")).strip()
            if action not in {"ignore", "match_existing", "new_application"}:
                continue
            if not email_id or email_id not in email_map:
                continue

            message_record = email_map[email_id]
            message_thread_id = str(message_record.get("thread_id", "") or "").strip()
            reply_to_email_address = resolve_message_reply_to_email(message_record)
            raw_sender_email_address = resolve_message_sender_email(message_record)
            raw_sender_email_domain = extract_email_domain(raw_sender_email_address)
            raw_from_email_address = extract_email_address(str(message_record.get("from", "")))
            raw_from_email_domain = extract_email_domain(raw_from_email_address)
            raw_sender_company_hint = infer_company_hint_from_sender_domain(raw_sender_email_domain)
            sender_header_company_hint = infer_company_hint_from_sender_header(
                str(message_record.get("from", "") or "")
            )
            deterministic_company_from_message = extract_company_name_from_email_message(message_record)
            deterministic_role_titles = extract_role_titles_from_email_message(message_record)
            deterministic_role_title = choose_preferred_role_title(*deterministic_role_titles)
            deterministic_status = extract_status_from_email_message(message_record)
            deterministic_sent_date_iso = extract_sent_date_from_email_message(message_record)
            deterministic_scheduled_interview_date = extract_scheduled_interview_date_from_email_message(
                message_record
            )
            deterministic_has_interview_schedule_signal = has_interview_schedule_signal(message_record)
            deterministic_has_strong_application_signal = has_strong_application_signal(message_record)
            deterministic_is_unsolicited_outreach = is_unsolicited_recruiter_outreach_email(message_record)
            deterministic_last_activity_date = choose_latest_activity_date(
                deterministic_sent_date_iso,
                deterministic_scheduled_interview_date if deterministic_status == "Interview" else "",
            )
            deterministic_company_name = (
                deterministic_company_from_message
                or
                sender_header_company_hint
                or raw_sender_company_hint.title()
                or ""
            )
            existing_thread_match_appl_id = find_existing_application_id_by_thread_id(
                existing_apps_by_id,
                message_thread_id,
            )
            if action == "ignore" and existing_thread_match_appl_id:
                action = "match_existing"
                r["appl_id"] = existing_thread_match_appl_id
                extracted_payload = cast(JsonObject, r.setdefault("extracted", {}))
                existing_thread_match = existing_apps_by_id.get(existing_thread_match_appl_id, {})
                if not str(extracted_payload.get("company", "") or "").strip():
                    extracted_payload["company"] = (
                        deterministic_company_name
                        or str(existing_thread_match.get("company", "") or "").strip()
                    )
                if not str(extracted_payload.get("role", "") or "").strip() and deterministic_role_title:
                    extracted_payload["role"] = deterministic_role_title
                if not str(extracted_payload.get("status", "") or "").strip() and deterministic_status:
                    extracted_payload["status"] = deterministic_status
            should_rescue_ignored_email = (
                action == "ignore"
                and is_ats_sender_domain(raw_sender_email_domain)
                and bool(deterministic_company_name)
                and bool(deterministic_role_title or deterministic_status)
            )
            if should_rescue_ignored_email:
                action = "new_application"
                extracted_payload = cast(JsonObject, r.setdefault("extracted", {}))
                if not str(extracted_payload.get("company", "") or "").strip():
                    extracted_payload["company"] = deterministic_company_name
                if not str(extracted_payload.get("role", "") or "").strip() and deterministic_role_title:
                    extracted_payload["role"] = deterministic_role_title
                if not str(extracted_payload.get("status", "") or "").strip() and deterministic_status:
                    extracted_payload["status"] = deterministic_status
                if not str(extracted_payload.get("sender_email", "") or "").strip() and raw_sender_email_address:
                    extracted_payload["sender_email"] = raw_sender_email_address
                extracted_payload["is_ats"] = True
                if not str(extracted_payload.get("date_iso", "") or "").strip() and deterministic_sent_date_iso:
                    extracted_payload["date_iso"] = deterministic_sent_date_iso
                if not str(r.get("appl_id", "") or "").strip():
                    extracted_date = str(extracted_payload.get("date_iso", "") or "").strip()
                    r["appl_id"] = build_deterministic_appl_id(
                        company_name=str(extracted_payload.get("company", "") or deterministic_company_name),
                        role_title=str(extracted_payload.get("role", "") or deterministic_role_title),
                        applied_date=extracted_date,
                    )

            if (
                action == "new_application"
                and deterministic_is_unsolicited_outreach
                and not deterministic_has_strong_application_signal
            ):
                action = "ignore"

            if action == "ignore":
                handled_message_ids.append(email_id)
                ignored_email_count += 1
                continue
            eid = email_id
            ex = cast(JsonObject, r.get("extracted", {}))
            raw_appl_id = str(r.get("appl_id", "") or "").strip()
            aid = raw_appl_id or str(uuid.uuid4())[:8]
            sender_email_address = (
                choose_preferred_replyable_email(
                    str(ex.get("sender_email", "") or ""),
                    raw_sender_email_address,
                )
                or extract_email_address(str(ex.get("sender_email", "")))
                or raw_sender_email_address
            )
            sender_is_user_owned = self._is_user_owned_sender(sender_email_address)
            usable_reply_to_is_distinct = (
                bool(reply_to_email_address)
                and reply_to_email_address != raw_from_email_address
            )
            sender_is_ats = bool(ex.get("is_ats")) or is_ats_sender_domain(raw_from_email_domain)
            sender_is_usable_contact = (
                bool(sender_email_address)
                and not sender_is_user_owned
                and not is_unusable_outbound_email(sender_email_address)
            )
            internet_message_id = str(
                message_record.get("internet_message_id", "") or ""
            ).strip()
            deterministic_status = extract_status_from_email_message(message_record)
            ai_extracted_status = str(ex.get("status") or "").strip()
            if sender_is_ats and deterministic_status:
                extracted_status = deterministic_status
            else:
                extracted_status = ai_extracted_status or deterministic_status or "Applied"
                if (
                    deterministic_status
                    and STATUS_RANK.get(deterministic_status, 0) > STATUS_RANK.get(extracted_status, 0)
                ):
                    extracted_status = deterministic_status
            extracted_applied_date = (
                str(ex.get("date_iso", "") or "").strip()
                or deterministic_sent_date_iso
            )
            extracted_last_activity_date = choose_latest_activity_date(
                extracted_applied_date,
                deterministic_scheduled_interview_date if extracted_status == "Interview" else "",
                deterministic_last_activity_date,
            )
            if (
                extracted_status == "Interview"
                and deterministic_has_interview_schedule_signal
                and not deterministic_scheduled_interview_date
            ):
                extracted_last_activity_date = extracted_applied_date or extracted_last_activity_date
            extracted_company_name = str(ex.get("company", "") or "").strip()
            if normalize_matching_text(extracted_company_name) in {"", "unknown"}:
                extracted_company_name = deterministic_company_name or raw_sender_company_hint.title()
            ai_extracted_role_name = str(ex.get("role", "") or "").strip()
            normalized_ai_extracted_role_name = normalize_role_candidate_text(ai_extracted_role_name)
            if sender_is_ats and not normalized_ai_extracted_role_name:
                extracted_role_name = deterministic_role_title
            else:
                extracted_role_name = normalized_ai_extracted_role_name or deterministic_role_title
            if not raw_appl_id and action == "new_application":
                aid = build_deterministic_appl_id(
                    company_name=extracted_company_name,
                    role_title=extracted_role_name,
                    applied_date=extracted_applied_date,
                )
            extracted_application: ApplicationRecord = {
                "appl_id": aid,
                "company": extracted_company_name,
                "role": extracted_role_name,
                "status": extracted_status,
                "applied_date": extracted_applied_date,
                "last_activity_date": extracted_last_activity_date,
            }

            strongest_match_score = 0
            strongest_match_appl_id: Optional[str] = None
            if existing_thread_match_appl_id:
                strongest_match_appl_id = existing_thread_match_appl_id
                strongest_match_score = max(STATUS_RANK.values(), default=0) + 1
            for existing_appl_id, existing_app in existing_apps_by_id.items():
                merge_strength = get_application_merge_strength(existing_app, extracted_application)
                if merge_strength > strongest_match_score:
                    strongest_match_score = merge_strength
                    strongest_match_appl_id = existing_appl_id

            for update_appl_id, pending_update in updates.items():
                merge_strength = get_application_merge_strength(pending_update, extracted_application)
                if merge_strength > strongest_match_score:
                    strongest_match_score = merge_strength
                    strongest_match_appl_id = update_appl_id

            if strongest_match_appl_id:
                aid = strongest_match_appl_id
                if aid in existing_apps_by_id:
                    action = "match_existing"
                else:
                    action = "new_application"
            elif action == "match_existing":
                ai_selected_existing_app = existing_apps_by_id.get(aid)
                if ai_selected_existing_app is None:
                    action = "new_application"
                else:
                    ai_selected_merge_strength = get_application_merge_strength(
                        ai_selected_existing_app,
                        extracted_application,
                    )
                    if ai_selected_merge_strength <= 0:
                        action = "new_application"

            if action == "match_existing":
                if aid in updates:
                    base = updates[aid]
                else:
                    base = next((dict(a) for a in existing_apps if a.get("appl_id") == aid), None)
                if not base:
                    raise TrackerError(
                        "AI grouping matched email "
                        f"{email_id} to non-existent appl_id '{aid}'. "
                        "Re-run with --debug-message-ids or --debug-app-emails to inspect the raw model output."
                    )
                handled_message_ids.append(email_id)
                matched_existing_email_count += 1
                updated_existing_application_ids.add(aid)
                # Advance status only, never go backwards
                new_st = extracted_status
                if new_st and STATUS_RANK.get(new_st, 0) > STATUS_RANK.get(base.get("status", "Applied"), 0):
                    base["status"] = new_st
                if extracted_status == "Rejected":
                    base["deferred_until"] = ""
                extracted_role_title = extracted_role_name
                preferred_role_title = choose_preferred_role_title(
                    str(base.get("role", "") or "").strip(),
                    extracted_role_title,
                )
                if preferred_role_title:
                    base["role"] = preferred_role_title
                if extracted_status == "Interview" and deterministic_has_interview_schedule_signal:
                    base["last_activity_date"] = (
                        extracted_last_activity_date
                        or str(base.get("last_activity_date", "") or "").strip()
                    )
                else:
                    base["last_activity_date"] = choose_latest_activity_date(
                        str(base.get("last_activity_date", "") or "").strip(),
                        extracted_last_activity_date,
                    ) or base.get("last_activity_date")
                extracted_sender_name = str(ex.get("sender_name", "") or "").strip()
                sanitized_sender_name = (
                    ""
                    if is_unusable_recruiter_name(extracted_sender_name)
                    else extracted_sender_name
                )
                if (usable_reply_to_is_distinct and sender_is_usable_contact) or (
                    not sender_is_ats and sender_is_usable_contact
                ):
                    base["recruiter_email"] = sender_email_address
                    if sanitized_sender_name:
                        base["recruiter_name"] = sanitized_sender_name
                    base["contact_email"]   = sender_email_address
                elif sender_is_ats and sender_email_address:
                    base["ats_email"] = sender_email_address
                ids = cast(list[str], safe_json_loads(
                    base.get("email_ids") or "[]",
                    f"email_ids for appl_id {aid}",
                    [],
                ))
                if eid not in ids:
                    ids.append(eid)
                base["email_ids"] = json.dumps(ids)

                thread_ids = cast(list[str], safe_json_loads(
                    base.get("thread_ids") or "[]",
                    f"thread_ids for appl_id {aid}",
                    [],
                ))
                if message_thread_id and message_thread_id not in thread_ids:
                    thread_ids.append(message_thread_id)
                base["thread_ids"] = json.dumps(thread_ids)

                internet_message_ids = cast(list[str], safe_json_loads(
                    base.get("internet_message_ids") or "[]",
                    f"internet_message_ids for appl_id {aid}",
                    [],
                ))
                if internet_message_id and internet_message_id not in internet_message_ids:
                    internet_message_ids.append(internet_message_id)
                base["internet_message_ids"] = json.dumps(internet_message_ids)
                updates[aid] = base

            elif action == "new_application":
                handled_message_ids.append(email_id)
                new_application_email_count += 1
                created_application_ids.add(aid)
                if aid in updates:
                    # Merge additional email into the same new record being built
                    existing_new = updates[aid]
                    ids = cast(list[str], safe_json_loads(
                        existing_new.get("email_ids") or "[]",
                        f"email_ids for appl_id {aid}",
                        [],
                    ))
                    if eid not in ids:
                        ids.append(eid)
                    existing_new["email_ids"] = json.dumps(ids)

                    thread_ids = cast(list[str], safe_json_loads(
                        existing_new.get("thread_ids") or "[]",
                        f"thread_ids for appl_id {aid}",
                        [],
                    ))
                    if message_thread_id and message_thread_id not in thread_ids:
                        thread_ids.append(message_thread_id)
                    existing_new["thread_ids"] = json.dumps(thread_ids)

                    internet_message_ids = cast(list[str], safe_json_loads(
                        existing_new.get("internet_message_ids") or "[]",
                        f"internet_message_ids for appl_id {aid}",
                        [],
                    ))
                    if internet_message_id and internet_message_id not in internet_message_ids:
                        internet_message_ids.append(internet_message_id)
                    existing_new["internet_message_ids"] = json.dumps(internet_message_ids)
                    extracted_role_title = extracted_role_name
                    preferred_role_title = choose_preferred_role_title(
                        str(existing_new.get("role", "") or "").strip(),
                        extracted_role_title,
                    )
                    if preferred_role_title:
                        existing_new["role"] = preferred_role_title
                    if extracted_status == "Interview" and deterministic_has_interview_schedule_signal:
                        existing_new["last_activity_date"] = (
                            extracted_last_activity_date
                            or existing_new.get("last_activity_date", "")
                        )
                    else:
                        existing_new["last_activity_date"] = choose_latest_activity_date(
                            str(existing_new.get("last_activity_date", "") or "").strip(),
                            extracted_last_activity_date,
                        ) or existing_new.get("last_activity_date", "")
                    # Update contact if human sender
                    extracted_sender_name = str(ex.get("sender_name", "") or "").strip()
                    sanitized_sender_name = (
                        ""
                        if is_unusable_recruiter_name(extracted_sender_name)
                        else extracted_sender_name
                    )
                    if (
                        ((usable_reply_to_is_distinct and sender_is_usable_contact) or (
                            not sender_is_ats and sender_is_usable_contact
                        ))
                        and not existing_new.get("recruiter_email")
                    ):
                        existing_new["recruiter_email"] = sender_email_address
                        existing_new["recruiter_name"]  = sanitized_sender_name
                        existing_new["contact_email"]   = sender_email_address
                else:
                    is_ats = sender_is_ats
                    extracted_sender_name = str(ex.get("sender_name", "") or "").strip()
                    sanitized_sender_name = (
                        ""
                        if is_unusable_recruiter_name(extracted_sender_name)
                        else extracted_sender_name
                    )
                    recruiter_name = ""
                    recruiter_email = ""
                    ats_email = ""
                    contact_email = ""
                    if usable_reply_to_is_distinct and sender_is_usable_contact:
                        recruiter_name = sanitized_sender_name
                        recruiter_email = sender_email_address
                        contact_email = sender_email_address
                    elif is_ats and sender_email_address:
                        ats_email = sender_email_address
                    elif sender_is_usable_contact:
                        recruiter_name = sanitized_sender_name
                        recruiter_email = sender_email_address
                        contact_email = sender_email_address

                    updates[aid] = {
                        "appl_id":               aid,
                        "company":              extracted_company_name or "Unknown",
                        "role":                 extracted_role_name or "Unknown",
                        "status":               extracted_status,
                        "source":               "email",
                        "applied_date":         extracted_applied_date,
                        "last_activity_date":   extracted_last_activity_date,
                        "recruiter_name":       recruiter_name,
                        "recruiter_email":      recruiter_email,
                        "ats_email":            ats_email,
                        "contact_email":        contact_email,
                        "follow_up_sent_date":  "",
                        "follow_up_count":      "0",
                        "withdrawal_sent_date": "",
                        "follow_up_opt_out":    "",
                        "withdraw_in_next_digest": "",
                        "notes":                "",
                        "linkedin_contact":     "",
                        "email_ids":            json.dumps([eid]),
                        "thread_ids":           json.dumps([message_thread_id]) if message_thread_id else "[]",
                        "internet_message_ids": json.dumps([internet_message_id]) if internet_message_id else "[]",
                        "gmail_review_url":     "",
                        "draft_id":             "",
                    }

        return EmailGroupingResult(
            updates=list(updates.values()),
            ignored_email_count=ignored_email_count,
            handled_message_ids=deduplicate_preserving_order(handled_message_ids),
            matched_existing_email_count=matched_existing_email_count,
            new_application_email_count=new_application_email_count,
            updated_existing_application_count=len(updated_existing_application_ids),
            created_application_count=len(created_application_ids),
        )

    def generate_follow_up(
        self,
        app: ApplicationRecord,
        user_name: str,
        follow_up_n: int,
    ) -> tuple[str, str]:
        """Returns (subject, body). Uses template if available, Gemini as fallback."""
        company_display_name = self._resolve_outreach_company_name(app)
        resolved_target_email = resolve_outbound_target_email(app, "follow_up")
        salutation_name = self._resolve_outreach_salutation_name(
            app,
            company_display_name,
            resolved_target_email,
        )
        variables: dict[str, str] = {
            "user_name":       user_name,
            "company":         company_display_name,
            "role":            app.get("role", ""),
            "recruiter_name":  app.get("recruiter_name", ""),
            "salutation_name": salutation_name,
        }
        result = render_template(_resolve_follow_up_template(self._config, follow_up_n), variables)
        if result:
            return result

        prompt = f"""Write a professional, concise follow-up email for a job application.
Applicant: {user_name}
Position: {app.get('role')} at {company_display_name}
Follow-up number: {follow_up_n}
Return format — first line: Subject: <subject line>, then a blank line, then the body.
No filler openers like "I hope this email finds you well"."""
        return self._split_subject_body(self._call(prompt, json_mode=False))

    def generate_withdrawal(self, app: ApplicationRecord, user_name: str) -> tuple[str, str]:
        """Returns (subject, body). Uses template if available, Gemini as fallback."""
        company_display_name = self._resolve_outreach_company_name(app)
        resolved_target_email = resolve_outbound_target_email(app, "withdraw")
        salutation_name = self._resolve_outreach_salutation_name(
            app,
            company_display_name,
            resolved_target_email,
        )
        variables: dict[str, str] = {
            "user_name":       user_name,
            "company":         company_display_name,
            "role":            app.get("role", ""),
            "recruiter_name":  app.get("recruiter_name", ""),
            "salutation_name": salutation_name,
        }
        result = render_template(_resolve_template(self._config, "withdrawal"), variables)
        if result:
            return result

        prompt = f"""Write a professional email withdrawing a job application and requesting GDPR data deletion.
Applicant: {user_name}
Position: {app.get('role')} at {company_display_name}
Return format — first line: Subject: <subject line>, then a blank line, then the body."""
        return self._split_subject_body(self._call(prompt, json_mode=False))

    def generate_deletion_request(self, app: ApplicationRecord, user_name: str) -> tuple[str, str]:
        """Returns (subject, body). Uses template if available, Gemini as fallback."""
        company_display_name = self._resolve_outreach_company_name(app)
        resolved_target_email = resolve_outbound_target_email(app, "deletion_request")
        salutation_name = self._resolve_outreach_salutation_name(
            app,
            company_display_name,
            resolved_target_email,
        )
        variables: dict[str, str] = {
            "user_name":       user_name,
            "company":         company_display_name,
            "role":            app.get("role", ""),
            "recruiter_name":  app.get("recruiter_name", ""),
            "salutation_name": salutation_name,
        }
        result = render_template(_resolve_template(self._config, "deletion_request"), variables)
        if result:
            return result

        prompt = f"""Write a professional email requesting GDPR / privacy data deletion after a rejected job application.
Applicant: {user_name}
Position: {app.get('role')} at {company_display_name}
Return format — first line: Subject: <subject line>, then a blank line, then the body."""
        return self._split_subject_body(self._call(prompt, json_mode=False))

    def _resolve_outreach_company_name(self, app: ApplicationRecord) -> str:
        """
        Prefer the stored company name unless it only repeats the application source platform.
        When that happens, recover a better employer label from resolved contact domains.
        """
        company_name = str(app.get("company", "") or "").strip()
        source_name = str(app.get("source", "") or "").strip()
        normalized_company_name = normalize_matching_text(company_name)
        normalized_source_name = normalize_matching_text(source_name)

        if company_name and normalized_company_name != normalized_source_name:
            return company_name

        for field_name in ("contact_email", "recruiter_email", "ats_email"):
            email_address = extract_email_address(str(app.get(field_name, "") or ""))
            email_domain = get_base_domain(extract_email_domain(email_address))
            if (
                not email_domain
                or email_domain in SOCIAL_RELAY_DOMAINS
                or is_linkedin_noise_domain(email_domain)
                or is_trusted_contact_source_domain(email_domain)
            ):
                continue

            candidate_label = get_primary_domain_label(email_domain)
            if candidate_label and candidate_label != normalized_source_name:
                return candidate_label.title()

        return company_name or "the company"

    def _resolve_outreach_salutation_name(
        self,
        app: ApplicationRecord,
        company_display_name: str,
        resolved_target_email: str = "",
    ) -> str:
        recruiter_name = str(app.get("recruiter_name", "") or "").strip()
        recruiter_email = extract_email_address(str(app.get("recruiter_email", "") or ""))
        normalized_target_email = extract_email_address(resolved_target_email)
        if (
            recruiter_name
            and not is_unusable_recruiter_name(recruiter_name)
            and recruiter_email
            and normalized_target_email == recruiter_email
        ):
            return recruiter_name.split()[0]
        if company_display_name and company_display_name != "the company":
            return f"{company_display_name} Hiring Team"
        return "Hiring Team"

    @staticmethod
    def _split_subject_body(text: str) -> tuple[str, str]:
        """Parse 'Subject: ...\n\nbody' from a raw Gemini response."""
        lines = text.strip().splitlines()
        if lines and lines[0].lower().startswith("subject:"):
            subject = lines[0][len("subject:"):].strip()
            body    = "\n".join(lines[2:] if len(lines) > 1 and lines[1].strip() == "" else lines[1:]).strip()
            return subject, body
        return "Application Update", text.strip()

    def find_privacy_email(self, company: str) -> Optional[str]:
        """Ask the model if it knows the company's privacy/GDPR contact email."""
        prompt = f"""What is the data deletion / GDPR / privacy request email address for the company "{company}"?
Common patterns: privacy@, dpo@, dataprivacy@, gdpr@, legal@

If you are confident you know it, return ONLY the email address (lowercase).
If you are not sure, return exactly: unknown"""
        val = self._call(prompt, json_mode=False).strip().lower()
        if "@" in val and "unknown" not in val and len(val) < 100:
            return val
        return None

    def find_follow_up_email(self, company: str) -> Optional[str]:
        """Ask the model for a recruiter / HR / talent contact email suitable for follow-ups."""
        prompt = f"""What is the best email address to send a job-application follow-up to for the company "{company}"?
Prefer recruiter, recruiting, talent, careers, jobs, people, hiring, or HR inboxes.
Do NOT return privacy / GDPR / legal-only addresses unless there is truly no better hiring-related option.
Do NOT return no-reply / noreply / notification / scheduler style addresses.

If you are confident you know it, return ONLY the email address (lowercase).
If you are not sure, return exactly: unknown"""
        val = self._call(prompt, json_mode=False).strip().lower()
        if "@" in val and "unknown" not in val and len(val) < 100:
            return val
        return None


# ── Follow-up Engine ───────────────────────────────────────────────────────────

class FollowUpEngine:
    def __init__(self, config: ConfigDict):
        t = config["thresholds"]
        self.follow_up_days        = t["follow_up_days"]
        self.withdraw_days         = t["withdraw_days"]
        self.follow_up_repeat_days = t.get("follow_up_repeat_days", t["follow_up_days"])
        self.deletion_request_days = t.get("deletion_request_days", 0)

    def _get_withdraw_reference_date(self, app: ApplicationRecord) -> Optional[datetime.date]:
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

    def compute_actions(
        self,
        applications: list[ApplicationRecord],
    ) -> list[PendingActionRecord]:
        today   = datetime.now(timezone.utc).date()
        actions: list[PendingActionRecord] = []

        for app in applications:
            status = app.get("status", "Applied")
            manual_withdraw_requested = is_truthy_sheet_value(app.get("withdraw_in_next_digest"))

            if status in ("Offer", "Withdrawn", "Paused"):
                continue

            withdrawal_already_sent = bool(app.get("withdrawal_sent_date"))
            if withdrawal_already_sent:
                continue

            if manual_withdraw_requested:
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

            follow_up_opted_out = is_truthy_sheet_value(app.get("follow_up_opt_out"))
            deletion_request_already_sent = bool(app.get("deletion_request_sent_date"))
            deletion_request_opted_out = is_truthy_sheet_value(app.get("deletion_request_opt_out"))

            if status == "Rejected":
                if deletion_request_already_sent or deletion_request_opted_out:
                    continue
                if days >= self.deletion_request_days:
                    actions.append({
                        "type": "deletion_request",
                        "app": dict(app),
                        "reason": f"Rejected — {days}d since last activity",
                    })
                continue

            # Withdraw threshold — don't auto-withdraw if actively in Interview/Assessment
            if days >= self.withdraw_days and status not in ("Interview", "Assessment"):
                actions.append({
                    "type": "withdraw",
                    "app": dict(app),
                    "reason": f"Ghosted — {days}d since last activity",
                })
                continue

            # Follow-up threshold
            if days >= self.follow_up_days and not follow_up_opted_out:
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
                actions.append({"type": "follow_up", "app": dict(app),
                                 "reason": f"{days}d inactive — follow-up #{follow_up_n}",
                                 "follow_up_n": follow_up_n})

        return actions


# ── Tracker Orchestrator ───────────────────────────────────────────────────────

class Tracker:
    MANAGE_ACTION_ALIASES = {
        "d": "defer",
        "defer": "defer",
        "p": "pause",
        "pause": "pause",
        "r": "resume",
        "resume": "resume",
        "e": "email",
        "email": "email",
        "o": "policy",
        "policy": "policy",
        "policies": "policy",
        "optout": "policy",
        "opt-out": "policy",
        "w": "withdraw",
        "withdraw": "withdraw",
        "c": "exit",
        "cancel": "exit",
        "exit": "exit",
    }

    def __init__(self):
        self.cfg    = load_config()
        self.processing_labels = GmailProcessingLabelConfig.from_config(self.cfg)
        self.processed_message_state = ProcessedMessageStateStore(PROCESSED_GMAIL_STATE_PATH)
        self.gmail  = GmailClient(self.cfg)
        self.sheets = SheetsClient(self.cfg)
        self.ai     = AIGrouper(self.cfg)
        self.engine = FollowUpEngine(self.cfg)

    def _grouping_run_state_dir(self) -> Path:
        return cast(Path, getattr(self, "grouping_run_state_dir", GROUPING_RUNS_DIR))

    def _grouping_run_state_path(self, run_id: str) -> Path:
        return self._grouping_run_state_dir() / f"{run_id}.json"

    @staticmethod
    def _new_grouping_run_id() -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%S")

    def _save_grouping_run_state(self, run_state: JsonObject) -> Path:
        run_id = str(run_state.get("run_id", "") or "").strip()
        if not run_id:
            raise TrackerError("Could not save grouping run state because run_id was missing")
        state_path = self._grouping_run_state_path(run_id)
        try:
            state_path.parent.mkdir(parents=True, exist_ok=True)
            state_path.write_text(
                json.dumps(run_state, indent=2, sort_keys=True),
                encoding="utf-8",
            )
        except OSError as error:
            raise TrackerError(f"Could not write grouping run state to {state_path}: {error}") from error
        return state_path

    def _load_grouping_run_state(self, run_id: str) -> JsonObject:
        normalized_run_id = str(run_id or "").strip()
        if not normalized_run_id:
            raise TrackerError("resume-run requires a non-empty saved run ID")

        state_path = self._grouping_run_state_path(normalized_run_id)
        if not state_path.exists():
            raise TrackerError(f"Saved grouping run '{normalized_run_id}' was not found at {state_path}")

        try:
            payload = cast(JsonObject, json.loads(state_path.read_text(encoding="utf-8")))
        except (OSError, json.JSONDecodeError) as error:
            raise TrackerError(f"Could not load saved grouping run '{normalized_run_id}': {error}") from error
        return payload

    def _delete_grouping_run_state(self, run_id: str) -> None:
        state_path = self._grouping_run_state_path(run_id)
        try:
            if state_path.exists():
                state_path.unlink()
        except OSError as error:
            raise TrackerError(f"Could not delete saved grouping run state {state_path}: {error}") from error

    @staticmethod
    def _merge_updates_into_working_existing(
        working_existing: list[ApplicationRecord],
        updates: list[ApplicationRecord],
    ) -> list[ApplicationRecord]:
        merged_existing = copy.deepcopy(working_existing)
        for update in updates:
            appl_id = str(update.get("appl_id", "") or "").strip()
            if not appl_id:
                continue
            existing_index = next(
                (
                    index
                    for index, existing_application in enumerate(merged_existing)
                    if existing_application.get("appl_id") == appl_id
                ),
                None,
            )
            if existing_index is not None:
                merged_existing[existing_index] = copy.deepcopy(update)
            else:
                merged_existing.append(copy.deepcopy(update))
        return merged_existing

    def _build_grouping_clusters(
        self,
        new_emails: list[GmailMessage],
        existing_apps: list[ApplicationRecord],
        transport_window_size: int,
        cluster_size_limit: int,
        cluster_emergency_margin: int,
    ) -> list[JsonObject]:
        """
        Partition new Gmail messages into merge-biased AI grouping clusters.

        Inputs:
        - new_emails: Sorted Gmail messages that still need AI grouping.
        - existing_apps: Existing application rows used to seed thread-based matching hints.
        - transport_window_size: Maximum number of raw inbox messages considered together when
          planning clusters.
        - cluster_size_limit: Normal maximum cluster size passed to Gemini in one call.
        - cluster_emergency_margin: Extra overflow slots allowed for strong same-application matches.

        Outputs:
        - Serializable cluster plan entries with status metadata for saved-run persistence.

        Edge cases:
        - Empty input returns an empty list.
        - Ambiguous emails prefer merging into the best existing cluster instead of splitting.
        - Strong same-thread/application matches may overflow the limit by up to the configured
          emergency margin.

        Atomicity / concurrency:
        - Pure in-memory planning with no external side effects.
        """
        if not new_emails:
            return []

        normalized_window_size = max(1, int(transport_window_size))
        normalized_cluster_size_limit = max(1, int(cluster_size_limit))
        normalized_cluster_emergency_margin = max(0, int(cluster_emergency_margin))
        existing_apps_by_id = {
            str(app.get("appl_id", "") or "").strip(): app
            for app in existing_apps
            if str(app.get("appl_id", "") or "").strip()
        }

        def build_email_hints(email_message: GmailMessage) -> JsonObject:
            sender_email_address = resolve_message_sender_email(email_message)
            sender_domain = extract_email_domain(sender_email_address)
            company_hint = normalize_matching_text(
                extract_company_name_from_email_message(email_message)
                or infer_company_hint_from_sender_header(str(email_message.get("from", "") or ""))
                or infer_company_hint_from_sender_domain(sender_domain)
            )
            role_hint = normalize_matching_text(
                choose_preferred_role_title(*extract_role_titles_from_email_message(email_message))
            )
            existing_appl_id_hint = find_existing_application_id_by_thread_id(
                existing_apps_by_id,
                str(email_message.get("thread_id", "") or "").strip(),
            )
            return {
                "email_id": str(email_message.get("id", "") or "").strip(),
                "thread_id": str(email_message.get("thread_id", "") or "").strip(),
                "sender_email": sender_email_address,
                "sender_domain": sender_domain,
                "company_hint": company_hint,
                "role_hint": role_hint,
                "existing_appl_id_hint": existing_appl_id_hint,
                "has_strong_application_signal": has_strong_application_signal(email_message),
            }

        def score_email_against_cluster(email_hints: JsonObject, cluster_state: JsonObject) -> int:
            score = 0
            existing_appl_id_hint = str(email_hints.get("existing_appl_id_hint", "") or "").strip()
            thread_id = str(email_hints.get("thread_id", "") or "").strip()
            sender_email = str(email_hints.get("sender_email", "") or "").strip()
            sender_domain = str(email_hints.get("sender_domain", "") or "").strip()
            company_hint = str(email_hints.get("company_hint", "") or "").strip()
            role_hint = str(email_hints.get("role_hint", "") or "").strip()

            if existing_appl_id_hint and existing_appl_id_hint in cast(set[str], cluster_state["existing_appl_id_hints"]):
                score += 12
            if thread_id and thread_id in cast(set[str], cluster_state["thread_ids"]):
                score += 10
            if sender_email and sender_email in cast(set[str], cluster_state["sender_emails"]):
                score += 4
            if sender_domain and sender_domain in cast(set[str], cluster_state["sender_domains"]):
                score += 2
            if company_hint and company_hint in cast(set[str], cluster_state["company_hints"]):
                score += 5
            if role_hint and role_hint in cast(set[str], cluster_state["role_hints"]):
                score += 4
            if (
                company_hint
                and role_hint
                and company_hint in cast(set[str], cluster_state["company_hints"])
                and role_hint in cast(set[str], cluster_state["role_hints"])
            ):
                score += 3
            if (
                bool(email_hints.get("has_strong_application_signal"))
                and bool(cluster_state.get("has_strong_application_signal"))
            ):
                score += 1
            return score

        def add_email_to_cluster(cluster_state: JsonObject, email_hints: JsonObject) -> None:
            email_id = str(email_hints.get("email_id", "") or "").strip()
            if email_id:
                cast(list[str], cluster_state["email_ids"]).append(email_id)
            for field_name in (
                "thread_id",
                "sender_email",
                "sender_domain",
                "company_hint",
                "role_hint",
                "existing_appl_id_hint",
            ):
                value = str(email_hints.get(field_name, "") or "").strip()
                if not value:
                    continue
                target_key = f"{field_name}s"
                if field_name == "existing_appl_id_hint":
                    target_key = "existing_appl_id_hints"
                cast(set[str], cluster_state[target_key]).add(value)
            if bool(email_hints.get("has_strong_application_signal")):
                cluster_state["has_strong_application_signal"] = True

        planned_clusters: list[JsonObject] = []
        cluster_counter = 0
        total_window_count = -(-len(new_emails) // normalized_window_size)

        for window_start in range(0, len(new_emails), normalized_window_size):
            window_emails = new_emails[window_start : window_start + normalized_window_size]
            window_number = window_start // normalized_window_size + 1
            window_clusters: list[JsonObject] = []

            for window_email in window_emails:
                email_hints = build_email_hints(window_email)
                best_cluster: Optional[JsonObject] = None
                best_score = -1
                for cluster_state in window_clusters:
                    candidate_score = score_email_against_cluster(email_hints, cluster_state)
                    if candidate_score > best_score:
                        best_score = candidate_score
                        best_cluster = cluster_state

                should_merge = best_cluster is not None and best_score >= 4
                if should_merge and best_cluster is not None:
                    current_cluster_size = len(cast(list[str], best_cluster["email_ids"]))
                    if current_cluster_size >= normalized_cluster_size_limit:
                        can_use_emergency_overflow = (
                            best_score >= 10
                            and current_cluster_size
                            < normalized_cluster_size_limit + normalized_cluster_emergency_margin
                        )
                        if can_use_emergency_overflow:
                            best_cluster["emergency_overflow_used"] = True
                            add_email_to_cluster(best_cluster, email_hints)
                            continue
                        should_merge = False
                    else:
                        add_email_to_cluster(best_cluster, email_hints)
                        continue

                if not should_merge:
                    new_cluster_state: JsonObject = {
                        "email_ids": [],
                        "thread_ids": set(),
                        "sender_emails": set(),
                        "sender_domains": set(),
                        "company_hints": set(),
                        "role_hints": set(),
                        "existing_appl_id_hints": set(),
                        "has_strong_application_signal": False,
                        "emergency_overflow_used": False,
                    }
                    add_email_to_cluster(new_cluster_state, email_hints)
                    window_clusters.append(new_cluster_state)

            for window_cluster in window_clusters:
                cluster_counter += 1
                cluster_email_ids = deduplicate_preserving_order(
                    cast(list[str], window_cluster.get("email_ids", []))
                )
                planned_clusters.append({
                    "cluster_id": f"cluster-{cluster_counter:03d}",
                    "status": "pending",
                    "error": "",
                    "completed_at": "",
                    "failed_at": "",
                    "transport_window_number": window_number,
                    "transport_window_total": total_window_count,
                    "email_ids": cluster_email_ids,
                    "email_count": len(cluster_email_ids),
                    "emergency_overflow_used": bool(window_cluster.get("emergency_overflow_used")),
                })

        return planned_clusters

    def _describe_grouping_failure_summary(self, failure_reason: str) -> str:
        normalized_failure_reason = str(failure_reason or "").strip()
        if is_json_parse_error_text(normalized_failure_reason):
            return "Gemini returned malformed JSON while classifying new Gmail messages."
        if "quota" in normalized_failure_reason.lower() or "rate limit" in normalized_failure_reason.lower():
            return "Gemini rate limited while classifying new Gmail messages."
        return "Gemini request failed while classifying new Gmail messages."

    def _format_grouping_failure_message(
        self,
        *,
        failure_reason: str,
        cluster_number: int,
        total_cluster_count: int,
        cluster_email_count: int,
        run_state_path: Path,
        run_id: str,
    ) -> str:
        detail_label = "Parse error" if is_json_parse_error_text(failure_reason) else "Failure"
        return (
            f"AI grouping failed — {self._describe_grouping_failure_summary(failure_reason)}\n"
            f"Cluster: {cluster_number} / {total_cluster_count}\n"
            f"Emails in cluster: {cluster_email_count}\n"
            f"Run state saved to: {run_state_path}\n"
            f"{detail_label}: {failure_reason or 'Unknown Gemini failure'}\n"
            "Sync aborted before updating Sheets.\n"
            "Digest will not continue because actions could be stale.\n"
            "\n"
            "Recovery options:\n"
            "  1. Rerun sync from scratch:\n"
            "     python tracker.py --sync\n"
            "  2. Resume this saved run:\n"
            f"     python tracker.py --resume-run {run_id}"
        )

    def _apply_grouping_result_to_run_state(
        self,
        run_state: JsonObject,
        grouping_result: EmailGroupingResult,
    ) -> None:
        initial_existing_appl_ids = set(cast(list[str], run_state.get("initial_existing_appl_ids", [])))
        run_state["ignored_email_count"] = int(run_state.get("ignored_email_count", 0)) + grouping_result.ignored_email_count
        handled_message_ids = cast(list[str], run_state.get("handled_message_ids", []))
        handled_message_ids.extend(grouping_result.handled_message_ids)
        run_state["handled_message_ids"] = deduplicate_preserving_order(handled_message_ids)
        run_state["matched_existing_email_count"] = (
            int(run_state.get("matched_existing_email_count", 0))
            + grouping_result.matched_existing_email_count
        )
        run_state["new_application_email_count"] = (
            int(run_state.get("new_application_email_count", 0))
            + grouping_result.new_application_email_count
        )

        updated_existing_application_ids = set(cast(list[str], run_state.get("updated_existing_application_ids", [])))
        created_application_ids = set(cast(list[str], run_state.get("created_application_ids", [])))
        all_updates = cast(list[ApplicationRecord], run_state.get("all_updates", []))

        for update in grouping_result.updates:
            appl_id = str(update.get("appl_id", "") or "").strip()
            if appl_id:
                if appl_id in initial_existing_appl_ids:
                    updated_existing_application_ids.add(appl_id)
                else:
                    created_application_ids.add(appl_id)
            all_updates.append(copy.deepcopy(update))

        run_state["updated_existing_application_ids"] = sorted(updated_existing_application_ids)
        run_state["created_application_ids"] = sorted(created_application_ids)
        run_state["all_updates"] = all_updates
        run_state["working_existing"] = self._merge_updates_into_working_existing(
            cast(list[ApplicationRecord], run_state.get("working_existing", [])),
            grouping_result.updates,
        )

    def _execute_grouping_run(self, run_state: JsonObject) -> None:
        gemini_config = self.cfg.get("gemini", {})
        configured_max_rate_limit_wait_seconds = gemini_config.get(
            "rate_limit_max_wait_seconds",
            DEFAULT_AI_RATE_LIMIT_MAX_WAIT_SECONDS,
        )
        configured_max_rate_limit_retries_per_batch = gemini_config.get(
            "rate_limit_max_retries_per_batch",
            DEFAULT_AI_RATE_LIMIT_MAX_RETRIES_PER_BATCH,
        )
        max_rate_limit_wait_seconds = max(1, int(configured_max_rate_limit_wait_seconds))
        max_rate_limit_retries_per_cluster = max(0, int(configured_max_rate_limit_retries_per_batch))
        clusters = cast(list[JsonObject], run_state.get("clusters", []))
        total_cluster_count = len(clusters)
        run_id = str(run_state.get("run_id", "") or "").strip()
        run_state_path = self._grouping_run_state_path(run_id)
        email_map = {
            str(email_message.get("id", "") or "").strip(): email_message
            for email_message in cast(list[GmailMessage], run_state.get("new_emails", []))
            if str(email_message.get("id", "") or "").strip()
        }

        for cluster_index, cluster in enumerate(clusters):
            cluster_status = str(cluster.get("status", "") or "").strip().lower()
            if cluster_status == "completed":
                continue

            cluster_email_ids = cast(list[str], cluster.get("email_ids", []))
            cluster_emails = [
                email_map[email_id]
                for email_id in cluster_email_ids
                if email_id in email_map
            ]
            cluster_number = cluster_index + 1
            console.print(f"  AI grouping cluster {cluster_number} / {total_cluster_count} …")

            grouping_result = EmailGroupingResult(
                updates=[],
                ignored_email_count=0,
                handled_message_ids=[],
                matched_existing_email_count=0,
                new_application_email_count=0,
                updated_existing_application_count=0,
                created_application_count=0,
            )
            failure_reason = ""

            for retry_attempt in range(max_rate_limit_retries_per_cluster + 1):
                try:
                    grouping_result = self.ai.group_emails(
                        cluster_emails,
                        cast(list[ApplicationRecord], run_state.get("working_existing", [])),
                    )
                    failure_reason = ""
                    break
                except GeminiRateLimitError as error:
                    retry_delay_seconds = error.retry_delay_seconds or max_rate_limit_wait_seconds
                    if error.is_daily_quota_exhausted:
                        failure_reason = "Gemini daily quota exhausted while grouping new Gmail messages"
                        break
                    if retry_attempt >= max_rate_limit_retries_per_cluster:
                        failure_reason = str(error)
                        break
                    if retry_delay_seconds > max_rate_limit_wait_seconds:
                        failure_reason = (
                            "Gemini requested a retry wait longer than the configured limit "
                            f"({retry_delay_seconds}s)"
                        )
                        break
                    console.print(
                        f"[yellow]  Gemini rate limited this cluster. Waiting "
                        f"{retry_delay_seconds}s before retry {retry_attempt + 2} "
                        f"of {max_rate_limit_retries_per_cluster + 1}.[/yellow]"
                    )
                    time.sleep(retry_delay_seconds)
                except Exception as error:
                    failure_reason = str(error)
                    break

            if failure_reason:
                cluster["status"] = "failed"
                cluster["error"] = failure_reason
                cluster["failed_at"] = datetime.now(timezone.utc).isoformat()
                self._save_grouping_run_state(run_state)
                raise TrackerError(
                    self._format_grouping_failure_message(
                        failure_reason=failure_reason,
                        cluster_number=cluster_number,
                        total_cluster_count=total_cluster_count,
                        cluster_email_count=len(cluster_emails),
                        run_state_path=run_state_path,
                        run_id=run_id,
                    )
                )

            self._apply_grouping_result_to_run_state(run_state, grouping_result)
            cluster["status"] = "completed"
            cluster["error"] = ""
            cluster["failed_at"] = ""
            cluster["completed_at"] = datetime.now(timezone.utc).isoformat()
            self._save_grouping_run_state(run_state)

    def _finalize_completed_sync_run(self, run_state: JsonObject) -> None:
        all_updates = cast(list[ApplicationRecord], run_state.get("all_updates", []))
        query = str(run_state.get("query", "") or "")
        matched_existing_email_count = int(run_state.get("matched_existing_email_count", 0))
        new_application_email_count = int(run_state.get("new_application_email_count", 0))
        ignored_email_count = int(run_state.get("ignored_email_count", 0))
        updated_existing_application_ids = set(cast(list[str], run_state.get("updated_existing_application_ids", [])))
        created_application_ids = set(cast(list[str], run_state.get("created_application_ids", [])))

        relevant_email_count = matched_existing_email_count + new_application_email_count
        unique_updated_appl_ids = {update["appl_id"] for update in all_updates if update.get("appl_id")}
        merged_email_count = max(0, relevant_email_count - len(unique_updated_appl_ids))
        console.print(
            "  Grouping summary: "
            f"[green]{relevant_email_count}[/green] relevant emails, "
            f"[green]{ignored_email_count}[/green] ignored"
        )
        console.print(
            "  Application impact: "
            f"[green]{len(updated_existing_application_ids)}[/green] existing applications updated, "
            f"[green]{len(created_application_ids)}[/green] new applications created, "
            f"[green]{merged_email_count}[/green] emails merged into shared records"
        )

        all_updates = self._backfill_statuses_from_gmail(all_updates, base_query=query)
        all_updates = self._backfill_missing_companies_from_gmail(all_updates, base_query=query)
        all_updates = self._backfill_missing_roles_from_gmail(all_updates, base_query=query)
        run_state["all_updates"] = all_updates

        console.print(f"  Upserting [green]{len(all_updates)}[/green] records …")
        self.sheets.upsert_many(all_updates)

        processed_label_name = self.processing_labels.get_stage_label_name("processed")
        processing_label_ids_by_name = self.gmail.ensure_processing_labels(self.processing_labels)
        finalized_message_ids = deduplicate_preserving_order(cast(list[str], run_state.get("handled_message_ids", [])))
        state_retention_days = max(30, int(run_state.get("state_retention_days", 30)))
        processed_message_state_snapshot = self.processed_message_state.load(
            retention_days=state_retention_days
        )

        state_update_error: Optional[TrackerError] = None
        try:
            processed_message_state_snapshot = self.processed_message_state.record_processed_message_ids(
                existing_snapshot=processed_message_state_snapshot,
                message_ids=finalized_message_ids,
                retention_days=state_retention_days,
            )
        except TrackerError as error:
            state_update_error = error

        label_update_error: Optional[TrackerError] = None
        try:
            self.gmail.apply_labels_to_messages(
                message_ids=finalized_message_ids,
                label_ids=[
                    label_id
                    for label_id in [
                        processing_label_ids_by_name.get(self.processing_labels.root_label_name),
                        processing_label_ids_by_name.get(processed_label_name),
                    ]
                    if label_id
                ],
            )
        except TrackerError as error:
            label_update_error = error

        if state_update_error or label_update_error:
            error_messages = [
                str(error)
                for error in [state_update_error, label_update_error]
                if error is not None
            ]
            raise TrackerError(
                "Sync upserted Sheets successfully, but failed to finalize processed-message state:\n"
                + "\n".join(f"- {message}" for message in error_messages)
            )

        self._delete_grouping_run_state(str(run_state.get("run_id", "") or "").strip())
        console.print("[bold green]  OK Sync complete[/bold green]")
        console.print(
            f"  Sheet: [link={self.sheets.get_spreadsheet_url()}]{self.sheets.get_spreadsheet_url()}[/link]"
        )

    def resume_grouping_run(self, run_id: str) -> None:
        saved_run_state = self._load_grouping_run_state(run_id)
        console.rule("[bold blue]Resuming Saved Sync")
        console.print(f"  Saved run: [cyan]{run_id}[/cyan]")
        self._execute_grouping_run(saved_run_state)
        self._finalize_completed_sync_run(saved_run_state)
        if bool(saved_run_state.get("continue_to_digest")):
            self._run_digest_after_sync()

    @staticmethod
    def _missing_email_policy_field(action_type: str) -> str:
        field_by_action_type = {
            "follow_up": "follow_up_missing_email_policy",
            "withdraw": "withdraw_missing_email_policy",
            "deletion_request": "deletion_request_missing_email_policy",
        }
        return field_by_action_type.get(action_type, "")

    @staticmethod
    def _normalize_missing_email_policy(raw_policy: object) -> str:
        normalized_policy = str(raw_policy or "").strip().lower()
        allowed_policies = {"", "skip_always", "create_empty_draft"}
        if normalized_policy in allowed_policies:
            return normalized_policy
        return ""

    @classmethod
    def _describe_missing_email_policy(cls, raw_policy: object) -> str:
        normalized_policy = cls._normalize_missing_email_policy(raw_policy)
        policy_labels = {
            "": "Ask every time",
            "skip_always": "Opt out",
            "create_empty_draft": "Create empty draft",
        }
        return policy_labels.get(normalized_policy, "Ask every time")

    @staticmethod
    def _describe_action_opt_out(raw_value: object) -> str:
        return "Disabled" if is_truthy_sheet_value(raw_value) else "Enabled"

    def _manage_action_opt_outs(self, app: ApplicationRecord) -> None:
        opt_out_targets = {
            "f": ("follow_up_opt_out", "Follow-up"),
            "d": ("deletion_request_opt_out", "Deletion request"),
        }

        console.print("  Action opt-outs:")
        for shortcut, (field_name, label) in opt_out_targets.items():
            current_status = self._describe_action_opt_out(app.get(field_name, ""))
            console.print(f"  [cyan]{label} ({shortcut})[/cyan] — current: [green]{current_status}[/green]")
        selected_target = Prompt.ask(
            "  Choose action to edit",
            choices=[*opt_out_targets.keys(), "c"],
            default="c",
        ).strip().lower()
        if selected_target == "c":
            console.print("  [dim]Cancelled.[/dim]")
            return

        field_name, label = opt_out_targets[selected_target]
        current_status = self._describe_action_opt_out(app.get(field_name, ""))
        console.print(
            f"  {label}: current behavior is [green]{current_status}[/green]"
        )
        console.print(
            f"  Set {label.lower()} drafting for this row: [cyan]enabled (e)[/cyan], "
            "[cyan]disabled (a)[/cyan], "
            "or [cyan]cancel (c)[/cyan]"
        )
        selected_policy = Prompt.ask(
            "  New behavior",
            choices=["e", "a", "c"],
            default="c",
        ).strip().lower()
        if selected_policy == "c":
            console.print("  [dim]Cancelled.[/dim]")
            return

        opt_out_value = "" if selected_policy == "e" else "yes"
        status_label = self._describe_action_opt_out(opt_out_value)
        self.sheets.set_field(app["appl_id"], field_name, opt_out_value)
        app[field_name] = opt_out_value
        console.print(f"  [green]✓ {label} set to {status_label}[/green]")

    def _user_owned_email_addresses(self) -> set[str]:
        user_config = self.cfg.get("user", {})
        configured_user_email_addresses = {
            str(user_config.get("personal_email", "")).strip().lower(),
            str(user_config.get("career_email", "")).strip().lower(),
        }
        return {
            configured_email_address
            for configured_email_address in configured_user_email_addresses
            if configured_email_address
        }

    def _is_linkedin_application(self, app: ApplicationRecord) -> bool:
        """
        Determine whether an application appears to have originated via LinkedIn.
        """
        source_value = str(app.get("source", "") or "").strip().lower()
        if source_value == "linkedin":
            return True
        for related_message in self._iter_application_related_messages(app):
            sender_domain = extract_email_domain(resolve_message_sender_email(related_message))
            if get_base_domain(sender_domain) in SOCIAL_RELAY_DOMAINS:
                return True
            message_text = "\n".join([
                str(related_message.get("subject", "") or ""),
                str(related_message.get("snippet", "") or ""),
                str(related_message.get("body", "") or ""),
            ])
            if extract_linkedin_job_urls_from_text(message_text):
                return True
        return False

    def _iter_application_related_messages(self, app: ApplicationRecord) -> list[GmailMessage]:
        stored_email_ids = deduplicate_preserving_order(cast(list[str], safe_json_loads(
            app.get("email_ids") or "[]",
            f"email_ids for appl_id {app.get('appl_id', 'unknown')}",
            [],
        )))
        if not stored_email_ids:
            return []
        return self.gmail.get_messages_by_ids(stored_email_ids[:ROLE_BACKFILL_MAX_RELATED_MESSAGES])

    def _extract_known_application_contact_emails(
        self,
        app: ApplicationRecord,
        action_type: str,
    ) -> list[str]:
        """
        Collect candidate contact emails already present in the application's Gmail history.
        """
        known_contact_emails: list[str] = []
        user_owned_email_addresses = self._user_owned_email_addresses()
        for related_message in self._iter_application_related_messages(app):
            sender_email_address = resolve_message_sender_email(related_message)
            reply_to_email_address = resolve_message_reply_to_email(related_message)
            preferred_candidates = [
                reply_to_email_address,
                sender_email_address,
                *extract_email_addresses_from_text(str(related_message.get("body", "") or "")),
            ]
            for candidate_email in preferred_candidates:
                normalized_candidate_email = extract_email_address(candidate_email) or str(candidate_email or "").strip().lower()
                if not normalized_candidate_email or normalized_candidate_email in user_owned_email_addresses:
                    continue
                if is_unusable_outbound_email(normalized_candidate_email):
                    continue
                if (
                    extract_email_domain(normalized_candidate_email) in SOCIAL_RELAY_DOMAINS
                    or get_base_domain(extract_email_domain(normalized_candidate_email)) in SOCIAL_RELAY_DOMAINS
                ):
                    continue
                if normalized_candidate_email not in known_contact_emails:
                    known_contact_emails.append(normalized_candidate_email)
        return known_contact_emails

    def _fetch_text_url(self, url: str, timeout_seconds: int = 10) -> str:
        request = UrlRequest(
            url,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36"
                )
            },
        )
        with urlopen(request, timeout=timeout_seconds) as response:
            response_bytes = response.read(300_000)
            charset = getattr(response.headers, "get_content_charset", lambda _default=None: None)("utf-8")
        return response_bytes.decode(charset or "utf-8", errors="replace")

    def _extract_same_domain_candidate_links(
        self,
        page_url: str,
        page_text: str,
        *,
        max_links: int = 8,
    ) -> list[str]:
        """
        Extract likely contact-related links from a fetched HTML page.
        """
        parsed_page_url = urlparse(page_url)
        page_hostname = str(parsed_page_url.hostname or "").lower()
        page_base_domain = get_base_domain(page_hostname)
        if not page_base_domain:
            return []

        candidate_links: list[str] = []
        seen_links: set[str] = set()
        anchor_pattern = re.compile(
            r"""href\s*=\s*["'](?P<href>[^"'#]+)["'][^>]*>(?P<label>.{0,200}?)</a>""",
            flags=re.IGNORECASE | re.DOTALL,
        )
        likely_markers = (
            "contact",
            "about",
            "team",
            "career",
            "careers",
            "jobs",
            "hiring",
            "privacy",
            "legal",
            "gdpr",
            "impressum",
            "datenschutz",
            "people",
            "hr",
        )
        for match in anchor_pattern.finditer(page_text):
            raw_href = html.unescape(str(match.group("href") or "").strip())
            if not raw_href or raw_href.lower().startswith("mailto:"):
                continue
            absolute_url = urljoin(page_url, raw_href)
            parsed_absolute_url = urlparse(absolute_url)
            absolute_hostname = str(parsed_absolute_url.hostname or "").lower()
            if get_base_domain(absolute_hostname) != page_base_domain:
                continue
            link_text = html.unescape(str(match.group("label") or "")).lower()
            normalized_url_text = absolute_url.lower()
            if not any(marker in normalized_url_text or marker in link_text for marker in likely_markers):
                continue
            if absolute_url in seen_links:
                continue
            seen_links.add(absolute_url)
            candidate_links.append(absolute_url)
            if len(candidate_links) >= max_links:
                break
        return candidate_links

    def _record_contact_email_candidate(
        self,
        candidates: list[ContactEmailCandidate],
        seen_emails: set[str],
        *,
        email: str,
        source_type: str,
        source_url: str = "",
        source_domain: str = "",
        source_title: str = "",
        notes: str = "",
    ) -> None:
        """
        Add a normalized contact-email candidate if it passes basic safety checks.
        """
        normalized_email = extract_email_address(email) or str(email or "").strip().lower()
        if not normalized_email or normalized_email in seen_emails:
            return
        if normalized_email in self._user_owned_email_addresses():
            return
        if is_unusable_outbound_email(normalized_email):
            return
        if get_base_domain(extract_email_domain(normalized_email)) in SOCIAL_RELAY_DOMAINS:
            return
        seen_emails.add(normalized_email)
        candidates.append({
            "email": normalized_email,
            "source_type": source_type,
            "source_url": source_url.strip(),
            "source_domain": get_base_domain(source_domain.strip().lower()),
            "source_title": source_title.strip(),
            "notes": notes.strip(),
        })

    def _collect_contact_email_candidates_from_web_page(
        self,
        *,
        page_url: str,
        page_text: str,
        source_type: str,
        known_domains: set[str],
        company_name_hints: list[str],
        candidates: list[ContactEmailCandidate],
        seen_emails: set[str],
        source_title: str = "",
    ) -> None:
        """
        Extract and validate email candidates from a fetched web page.
        """
        page_hostname = str(urlparse(page_url).hostname or "").lower()
        page_base_domain = get_base_domain(page_hostname)
        for extracted_email in extract_email_addresses_from_text(page_text):
            if not self._is_grounded_contact_search_result_acceptable(
                company_name_hints=company_name_hints,
                known_domains=known_domains,
                candidate_email=extracted_email,
                source_domain=page_base_domain,
            ):
                continue
            self._record_contact_email_candidate(
                candidates,
                seen_emails,
                email=extracted_email,
                source_type=source_type,
                source_url=page_url,
                source_domain=page_base_domain,
                source_title=source_title,
            )

    def _collect_contact_email_candidates_from_url(
        self,
        *,
        page_url: str,
        source_type: str,
        known_domains: set[str],
        company_name_hints: list[str],
        candidates: list[ContactEmailCandidate],
        seen_emails: set[str],
        log_progress: bool = False,
    ) -> None:
        """
        Fetch a page, extract direct emails, then inspect likely same-domain contact pages.
        """
        try:
            page_text = self._fetch_text_url(page_url)
        except Exception:
            return
        if log_progress:
            console.print(f"  Inspecting web page: [dim]{page_url}[/dim]")
        self._collect_contact_email_candidates_from_web_page(
            page_url=page_url,
            page_text=page_text,
            source_type=source_type,
            known_domains=known_domains,
            company_name_hints=company_name_hints,
            candidates=candidates,
            seen_emails=seen_emails,
        )
        for candidate_link in self._extract_same_domain_candidate_links(page_url, page_text):
            try:
                linked_page_text = self._fetch_text_url(candidate_link)
            except Exception:
                continue
            self._collect_contact_email_candidates_from_web_page(
                page_url=candidate_link,
                page_text=linked_page_text,
                source_type=f"{source_type}_linked_page",
                known_domains=known_domains,
                company_name_hints=company_name_hints,
                candidates=candidates,
                seen_emails=seen_emails,
            )

    def _discover_linkedin_company_hints(
        self,
        app: ApplicationRecord,
        log_progress: bool = False,
    ) -> tuple[set[str], list[str]]:
        """
        Derive company-domain and company-name hints from LinkedIn job/company pages.
        """
        if not self._is_linkedin_application(app):
            return set(), []

        if log_progress:
            console.print("  Trying LinkedIn job-page hints…")

        linkedin_job_urls: list[str] = []
        for related_message in self._iter_application_related_messages(app):
            message_text = "\n".join([
                str(related_message.get("subject", "") or ""),
                str(related_message.get("snippet", "") or ""),
                str(related_message.get("body", "") or ""),
            ])
            for linkedin_job_url in extract_linkedin_job_urls_from_text(message_text):
                if linkedin_job_url not in linkedin_job_urls:
                    linkedin_job_urls.append(linkedin_job_url)

        discovered_domains: set[str] = set()
        discovered_name_hints: list[str] = []
        seen_urls: set[str] = set()
        pending_urls = list(linkedin_job_urls)

        while pending_urls:
            candidate_url = pending_urls.pop(0)
            if candidate_url in seen_urls:
                continue
            seen_urls.add(candidate_url)
            if log_progress:
                console.print(f"  Trying LinkedIn page: [dim]{candidate_url}[/dim]")
            try:
                page_text = self._fetch_text_url(candidate_url)
            except Exception:
                continue

            for company_url in extract_linkedin_company_urls_from_text(page_text):
                if company_url not in seen_urls and company_url not in pending_urls:
                    pending_urls.append(company_url)

            for extracted_url in extract_http_urls_from_text(page_text):
                parsed_url = urlparse(extracted_url)
                hostname = str(parsed_url.hostname or "").lower()
                base_domain = get_base_domain(hostname)
                if not base_domain or base_domain in SOCIAL_RELAY_DOMAINS or is_linkedin_noise_domain(base_domain):
                    continue
                if is_trusted_contact_source_domain(hostname):
                    continue
                discovered_domains.add(base_domain)

            company_name_patterns = (
                r'"companyName"\s*:\s*"([^"]+)"',
                r'"name"\s*:\s*"([^"]+)"',
                r'<title>\s*([^<]{2,120}?)\s*\|\s*LinkedIn',
            )
            for company_name_pattern in company_name_patterns:
                for match in re.finditer(company_name_pattern, page_text, flags=re.IGNORECASE):
                    candidate_company_name = html.unescape(match.group(1)).strip()
                    candidate_company_name = re.sub(r"\s+", " ", candidate_company_name).strip(" -|")
                    if (
                        candidate_company_name
                        and candidate_company_name not in discovered_name_hints
                        and not is_linkedin_noise_company_name_hint(candidate_company_name)
                        and not is_low_confidence_company_name(candidate_company_name)
                    ):
                        discovered_name_hints.append(candidate_company_name)

        row_company_name = str(app.get("company", "") or "").strip()
        if row_company_name and row_company_name not in discovered_name_hints:
            discovered_name_hints.append(row_company_name)

        return discovered_domains, discovered_name_hints

    def _search_web_result_urls(
        self,
        query: str,
        max_results: int = 5,
        company_name_hints: Optional[list[str]] = None,
        known_domains: Optional[set[str]] = None,
    ) -> list[str]:
        """
        Search DuckDuckGo for a query and return candidate result URLs.
        Applies a snippet-level pre-filter when company_name_hints are provided.
        """
        search_url = f"https://html.duckduckgo.com/html/?q={quote(query)}"
        try:
            search_html = self._fetch_text_url(search_url)
        except Exception:
            return []

        _JUNK_DOMAINS = {
            "claude.ai", "anthropic.com", "openai.com", "chatgpt.com",
            "wikipedia.org", "reddit.com", "twitter.com", "x.com",
            "facebook.com", "instagram.com", "youtube.com",
        }

        # Extract (url, snippet) pairs from DDG HTML result blocks
        result_blocks = re.findall(
            r'class="result__extras__url"[^>]*>\s*(https?://[^\s<"]+).*?'
            r'class="result__snippet"[^>]*>(.*?)</a>',
            search_html,
            flags=re.DOTALL,
        )
        # Fallback: just extract result URLs if block pattern didn't match
        if not result_blocks:
            raw_urls = re.findall(r'class="result__url"[^>]*>\s*(https?://[^\s<"]+)', search_html)
            result_blocks = [(u, "") for u in raw_urls]

        candidate_urls: list[str] = []
        for raw_url, snippet_html in result_blocks:
            raw_url = raw_url.strip().rstrip("/,")
            parsed_url = urlparse(raw_url)
            if parsed_url.scheme not in {"http", "https"}:
                continue
            hostname = str(parsed_url.hostname or "").lower()
            base_domain = get_base_domain(hostname)
            if not base_domain or base_domain in _JUNK_DOMAINS:
                continue
            # Snippet pre-filter: skip if snippet shows no company relevance
            if company_name_hints and snippet_html:
                snippet_text = re.sub(r"<[^>]+>", "", snippet_html).strip()
                combined_text = normalize_matching_text(f"{raw_url} {snippet_text}")
                tokens = deduplicate_preserving_order([
                    t
                    for hint in company_name_hints
                    for t in build_company_search_tokens(hint)
                ])
                if tokens and not any(t in combined_text for t in tokens):
                    continue
            if raw_url not in candidate_urls:
                candidate_urls.append(raw_url)
            if len(candidate_urls) >= max_results:
                break
        return candidate_urls

    def _is_grounded_contact_search_result_acceptable(
        self,
        company_name_hints: list[str],
        known_domains: set[str],
        candidate_email: str,
        source_domain: str,
    ) -> bool:
        normalized_candidate_email = extract_email_address(candidate_email)
        if not normalized_candidate_email:
            return False
        if normalized_candidate_email in self._user_owned_email_addresses():
            return False
        if is_unusable_outbound_email(normalized_candidate_email):
            return False

        candidate_email_base_domain = get_base_domain(extract_email_domain(normalized_candidate_email))
        normalized_source_domain = get_base_domain(source_domain)
        if not candidate_email_base_domain or not normalized_source_domain:
            return False

        company_search_tokens = deduplicate_preserving_order([
            company_search_token
            for company_name_hint in company_name_hints
            for company_search_token in build_company_search_tokens(company_name_hint)
        ])
        source_primary_label = get_primary_domain_label(normalized_source_domain)
        source_domain_is_acceptable = (
            normalized_source_domain in known_domains
            or is_trusted_contact_source_domain(normalized_source_domain)
            or source_primary_label in company_search_tokens
        )
        if not source_domain_is_acceptable:
            return False

        email_candidate_url = f"https://{candidate_email_base_domain}/"
        return (
            candidate_email_base_domain == normalized_source_domain
            or candidate_email_base_domain in known_domains
            or is_trusted_contact_source_domain(candidate_email_base_domain)
            or is_search_result_likely_related_to_company(
                candidate_url=email_candidate_url,
                company_name_hints=company_name_hints,
                known_domains=known_domains,
            )
        )

    def _discover_company_contact_via_web(
        self,
        company: str,
        action_type: str,
        app: Optional[ApplicationRecord] = None,
        log_progress: bool = False,
    ) -> Optional[str]:
        """
        Discover a company contact email via Gemini Google Search grounding.
        """
        normalized_company = str(company or "").strip()
        if not normalized_company:
            return None

        known_domains: set[str] = set()
        company_name_hints: list[str] = []
        web_seed_urls: list[str] = []
        if app is not None:
            for field_name in ("contact_email", "recruiter_email"):
                email_domain = get_base_domain(extract_email_domain(str(app.get(field_name, "") or "")))
                if email_domain and email_domain not in SOCIAL_RELAY_DOMAINS:
                    known_domains.add(email_domain)
            for related_message in self._iter_application_related_messages(app):
                message_text = "\n".join([
                    str(related_message.get("subject", "") or ""),
                    str(related_message.get("snippet", "") or ""),
                    str(related_message.get("body", "") or ""),
                ])
                for extracted_email in extract_email_addresses_from_text(message_text):
                    email_domain = get_base_domain(extract_email_domain(extracted_email))
                    if email_domain and email_domain not in SOCIAL_RELAY_DOMAINS:
                        known_domains.add(email_domain)
                for url in extract_http_urls_from_text(message_text):
                    parsed_url = urlparse(url)
                    base_domain = get_base_domain(str(parsed_url.hostname or ""))
                    if base_domain and base_domain not in SOCIAL_RELAY_DOMAINS:
                        known_domains.add(base_domain)
                    if url not in web_seed_urls:
                        web_seed_urls.append(url)
            linkedin_known_domains, linkedin_company_name_hints = self._discover_linkedin_company_hints(
                app,
                log_progress=log_progress,
            )
            known_domains.update(linkedin_known_domains)
            for linkedin_company_name_hint in linkedin_company_name_hints:
                if linkedin_company_name_hint not in company_name_hints:
                    company_name_hints.append(linkedin_company_name_hint)

        if normalized_company and normalized_company not in company_name_hints:
            company_name_hints.insert(0, normalized_company)

        candidates: list[ContactEmailCandidate] = []
        seen_candidate_emails: set[str] = set()

        if log_progress:
            console.print(
                "  Searching the web with Gemini for a "
                f"{'privacy' if action_type in {'withdraw', 'deletion_request'} else 'hiring'} contact…"
            )

        grounded_result: JsonObject = {}
        try:
            grounded_result = self.ai.find_grounded_company_contact_email(
                company=normalized_company,
                action_type=action_type,
                known_domains=sorted(known_domains),
                company_name_hints=company_name_hints,
            )
        except GeminiRateLimitError as error:
            if log_progress:
                console.print(
                    "[yellow]  Gemini web search is temporarily unavailable: "
                    f"{error}. Falling back to manual handling.[/yellow]"
                )
            grounded_result = {}
        except TrackerError as error:
            if log_progress:
                console.print(
                    "[yellow]  Gemini web search failed: "
                    f"{error}. Falling back to manual handling.[/yellow]"
                )
            grounded_result = {}

        search_queries = cast(list[str], grounded_result.get("search_queries") or [])
        if log_progress:
            for search_query in search_queries[:3]:
                console.print(f"  Gemini web search query: [dim]{search_query}[/dim]")

        grounded_email = extract_email_address(str(grounded_result.get("email", "") or ""))
        grounded_source_domain = get_base_domain(str(grounded_result.get("source_domain", "") or "").strip().lower())
        grounded_source_title = str(grounded_result.get("source_title", "") or "").strip()
        grounded_source_url = str(grounded_result.get("source_url", "") or "").strip()
        grounded_notes = str(grounded_result.get("notes", "") or "").strip()

        if grounded_email and self._is_grounded_contact_search_result_acceptable(
            company_name_hints=company_name_hints,
            known_domains=known_domains,
            candidate_email=grounded_email,
            source_domain=grounded_source_domain,
        ):
            self._record_contact_email_candidate(
                candidates,
                seen_candidate_emails,
                email=grounded_email,
                source_type="gemini_grounded",
                source_url=grounded_source_url,
                source_domain=grounded_source_domain,
                source_title=grounded_source_title,
                notes=grounded_notes,
            )

        page_urls_to_inspect = deduplicate_preserving_order([
            grounded_source_url,
            *web_seed_urls,
        ])
        for page_url in page_urls_to_inspect[:8]:
            self._collect_contact_email_candidates_from_url(
                page_url=page_url,
                source_type="related_page",
                known_domains=known_domains,
                company_name_hints=company_name_hints,
                candidates=candidates,
                seen_emails=seen_candidate_emails,
                log_progress=log_progress,
            )

        _GENERIC_SEARCH_TERMS = {
            "ctrl", "core", "data", "tech", "labs", "next", "open", "plus",
            "soft", "apps", "base", "edge", "link", "node", "sync", "work",
        }
        company_tokens = build_company_search_tokens(normalized_company)
        short_name = company_tokens[0] if company_tokens else ""
        use_short_name = (
            bool(short_name)
            and short_name != normalized_company.lower()
            and len(short_name) >= 5
            and short_name not in _GENERIC_SEARCH_TERMS
        )
        is_privacy_action = action_type in {"withdraw", "deletion_request"}
        if is_privacy_action:
            keyword_variants = ["privacy contact", "privacy email", "privacy policy"]
            extra_queries = [
                f'"{normalized_company}" {kw}' for kw in keyword_variants
            ] + ([
                f'"{short_name}" {kw}' for kw in keyword_variants
            ] if use_short_name else [])
        else:
            extra_queries = [
                f'"{normalized_company}" careers email',
                f'"{normalized_company}" recruiting email',
                f'"{normalized_company}" hr email',
            ] + ([
                f'"{short_name}" careers email',
                f'"{short_name}" hr email',
            ] if use_short_name else [])
        search_queries_to_try = deduplicate_preserving_order([
            *search_queries,
            *extra_queries,
            f'"@{next(iter(sorted(known_domains)), "")}" email' if known_domains else "",
        ])
        for search_query in search_queries_to_try[:6]:
            if log_progress:
                console.print(f"  Trying web search: [dim]{search_query}[/dim]")
            for result_url in self._search_web_result_urls(
                search_query,
                max_results=4,
                company_name_hints=company_name_hints,
                known_domains=known_domains,
            ):
                if not is_search_result_likely_related_to_company(
                    candidate_url=result_url,
                    company_name_hints=company_name_hints,
                    known_domains=known_domains,
                ):
                    continue
                self._collect_contact_email_candidates_from_url(
                    page_url=result_url,
                    source_type="search_result",
                    known_domains=known_domains,
                    company_name_hints=company_name_hints,
                    candidates=candidates,
                    seen_emails=seen_candidate_emails,
                    log_progress=log_progress,
                )

        if not candidates:
            return None

        locally_best_candidate_email = choose_best_contact_email_for_action(
            [str(candidate.get("email", "") or "") for candidate in candidates],
            action_type,
        )
        selected_email = locally_best_candidate_email
        selection_reason = ""
        if len(candidates) > 1:
            try:
                ai_selection = self.ai.choose_best_company_contact_email(
                    company=normalized_company,
                    action_type=action_type,
                    candidates=candidates,
                    known_domains=sorted(known_domains),
                    company_name_hints=company_name_hints,
                )
                ai_selected_email = extract_email_address(str(ai_selection.get("email", "") or ""))
                if ai_selected_email:
                    selected_email = ai_selected_email
                    selection_reason = str(ai_selection.get("reason", "") or "").strip()
            except TrackerError:
                pass

        if log_progress:
            console.print("  Candidate emails discovered:")
            for candidate in candidates[:8]:
                candidate_email = str(candidate.get("email", "") or "")
                candidate_source = (
                    str(candidate.get("source_title", "") or "")
                    or str(candidate.get("source_domain", "") or "")
                    or str(candidate.get("source_url", "") or "")
                    or str(candidate.get("source_type", "") or "")
                )
                candidate_score = score_contact_email_for_action(candidate_email, action_type)
                console.print(
                    f"    [dim]{candidate_score:+d}[/dim] [green]{candidate_email}[/green]"
                    f" [dim]({candidate_source})[/dim]"
                )
            if selection_reason:
                console.print(f"  Selected by Gemini: [green]{selected_email}[/green] [dim]{selection_reason}[/dim]")
            else:
                console.print(f"  Selected recipient: [green]{selected_email}[/green]")
        return selected_email or None

    def _resolve_company_contact_email(
        self,
        app: ApplicationRecord,
        action_type: str,
        log_progress: bool = False,
        allow_stored_recipient: bool = True,
        privacy_only: bool = False,
    ) -> str:
        """
        Resolve a contact email using stored row data, related Gmail messages, then web lookup.
        """
        direct_target = resolve_outbound_target_email(app, action_type)
        if direct_target and allow_stored_recipient:
            if log_progress:
                console.print(f"  Using stored recipient on row: [green]{direct_target}[/green]")
            return direct_target

        if log_progress:
            if direct_target and not allow_stored_recipient:
                console.print(
                    "  Stored recipient on the row is being kept as a fallback while "
                    f"searching for a {'privacy' if action_type in {'withdraw', 'deletion_request'} else 'hiring'} contact: "
                    f"[green]{direct_target}[/green]"
                )
            else:
                console.print("  No usable recipient stored on the row.")
            console.print("  Checking application emails for a real contact…")
        known_contact_emails = self._extract_known_application_contact_emails(app, action_type)
        if privacy_only:
            known_contact_emails = [
                contact_email
                for contact_email in known_contact_emails
                if is_privacy_style_contact_email(contact_email)
            ]
        selected_known_contact_email = choose_best_contact_email_for_action(known_contact_emails, action_type)
        if selected_known_contact_email:
            if log_progress:
                console.print(f"  Found recipient in application messages: [green]{selected_known_contact_email}[/green]")
            return selected_known_contact_email

        if log_progress:
            console.print("  No usable email found in application messages.")
            console.print(
                "  Searching the web for a "
                f"{'privacy' if action_type in {'withdraw', 'deletion_request'} else 'hiring'} contact…"
            )

        discovered_contact_email = self._discover_company_contact_via_web(
            company=str(app.get("company", "") or ""),
            action_type=action_type,
            app=app,
            log_progress=log_progress,
        )
        if discovered_contact_email and privacy_only and not is_privacy_style_contact_email(discovered_contact_email):
            if log_progress:
                console.print(
                    "  Ignoring discovered recipient because it is not a privacy contact: "
                    f"[yellow]{discovered_contact_email}[/yellow]"
                )
            discovered_contact_email = ""
        if discovered_contact_email and is_unusable_outbound_email(discovered_contact_email):
            if log_progress:
                console.print(
                    f"  Ignoring unusable discovered recipient: "
                    f"[yellow]{discovered_contact_email}[/yellow]"
                )
            discovered_contact_email = ""
        if log_progress and not discovered_contact_email:
            console.print("  No trusted contact email found.")
        return discovered_contact_email or ""

    def _choose_existing_or_manual_privacy_contact(
        self,
        app: ApplicationRecord,
        action_type: str,
    ) -> tuple[str, bool]:
        """
        For privacy-style actions, let the user explicitly keep the stored contact or
        override it manually before automatic discovery runs.
        """
        if action_type not in {"withdraw", "deletion_request"}:
            return "", False

        existing_email = resolve_outbound_target_email(app, action_type)
        if not existing_email:
            return "", False

        company_name = str(app.get("company", "") or "").strip() or "Unknown company"
        role_title = str(app.get("role", "") or "").strip() or "Unknown role"
        console.print(
            "  Privacy action recipient found on the row for "
            f"[cyan]{company_name}[/cyan] — {role_title}: [green]{existing_email}[/green]"
        )
        console.print(
            "  Choose: (Enter/u) use stored email  (m) set manual email  (s) search for privacy contact"
        )

        while True:
            choice = Prompt.ask("  Select option", default="").strip().lower()
            if choice in ("u", ""):
                return existing_email, False
            if choice == "m":
                confirmed_email = self._confirm_email_value("")
                return confirmed_email, False
            if choice == "s":
                return "", True
            console.print("[red]  Choose u / m / s, or press Enter to use stored.[/red]")

    def _resolve_privacy_contact_after_search_choice(
        self,
        app: ApplicationRecord,
        action_type: str,
    ) -> str:
        """
        Run an explicit privacy-contact search, then re-prompt with the stored email if needed.
        """
        discovered_privacy_contact = self._resolve_company_contact_email(
            app,
            action_type,
            log_progress=True,
            allow_stored_recipient=False,
            privacy_only=True,
        )
        if discovered_privacy_contact:
            return discovered_privacy_contact
        return self._prompt_for_privacy_contact_after_failed_search(app, action_type)

    def _prompt_for_privacy_contact_after_failed_search(
        self,
        app: ApplicationRecord,
        action_type: str,
    ) -> str:
        """
        After an explicit privacy search fails, offer the stored recipient or a manual override.
        """
        if action_type not in {"withdraw", "deletion_request"}:
            return ""

        existing_email = resolve_outbound_target_email(app, action_type)
        if not existing_email:
            return ""

        company_name = str(app.get("company", "") or "").strip() or "Unknown company"
        role_title = str(app.get("role", "") or "").strip() or "Unknown role"
        console.print(
            "  No trusted privacy contact found for "
            f"[cyan]{company_name}[/cyan] — {role_title}."
        )
        console.print(f"  Available recipient on the row: [green]{existing_email}[/green]")
        console.print("  Choose: (Enter/u) use available email  (m) set manual email")

        while True:
            choice = Prompt.ask("  Select option", default="").strip().lower()
            if choice in ("u", ""):
                return existing_email
            if choice == "m":
                return self._confirm_email_value("")
            console.print("[red]  Choose u / m, or press Enter to use the available email.[/red]")

    def _persist_resolved_contact_email(
        self,
        app: ApplicationRecord,
        resolved_email: str,
        action_type: str,
    ) -> None:
        """
        Persist a resolved outbound contact email back into the application row.
        """
        if action_type != "follow_up":
            return
        normalized_resolved_email = extract_email_address(resolved_email) or str(resolved_email or "").strip().lower()
        if not normalized_resolved_email:
            return
        if is_unusable_outbound_email(normalized_resolved_email):
            return

        appl_id = str(app.get("appl_id", "") or "").strip()
        if not appl_id:
            return

        current_contact_email = extract_email_address(str(app.get("contact_email", "") or ""))
        if current_contact_email == normalized_resolved_email:
            return

        self.sheets.set_field(appl_id, "contact_email", normalized_resolved_email)
        app["contact_email"] = normalized_resolved_email

    def _confirm_email_value(self, initial_email: str) -> str:
        candidate_email = initial_email.strip().lower()
        while True:
            if "@" not in candidate_email:
                candidate_email = Prompt.ask("  Email address").strip().lower()
                continue

            console.print(f"  Proposed email: [cyan]{candidate_email}[/cyan]")
            confirmation_choice = Prompt.ask(
                "  Confirm email (y)es/(f)ix",
                choices=["y", "f"],
                default="y",
            ).strip().lower()
            if confirmation_choice == "y":
                return candidate_email
            candidate_email = Prompt.ask("  Fix email").strip().lower()

    def _resolve_missing_email_action(
        self,
        app: ApplicationRecord,
        action_type: str,
    ) -> tuple[str, bool, bool]:
        policy_field = self._missing_email_policy_field(action_type)
        saved_policy = self._normalize_missing_email_policy(app.get(policy_field, ""))
        if saved_policy == "skip_always":
            return "", False, True
        if saved_policy == "create_empty_draft":
            return "", True, False

        company_name = str(app.get("company", "") or "").strip() or "Unknown company"
        role_title = str(app.get("role", "") or "").strip() or "Unknown role"
        console.print(
            f"[yellow]  No usable recipient found for {action_type} — "
            f"{company_name} — {role_title}[/yellow]"
        )
        google_search_url = (
            f"https://www.google.com/search?q={quote(company_name)}"
            if company_name and company_name != "Unknown company"
            else ""
        )
        if google_search_url:
            console.print(
                f"  Search: [link={google_search_url}]{google_search_url}[/link]"
            )
        console.print(
            "  Enter an email address directly, or choose: "
            "(o) skip once  (a) skip always  (d) create empty draft"
        )
        console.print("  Press Enter to manually type an email")

        while True:
            choice = Prompt.ask("  Select option", default="").strip()
            normalized_choice = choice.lower()

            if "@" in choice:
                confirmed_email = self._confirm_email_value(choice)
                if action_type == "follow_up":
                    self.sheets.set_field(app["appl_id"], "contact_email", confirmed_email)
                    app["contact_email"] = confirmed_email
                if policy_field:
                    self.sheets.set_field(app["appl_id"], policy_field, "")
                app["recruiter_email"] = app.get("recruiter_email", "")
                return confirmed_email, False, False

            if normalized_choice == "":
                confirmed_email = self._confirm_email_value("")
                if action_type == "follow_up":
                    self.sheets.set_field(app["appl_id"], "contact_email", confirmed_email)
                    app["contact_email"] = confirmed_email
                if policy_field:
                    self.sheets.set_field(app["appl_id"], policy_field, "")
                app["recruiter_email"] = app.get("recruiter_email", "")
                return confirmed_email, False, False

            if normalized_choice == "o":
                return "", False, True

            if normalized_choice == "a":
                if policy_field:
                    self.sheets.set_field(app["appl_id"], policy_field, "skip_always")
                    app[policy_field] = "skip_always"
                return "", False, True

            if normalized_choice == "d":
                if policy_field:
                    self.sheets.set_field(app["appl_id"], policy_field, "create_empty_draft")
                    app[policy_field] = "create_empty_draft"
                return "", True, False

            console.print("[red]  Enter an email address, or choose o / a / d.[/red]")

    def _revalidate_pending_action_target_email(
        self,
        pending_action: PendingActionRecord,
    ) -> tuple[str, str]:
        """
        Re-check a queued action's recipient before send-time so stale ATS no-reply
        targets do not get sent accidentally.
        """
        queued_target_email = extract_email_address(str(pending_action.get("target_email", "") or ""))
        if queued_target_email and not is_unusable_outbound_email(queued_target_email):
            return queued_target_email, ""

        appl_id = str(pending_action.get("appl_id", "") or "").strip()
        if not appl_id:
            return "", "missing appl_id on pending action"

        current_applications = self.sheets.get_all()
        current_application = next(
            (
                application
                for application in current_applications
                if str(application.get("appl_id", "") or "").strip() == appl_id
            ),
            None,
        )
        if current_application is None:
            return "", "application row no longer exists"

        action_type = str(pending_action.get("type", "") or "").strip()
        refreshed_target_email = self._resolve_company_contact_email(
            current_application,
            action_type,
            log_progress=False,
        )
        if refreshed_target_email and not is_unusable_outbound_email(refreshed_target_email):
            self._persist_resolved_contact_email(
                current_application,
                refreshed_target_email,
                action_type=action_type,
            )
            return refreshed_target_email, ""

        return "", "no usable recipient found after revalidation"

    def _find_best_related_role_title(
        self,
        application_row: ApplicationRecord,
        base_query: str,
    ) -> str:
        """
        Search related Gmail messages and recover a better role title for one application row.

        Inputs:
        - application_row: Existing or newly built application record that currently lacks a
          reliable role title.
        - base_query: Configured Gmail query used to keep the search within the user's job-mail
          corpus and exclusions.

        Outputs:
        - Best recovered role title, or an empty string when no stronger title is found.

        Edge cases:
        - Rows without a company or contact signals return an empty string immediately.
        - Only a bounded number of related Gmail messages are scanned to avoid excessive API use.
        - Stage-like phrases such as interview labels are rejected even when they match a pattern.

        Atomicity / concurrency:
        - Read-only Gmail search and parsing flow with no local side effects.
        """
        related_messages = self._load_related_messages_for_backfill(
            application_row,
            base_query=base_query,
        )
        candidate_role_titles: list[str] = []
        for related_message in related_messages:
            candidate_role_titles.extend(
                extract_role_titles_from_email_message(related_message)
            )
        return choose_preferred_role_title(*candidate_role_titles)

    def _load_related_messages_for_backfill(
        self,
        application_row: ApplicationRecord,
        base_query: str,
    ) -> list[GmailMessage]:
        """
        Load Gmail messages that are likely related to one application for backfill recovery.

        Inputs:
        - application_row: Existing application record being enriched from Gmail history.
        - base_query: Configured Gmail query used to keep search results within the job-mail corpus.

        Outputs:
        - Deduplicated parsed Gmail messages gathered from stored message IDs and bounded search.

        Edge cases:
        - Generic ATS sender addresses may prevent related search terms from being generated, so
          stored Gmail message IDs are loaded first when present.
        - Missing or malformed stored `email_ids` are tolerated and treated as empty.

        Atomicity / concurrency:
        - Read-only Gmail fetch/search flow with no local side effects.
        """
        related_messages: list[GmailMessage] = []
        seen_message_ids: set[str] = set()

        stored_email_ids = deduplicate_preserving_order(cast(list[str], safe_json_loads(
            application_row.get("email_ids") or "[]",
            f"email_ids for appl_id {application_row.get('appl_id', 'unknown')}",
            [],
        )))
        if stored_email_ids:
            for related_message in self.gmail.get_messages_by_ids(
                stored_email_ids[:ROLE_BACKFILL_MAX_RELATED_MESSAGES]
            ):
                message_id = str(related_message.get("id", "") or "").strip()
                if message_id and message_id not in seen_message_ids:
                    seen_message_ids.add(message_id)
                    related_messages.append(related_message)

        related_search_terms = build_related_gmail_search_terms(application_row)
        if not related_search_terms or len(related_messages) >= ROLE_BACKFILL_MAX_RELATED_MESSAGES:
            return related_messages

        related_query = f"({base_query}) ({' OR '.join(related_search_terms)})"
        searched_messages = self.gmail.search_messages(
            query=related_query,
            since_days=ROLE_BACKFILL_LOOKBACK_DAYS,
            max_results=ROLE_BACKFILL_MAX_RELATED_MESSAGES,
        )
        for related_message in searched_messages:
            message_id = str(related_message.get("id", "") or "").strip()
            if message_id and message_id in seen_message_ids:
                continue
            if message_id:
                seen_message_ids.add(message_id)
            related_messages.append(related_message)
            if len(related_messages) >= ROLE_BACKFILL_MAX_RELATED_MESSAGES:
                break
        return related_messages

    def _find_best_related_company_name(
        self,
        application_row: ApplicationRecord,
        base_query: str,
    ) -> str:
        """
        Search related Gmail messages and recover the strongest company name signal.

        Inputs:
        - application_row: Existing or newly built application record that may need a company fix.
        - base_query: Configured Gmail query used to keep the search within the user's job-mail corpus.

        Outputs:
        - Recovered company name, or an empty string when no stronger company signal is found.

        Edge cases:
        - Prefers explicit company phrases from email subjects/bodies over sender-header hints.
        - Returns an empty string when every candidate matches the current company after normalization.

        Atomicity / concurrency:
        - Read-only Gmail search and parsing flow with no local side effects.
        """
        related_messages = self._load_related_messages_for_backfill(
            application_row,
            base_query=base_query,
        )
        current_company_name = str(application_row.get("company", "") or "").strip()
        normalized_current_company_name = normalize_matching_text(current_company_name)
        candidate_company_names: list[str] = []
        for related_message in related_messages:
            explicit_company_name = extract_company_name_from_email_message(related_message)
            if explicit_company_name:
                candidate_company_names.append(explicit_company_name)
                continue

            sender_email_address = resolve_message_sender_email(related_message)
            sender_email_domain = extract_email_domain(sender_email_address)
            sender_header_company_hint = infer_company_hint_from_sender_header(
                str(related_message.get("from", "") or "")
            )
            sender_domain_company_hint = infer_company_hint_from_sender_domain(sender_email_domain).title()
            fallback_company_name = sender_header_company_hint or sender_domain_company_hint
            if fallback_company_name:
                candidate_company_names.append(fallback_company_name)

        for candidate_company_name in candidate_company_names:
            normalized_candidate_company_name = normalize_matching_text(candidate_company_name)
            if not normalized_candidate_company_name:
                continue
            if normalized_candidate_company_name == normalized_current_company_name:
                continue
            return candidate_company_name
        return ""

    def _find_best_related_status(
        self,
        application_row: ApplicationRecord,
        base_query: str,
    ) -> str:
        """
        Search related Gmail messages and recover the strongest status signal for one application.

        Inputs:
        - application_row: Existing or newly built application record that may need a status update.
        - base_query: Configured Gmail query used to keep the search within the user's job-mail corpus.

        Outputs:
        - Strongest recovered status, or an empty string when no stronger status is found.

        Edge cases:
        - Rows without company or contact signals return an empty string immediately.
        - Searches a bounded number of related messages to avoid excessive API use.

        Atomicity / concurrency:
        - Read-only Gmail search and parsing flow with no local side effects.
        """
        related_messages = self._load_related_messages_for_backfill(
            application_row,
            base_query=base_query,
        )

        strongest_status = str(application_row.get("status", "Applied") or "Applied")
        for related_message in related_messages:
            candidate_status = extract_status_from_email_message(related_message)
            if candidate_status and STATUS_RANK.get(candidate_status, 0) > STATUS_RANK.get(strongest_status, 0):
                strongest_status = candidate_status
        return strongest_status if strongest_status != str(application_row.get("status", "Applied") or "Applied") else ""

    def _backfill_statuses_from_gmail(
        self,
        application_rows: list[ApplicationRecord],
        base_query: str,
    ) -> list[ApplicationRecord]:
        """
        Enrich application rows with stronger statuses found in related Gmail history.

        Inputs:
        - application_rows: Application rows that may need status recovery.
        - base_query: Configured Gmail query used as the search baseline for related-email lookups.

        Outputs:
        - New list of application rows with any recovered statuses applied.

        Edge cases:
        - Terminal states are left unchanged because they already represent a completed outcome.
        - Gmail lookup failures are logged per row and do not stop the broader sync.

        Atomicity / concurrency:
        - Performs independent read-only Gmail searches and returns an in-memory copy of the
          provided rows without mutating the caller's list.
        """
        enriched_application_rows: list[ApplicationRecord] = []
        recovered_status_count = 0
        terminal_statuses = {"Rejected", "Withdrawn", "Offer", "Ghosted"}

        for application_row in application_rows:
            normalized_application_row = dict(application_row)
            application_source = str(normalized_application_row.get("source", "") or "").strip().lower()
            current_status = str(normalized_application_row.get("status", "Applied") or "Applied")
            if application_source != "email" or current_status in terminal_statuses:
                enriched_application_rows.append(normalized_application_row)
                continue

            try:
                recovered_status = self._find_best_related_status(
                    normalized_application_row,
                    base_query=base_query,
                )
            except TrackerError as error:
                console.print(
                    "[yellow]  Status backfill skipped for "
                    f"{normalized_application_row.get('appl_id', 'unknown')}: {error}[/yellow]"
                )
                enriched_application_rows.append(normalized_application_row)
                continue

            if recovered_status and STATUS_RANK.get(recovered_status, 0) > STATUS_RANK.get(current_status, 0):
                normalized_application_row["status"] = recovered_status
                recovered_status_count += 1
            enriched_application_rows.append(normalized_application_row)

        if recovered_status_count > 0:
            console.print(
                f"  Recovered stronger statuses from Gmail history: "
                f"[green]{recovered_status_count}[/green]"
            )
        return enriched_application_rows

    def _backfill_missing_companies_from_gmail(
        self,
        application_rows: list[ApplicationRecord],
        base_query: str,
    ) -> list[ApplicationRecord]:
        """
        Enrich application rows whose company looks missing or mismatched using Gmail history.

        Inputs:
        - application_rows: Application rows that may need company recovery.
        - base_query: Configured Gmail query used as the search baseline for related-email lookups.

        Outputs:
        - New list of application rows with any recovered company names applied.

        Edge cases:
        - Only email-sourced rows are considered.
        - Confident company names are left untouched unless the row also carries a low-quality role
          subject that strongly suggests the row was parsed from an ATS receipt.
        - Gmail lookup failures are logged per row and do not stop the broader sync.

        Atomicity / concurrency:
        - Performs independent read-only Gmail searches and returns an in-memory copy of the
          provided rows without mutating the caller's list.
        """
        enriched_application_rows: list[ApplicationRecord] = []
        recovered_company_count = 0
        for application_row in application_rows:
            normalized_application_row = dict(application_row)
            application_source = str(normalized_application_row.get("source", "") or "").strip().lower()
            current_company_name = str(normalized_application_row.get("company", "") or "").strip()
            current_role_title = str(normalized_application_row.get("role", "") or "").strip()
            should_attempt_company_backfill = (
                application_source == "email"
                and (
                    not current_company_name
                    or is_low_confidence_company_name(current_company_name)
                    or is_low_quality_role_title(current_role_title)
                )
            )
            if not should_attempt_company_backfill:
                enriched_application_rows.append(normalized_application_row)
                continue

            try:
                recovered_company_name = self._find_best_related_company_name(
                    normalized_application_row,
                    base_query=base_query,
                )
            except TrackerError as error:
                console.print(
                    "[yellow]  Company backfill skipped for "
                    f"{normalized_application_row.get('appl_id', 'unknown')}: {error}[/yellow]"
                )
                enriched_application_rows.append(normalized_application_row)
                continue

            if (
                recovered_company_name
                and not is_low_confidence_company_name(recovered_company_name)
                and normalize_matching_text(recovered_company_name)
                != normalize_matching_text(current_company_name)
            ):
                normalized_application_row["company"] = recovered_company_name
                recovered_company_count += 1
            enriched_application_rows.append(normalized_application_row)

        if recovered_company_count > 0:
            console.print(
                f"  Recovered company names from Gmail history: "
                f"[green]{recovered_company_count}[/green]"
            )
        return enriched_application_rows

    def _backfill_missing_roles_from_gmail(
        self,
        application_rows: list[ApplicationRecord],
        base_query: str,
    ) -> list[ApplicationRecord]:
        """
        Enrich application rows whose role is missing or stage-like using related Gmail history.

        Inputs:
        - application_rows: Application rows that may need role recovery.
        - base_query: Configured Gmail query used as the search baseline for related-email lookups.

        Outputs:
        - New list of application rows with any recovered role titles applied.

        Edge cases:
        - Only email-sourced rows with a missing or stage-like role are searched.
        - Rows with already-strong role titles are returned unchanged.
        - Gmail lookup failures are logged per row and do not stop the broader sync.

        Atomicity / concurrency:
        - Performs independent read-only Gmail searches and returns an in-memory copy of the
          provided rows without mutating the caller's list.
        """
        enriched_application_rows: list[ApplicationRecord] = []
        recovered_role_count = 0
        for application_row in application_rows:
            normalized_application_row = dict(application_row)
            current_role_title = str(normalized_application_row.get("role", "") or "").strip()
            application_source = str(normalized_application_row.get("source", "") or "").strip().lower()
            if application_source != "email" or not is_low_quality_role_title(current_role_title):
                enriched_application_rows.append(normalized_application_row)
                continue

            try:
                recovered_role_title = self._find_best_related_role_title(
                    normalized_application_row,
                    base_query=base_query,
                )
            except TrackerError as error:
                console.print(
                    "[yellow]  Role backfill skipped for "
                    f"{normalized_application_row.get('appl_id', 'unknown')}: {error}[/yellow]"
                )
                enriched_application_rows.append(normalized_application_row)
                continue

            preferred_role_title = choose_preferred_role_title(
                current_role_title,
                recovered_role_title,
            )
            if preferred_role_title and preferred_role_title != current_role_title:
                normalized_application_row["role"] = preferred_role_title
                recovered_role_count += 1
            enriched_application_rows.append(normalized_application_row)

        if recovered_role_count > 0:
            console.print(
                f"  Recovered missing role titles from Gmail history: "
                f"[green]{recovered_role_count}[/green]"
            )
        return enriched_application_rows

    def _find_related_resync_message_ids(
        self,
        application_row: ApplicationRecord,
        base_query: str,
    ) -> list[str]:
        """
        Find a bounded set of Gmail message IDs that likely belong to one application resync.

        Inputs:
        - application_row: Existing application row being resynced.
        - base_query: Configured Gmail query used to keep the search within the user's job-mail corpus.

        Outputs:
        - Ordered Gmail message IDs that appear related to the target application.

        Edge cases:
        - Falls back to the row's stored `email_ids` when no related search terms are available.
        - Prefers a strong role match when the row has a concrete role title, but still keeps
          exact stored message IDs so previously-linked messages are never lost.

        Atomicity / concurrency:
        - Read-only Gmail search and in-memory filtering with no local side effects.
        """
        stored_email_ids = deduplicate_preserving_order(cast(list[str], safe_json_loads(
            application_row.get("email_ids") or "[]",
            f"email_ids for appl_id {application_row.get('appl_id', 'unknown')}",
            [],
        )))
        stored_thread_ids = deduplicate_preserving_order(cast(list[str], safe_json_loads(
            application_row.get("thread_ids") or "[]",
            f"thread_ids for appl_id {application_row.get('appl_id', 'unknown')}",
            [],
        )))
        stored_thread_messages = self.gmail.get_messages_by_thread_ids(stored_thread_ids)
        stored_thread_message_ids = deduplicate_preserving_order([
            str(thread_message.get("id", "") or "").strip()
            for thread_message in stored_thread_messages
            if str(thread_message.get("id", "") or "").strip()
        ])
        related_search_terms = build_related_gmail_search_terms(application_row)
        if not related_search_terms:
            return deduplicate_preserving_order(stored_email_ids + stored_thread_message_ids)

        related_query = f"({base_query}) ({' OR '.join(related_search_terms)})"
        related_messages = self.gmail.search_messages(
            query=related_query,
            since_days=ROLE_BACKFILL_LOOKBACK_DAYS,
            max_results=RESYNC_APP_MAX_RELATED_MESSAGES,
        )

        target_company = normalize_matching_text(application_row.get("company", ""))
        target_role = str(application_row.get("role", "") or "").strip()
        target_role_is_strong = bool(target_role) and not is_stage_like_role_title(target_role)

        related_message_ids: list[str] = []
        for related_message in related_messages:
            related_message_id = str(related_message.get("id", "") or "").strip()
            if not related_message_id:
                continue

            extracted_company = normalize_matching_text(
                infer_company_hint_from_sender_header(str(related_message.get("from", "") or ""))
                or infer_company_hint_from_sender_domain(
                    extract_email_domain(extract_email_address(str(related_message.get("from", "") or "")))
                )
            )
            extracted_roles = extract_role_titles_from_email_message(related_message)
            has_matching_company = bool(target_company) and extracted_company == target_company
            has_matching_role = any(
                normalize_matching_text(extracted_role) == normalize_matching_text(target_role)
                for extracted_role in extracted_roles
            )

            if related_message_id in stored_email_ids:
                related_message_ids.append(related_message_id)
                continue
            if target_role_is_strong and has_matching_company and has_matching_role:
                related_message_ids.append(related_message_id)
                continue
            if not target_role_is_strong and has_matching_company:
                related_message_ids.append(related_message_id)

        return deduplicate_preserving_order(
            stored_email_ids + stored_thread_message_ids + related_message_ids
        )

    # ── sync ──────────────────────────────────────────────────────────────────
    def sync(self, continue_to_digest: bool = False):
        """
        Sync Gmail messages into the Google Sheet and group them into application records.

        Inputs:
        - None. Uses configured Gmail query, Gemini model, and Sheets destination.

        Outputs:
        - Writes grouped application updates to Google Sheets.

        Edge cases:
        - Already-seen email IDs are skipped before AI processing.
        - AI grouping failures save a resumable local run-state snapshot and abort before
          Sheets or digest actions can observe stale application state.

        Atomicity / concurrency:
        - Single-process sync flow. AI grouping writes local run-state checkpoints between
          clusters, but Sheets are only updated after the full run completes successfully.
        """
        console.rule("[bold blue]Syncing Gmail → Sheets")
        gcfg    = self.cfg["gmail"]
        query   = gcfg.get("query", "")
        lookback = gcfg.get("lookback_days", 90)
        gemini_config = self.cfg.get("gemini", {})
        processed_label_name = self.processing_labels.get_stage_label_name("processed")
        processed_label_query_name = build_gmail_label_query_name(processed_label_name)
        state_retention_days = max(30, int(lookback))

        existing, merged_duplicate_sheet_row_count = self.sheets.consolidate_similar_applications()
        console.print(f"  Existing records in Sheets: [green]{len(existing)}[/green]")
        if merged_duplicate_sheet_row_count > 0:
            console.print(
                f"  Consolidated [green]{merged_duplicate_sheet_row_count}[/green] duplicate sheet rows "
                "before AI grouping."
            )
        authenticated_gmail_address = self.gmail.get_profile_email()
        if authenticated_gmail_address:
            console.print(
                f"  Authenticated Gmail account: [cyan]{authenticated_gmail_address}[/cyan]"
            )

        # Filter already-seen email IDs
        existing_sheet_message_ids: set[str] = set()
        for a in existing:
            email_ids = cast(list[str], safe_json_loads(
                a.get("email_ids") or "[]",
                f"email_ids for appl_id {a.get('appl_id', 'unknown')}",
                [],
            ))
            existing_sheet_message_ids.update(email_ids)

        processed_message_state_snapshot = self.processed_message_state.load(
            retention_days=state_retention_days
        )
        tracked_message_ids = (
            set(processed_message_state_snapshot.processed_at_by_message_id)
            | existing_sheet_message_ids
        )
        self.gmail.ensure_processing_labels(self.processing_labels)

        filtered_query = f"({query}) -label:{processed_label_query_name}"
        console.print(f"  Query: [cyan]{query}[/cyan]  |  Lookback: [cyan]{lookback}d[/cyan]")
        console.print(f"  Processing label: [cyan]{processed_label_name}[/cyan]")
        gmail_fetch_result = self.gmail.get_emails(
            query=filtered_query,
            since_days=lookback,
            seen_message_ids=tracked_message_ids,
        )
        new_emails = sorted(
            gmail_fetch_result.unseen_emails,
            key=lambda email_message: (
                int(email_message.get("timestamp") or 0),
                str(email_message.get("id", "") or ""),
            ),
        )
        console.print(
            f"  Unprocessed Gmail matches in window: "
            f"[green]{gmail_fetch_result.total_matching_email_count}[/green]"
        )
        console.print(f"  New unseen emails: [green]{len(new_emails)}[/green]")

        if not new_emails:
            console.print("  [yellow]Nothing new to process.[/yellow]")
            console.print(f"  Sheet: [link={self.sheets.get_spreadsheet_url()}]{self.sheets.get_spreadsheet_url()}[/link]")
            return

        configured_transport_window_size = gemini_config.get(
            "grouping_batch_size",
            DEFAULT_AI_GROUPING_BATCH_SIZE,
        )
        configured_cluster_size_limit = gemini_config.get(
            "grouping_cluster_size",
            DEFAULT_AI_GROUPING_CLUSTER_SIZE,
        )
        configured_cluster_emergency_margin = gemini_config.get(
            "grouping_cluster_emergency_margin",
            DEFAULT_AI_GROUPING_CLUSTER_EMERGENCY_MARGIN,
        )
        transport_window_size = max(1, int(configured_transport_window_size))
        cluster_size_limit = max(1, int(configured_cluster_size_limit))
        cluster_emergency_margin = max(0, int(configured_cluster_emergency_margin))
        clusters = self._build_grouping_clusters(
            new_emails=new_emails,
            existing_apps=existing,
            transport_window_size=transport_window_size,
            cluster_size_limit=cluster_size_limit,
            cluster_emergency_margin=cluster_emergency_margin,
        )
        run_state: JsonObject = {
            "schema_version": 1,
            "run_id": self._new_grouping_run_id(),
            "created_at": datetime.now(timezone.utc).isoformat(),
            "continue_to_digest": bool(continue_to_digest),
            "query": str(query or ""),
            "lookback_days": int(lookback),
            "state_retention_days": state_retention_days,
            "transport_window_size": transport_window_size,
            "cluster_size_limit": cluster_size_limit,
            "cluster_emergency_margin": cluster_emergency_margin,
            "existing_apps": copy.deepcopy(existing),
            "initial_existing_appl_ids": sorted({
                str(app.get("appl_id", "") or "").strip()
                for app in existing
                if str(app.get("appl_id", "") or "").strip()
            }),
            "working_existing": copy.deepcopy(existing),
            "new_emails": copy.deepcopy(new_emails),
            "clusters": clusters,
            "all_updates": [],
            "ignored_email_count": 0,
            "handled_message_ids": [],
            "matched_existing_email_count": 0,
            "new_application_email_count": 0,
            "updated_existing_application_ids": [],
            "created_application_ids": [],
        }
        self._save_grouping_run_state(run_state)
        self._execute_grouping_run(run_state)
        self._finalize_completed_sync_run(run_state)

    def full_reset(self, skip_confirmation: bool = False) -> None:
        """
        Perform a destructive tracker reset and immediately rebuild from Gmail.

        Inputs:
        - skip_confirmation: When True, do not prompt before deleting tracker-managed data.

        Outputs:
        - Clears tracker-managed state in Gmail, Google Sheets, and local files, then runs
          a fresh sync using the current configuration.

        Edge cases:
        - Missing local files and missing Gmail labels are ignored so repeated resets remain
          safe to rerun.
        - Only tracker-managed Gmail labels configured in `gmail.processing_labels` are deleted.
        - Pending Gmail drafts referenced by `pending_actions.json` are deleted when possible.

        Atomicity / concurrency:
        - This is a multi-system destructive workflow across Gmail, Google Sheets, and the
          local filesystem. It is intentionally not atomic; a later step may fail after
          earlier destructive steps already succeeded.
        """
        if not skip_confirmation:
            should_continue = Confirm.ask(
                "[bold red]Full reset will delete tracker-managed Gmail labels, clear the "
                "Applications sheet, remove pending drafts referenced by pending_actions.json, "
                "and delete local tracker state. Continue?[/bold red]",
                default=False,
            )
            if not should_continue:
                console.print("[yellow]Full reset cancelled.[/yellow]")
                return

        console.rule("[bold red]Full Reset")
        self._delete_pending_gmail_drafts()

        deleted_label_count = self.gmail.delete_user_labels(
            self.processing_labels.all_label_names()
        )
        console.print(f"  Deleted Gmail processing labels: [green]{deleted_label_count}[/green]")

        self.sheets.reset_applications_sheet()
        console.print("  Reset Applications sheet to headers only.")

        self.processed_message_state.delete()
        console.print("  Deleted local processed-message state.")

        self._delete_pending_actions_file()
        console.print("  Deleted pending actions file.")

        console.print("  Rebuilding tracker state from Gmail …")
        self.sync()

    def _clear_processed_tracker_state_for_message_ids(self, message_ids: list[str]) -> int:
        """
        Remove tracker-managed Gmail processed state for one or more message IDs.

        Inputs:
        - message_ids: Gmail message IDs that should be eligible for reprocessing.

        Outputs:
        - Count of distinct non-empty message IDs that were targeted.

        Edge cases:
        - Blank and duplicate message IDs are ignored.
        - Missing Gmail processed labels are tolerated so the cleanup remains idempotent.

        Atomicity / concurrency:
        - Multi-step workflow across Gmail labels and local state. It is not atomic.
        """
        normalized_message_ids = deduplicate_preserving_order([
            str(message_id or "").strip()
            for message_id in message_ids
            if str(message_id or "").strip()
        ])
        if not normalized_message_ids:
            return 0

        processed_label_name = self.processing_labels.get_stage_label_name("processed")
        user_label_ids_by_name = self.gmail.get_user_label_ids_by_name()
        processed_label_id = user_label_ids_by_name.get(processed_label_name)
        if processed_label_id:
            self.gmail.remove_labels_from_messages(
                message_ids=normalized_message_ids,
                label_ids=[processed_label_id],
            )
            console.print(
                f"  Removed Gmail processed label from [green]{len(normalized_message_ids)}[/green] messages."
            )
        else:
            console.print(
                f"  Gmail processed label [cyan]{processed_label_name}[/cyan] was not present."
            )

        lookback_days = int(self.cfg["gmail"].get("lookback_days", 90))
        state_retention_days = max(30, lookback_days)
        processed_message_state_snapshot = self.processed_message_state.load(
            retention_days=state_retention_days
        )
        self.processed_message_state.remove_processed_message_ids(
            existing_snapshot=processed_message_state_snapshot,
            message_ids=normalized_message_ids,
            retention_days=state_retention_days,
        )
        console.print("  Cleared local processed-message state.")
        return len(normalized_message_ids)

    def resync_application(self, appl_id: str, skip_confirmation: bool = False) -> None:
        """
        Force one email-backed application to be reprocessed from its original Gmail messages.

        Inputs:
        - appl_id: Application identifier currently stored in the sheet.
        - skip_confirmation: When True, do not prompt before deleting the application row.

        Outputs:
        - Removes tracker state for the selected application and then runs a normal sync.

        Edge cases:
        - Applications without stored Gmail message IDs cannot be resynced because the tracker
          has no stable email identity to reprocess.
        - Missing Gmail processed labels are tolerated so the flow remains idempotent.

        Atomicity / concurrency:
        - This is a multi-system workflow across Google Sheets, Gmail labels, and local state.
          It is intentionally not atomic; a later step may fail after earlier cleanup succeeds.
        """
        normalized_appl_id = str(appl_id or "").strip()
        if not normalized_appl_id:
            raise TrackerError("resync-app requires a non-empty appl_id")

        existing_applications = self.sheets.get_all()
        target_application = next(
            (application for application in existing_applications if application.get("appl_id") == normalized_appl_id),
            None,
        )
        if target_application is None:
            raise TrackerError(f"Application '{normalized_appl_id}' was not found in the sheet")

        stored_email_ids = cast(list[str], safe_json_loads(
            target_application.get("email_ids") or "[]",
            f"email_ids for appl_id {normalized_appl_id}",
            [],
        ))
        if not stored_email_ids:
            raise TrackerError(
                f"Application '{normalized_appl_id}' has no stored Gmail message IDs, so it cannot be resynced"
            )

        base_query = str(self.cfg["gmail"].get("query", "") or "")
        related_message_ids = self._find_related_resync_message_ids(
            target_application,
            base_query=base_query,
        )

        company_name = str(target_application.get("company", "") or "").strip() or "Unknown company"
        role_title = str(target_application.get("role", "") or "").strip() or "Unknown role"
        if not skip_confirmation:
            should_continue = Confirm.ask(
                "[bold yellow]Resync application "
                f"[cyan]{normalized_appl_id}[/cyan] ({company_name} — {role_title})? "
                "This will delete the current sheet row, remove tracker processed labels from its "
                "stored and related Gmail messages, forget their local processed state, and then run sync again.[/bold yellow]",
                default=False,
            )
            if not should_continue:
                console.print("[yellow]Application resync cancelled.[/yellow]")
                return

        console.rule("[bold yellow]Resync Application")
        console.print(
            f"  Target: [cyan]{normalized_appl_id}[/cyan] — {company_name} — {role_title}"
        )
        console.print(
            f"  Related Gmail messages selected for reprocessing: "
            f"[green]{len(related_message_ids)}[/green] "
            f"(stored: [green]{len(deduplicate_preserving_order(stored_email_ids))}[/green])"
        )

        deleted_application = self.sheets.delete_application(normalized_appl_id)
        if deleted_application is None:
            raise TrackerError(
                f"Application '{normalized_appl_id}' disappeared from the sheet before resync could begin"
            )
        console.print("  Deleted current application row from Sheets.")

        self._clear_processed_tracker_state_for_message_ids(related_message_ids)

        console.print("  Loading selected Gmail messages for direct rebuild …")
        email_messages = self.gmail.get_messages_by_ids(related_message_ids)
        console.print(f"  Loaded [green]{len(email_messages)}[/green] Gmail messages for rebuild.")
        if not email_messages:
            raise TrackerError(
                f"Resync for '{normalized_appl_id}' selected no readable Gmail messages after cleanup"
            )

        gemini_config = self.cfg.get("gemini", {})
        configured_batch_size = gemini_config.get(
            "grouping_batch_size",
            DEFAULT_AI_GROUPING_BATCH_SIZE,
        )
        configured_max_rate_limit_wait_seconds = gemini_config.get(
            "rate_limit_max_wait_seconds",
            DEFAULT_AI_RATE_LIMIT_MAX_WAIT_SECONDS,
        )
        configured_max_rate_limit_retries_per_batch = gemini_config.get(
            "rate_limit_max_retries_per_batch",
            DEFAULT_AI_RATE_LIMIT_MAX_RETRIES_PER_BATCH,
        )
        batch_size = max(1, int(configured_batch_size))
        max_rate_limit_wait_seconds = max(1, int(configured_max_rate_limit_wait_seconds))
        max_rate_limit_retries_per_batch = max(
            0,
            int(configured_max_rate_limit_retries_per_batch),
        )

        processed_label_name = self.processing_labels.get_stage_label_name("processed")
        processing_label_ids_by_name = self.gmail.ensure_processing_labels(self.processing_labels)
        lookback_days = int(self.cfg["gmail"].get("lookback_days", 90))
        state_retention_days = max(30, lookback_days)
        processed_message_state_snapshot = self.processed_message_state.load(
            retention_days=state_retention_days
        )

        working_existing = [
            application
            for application in self.sheets.get_all()
            if application.get("appl_id") != normalized_appl_id
        ]
        existing_appl_ids = {app.get("appl_id", "") for app in working_existing if app.get("appl_id")}
        all_updates: list[ApplicationRecord] = []
        total_batch_count = -(-len(email_messages) // batch_size)
        stopped_due_to_gemini_quota = False
        ignored_email_count = 0
        handled_message_ids: list[str] = []
        matched_existing_email_count = 0
        new_application_email_count = 0
        updated_existing_application_ids: set[str] = set()
        created_application_ids: set[str] = set()

        for i in range(0, len(email_messages), batch_size):
            batch = email_messages[i : i + batch_size]
            batch_number = i // batch_size + 1
            console.print(f"  AI grouping batch {batch_number} / {total_batch_count} …")

            grouping_result = EmailGroupingResult(
                updates=[],
                ignored_email_count=0,
                handled_message_ids=[],
                matched_existing_email_count=0,
                new_application_email_count=0,
                updated_existing_application_count=0,
                created_application_count=0,
            )
            for retry_attempt in range(max_rate_limit_retries_per_batch + 1):
                try:
                    grouping_result = self.ai.group_emails(batch, working_existing)
                    break
                except GeminiRateLimitError as error:
                    retry_delay_seconds = error.retry_delay_seconds or max_rate_limit_wait_seconds
                    if error.is_daily_quota_exhausted:
                        console.print(
                            "[yellow]  Gemini daily quota exhausted. "
                            "Stopping AI grouping for remaining batches.[/yellow]"
                        )
                        stopped_due_to_gemini_quota = True
                        break

                    if retry_attempt >= max_rate_limit_retries_per_batch:
                        console.print(
                            f"[red]  AI grouper error — skipping batch after "
                            f"{max_rate_limit_retries_per_batch + 1} attempts: {error}[/red]"
                        )
                        break

                    if retry_delay_seconds > max_rate_limit_wait_seconds:
                        console.print(
                            "[yellow]  Gemini requested a long retry wait "
                            f"({retry_delay_seconds}s). Stopping AI grouping for remaining batches.[/yellow]"
                        )
                        stopped_due_to_gemini_quota = True
                        break

                    console.print(
                        f"[yellow]  Gemini rate limited this batch. Waiting "
                        f"{retry_delay_seconds}s before retry {retry_attempt + 2} "
                        f"of {max_rate_limit_retries_per_batch + 1}.[/yellow]"
                    )
                    time.sleep(retry_delay_seconds)
                except Exception as error:
                    console.print(f"[red]  AI grouper error — skipping batch: {error}[/red]")
                    break

            if stopped_due_to_gemini_quota:
                break

            ignored_email_count += grouping_result.ignored_email_count
            handled_message_ids.extend(grouping_result.handled_message_ids)
            matched_existing_email_count += grouping_result.matched_existing_email_count
            new_application_email_count += grouping_result.new_application_email_count
            for update in grouping_result.updates:
                appl_id = update.get("appl_id", "")
                if not appl_id:
                    continue
                if appl_id in existing_appl_ids:
                    updated_existing_application_ids.add(appl_id)
                else:
                    created_application_ids.add(appl_id)
            all_updates.extend(grouping_result.updates)
            for update in grouping_result.updates:
                existing_index = next(
                    (j for j, application in enumerate(working_existing) if application.get("appl_id") == update["appl_id"]),
                    None,
                )
                if existing_index is not None:
                    working_existing[existing_index] = update
                else:
                    working_existing.append(update)

        if stopped_due_to_gemini_quota:
            console.print(
                "[yellow]  AI grouping stopped early because Gemini quota was exhausted. "
                "Rerun resync after quota resets or increase Gemini limits.[/yellow]"
            )

        relevant_email_count = matched_existing_email_count + new_application_email_count
        unique_updated_appl_ids = {update["appl_id"] for update in all_updates if update.get("appl_id")}
        merged_email_count = max(0, relevant_email_count - len(unique_updated_appl_ids))
        console.print(
            "  Grouping summary: "
            f"[green]{relevant_email_count}[/green] relevant emails, "
            f"[green]{ignored_email_count}[/green] ignored"
        )
        console.print(
            "  Application impact: "
            f"[green]{len(updated_existing_application_ids)}[/green] existing applications updated, "
            f"[green]{len(created_application_ids)}[/green] new applications created, "
            f"[green]{merged_email_count}[/green] emails merged into shared records"
        )

        all_updates = self._backfill_statuses_from_gmail(
            all_updates,
            base_query=base_query,
        )
        all_updates = self._backfill_missing_companies_from_gmail(
            all_updates,
            base_query=base_query,
        )
        all_updates = self._backfill_missing_roles_from_gmail(
            all_updates,
            base_query=base_query,
        )
        console.print(f"  Upserting [green]{len(all_updates)}[/green] records …")
        self.sheets.upsert_many(all_updates)

        finalized_message_ids = deduplicate_preserving_order(handled_message_ids)
        state_update_error: Optional[TrackerError] = None
        try:
            processed_message_state_snapshot = self.processed_message_state.record_processed_message_ids(
                existing_snapshot=processed_message_state_snapshot,
                message_ids=finalized_message_ids,
                retention_days=state_retention_days,
            )
        except TrackerError as error:
            state_update_error = error

        label_update_error: Optional[TrackerError] = None
        try:
            self.gmail.apply_labels_to_messages(
                message_ids=finalized_message_ids,
                label_ids=[
                    label_id
                    for label_id in [
                        processing_label_ids_by_name.get(self.processing_labels.root_label_name),
                        processing_label_ids_by_name.get(processed_label_name),
                    ]
                    if label_id
                ],
            )
        except TrackerError as error:
            label_update_error = error

        if state_update_error or label_update_error:
            error_messages = [
                str(error)
                for error in [state_update_error, label_update_error]
                if error is not None
            ]
            raise TrackerError(
                "Resync upserted Sheets successfully, but failed to finalize processed-message state:\n"
                + "\n".join(f"- {message}" for message in error_messages)
            )

        console.print("[bold green]  OK Resync rebuild complete[/bold green]")
        console.print(f"  Sheet: [link={self.sheets.get_spreadsheet_url()}]{self.sheets.get_spreadsheet_url()}[/link]")

    def resync_all(self, skip_confirmation: bool = False) -> None:
        """
        Reprocess every email-backed application from Gmail without performing a full reset.

        Inputs:
        - skip_confirmation: When True, do not prompt before deleting the current email-backed rows.

        Outputs:
        - Deletes current email-backed sheet rows, clears processed Gmail state for their stored
          message IDs, and runs a normal sync to rebuild them from Gmail.

        Edge cases:
        - Manual non-email rows such as LinkedIn applications are preserved.
        - Email-backed rows without stored Gmail message IDs are deleted from the sheet but cannot
          contribute message IDs to the reprocessing cleanup.
        - Missing Gmail processed labels are tolerated so the flow remains idempotent.

        Atomicity / concurrency:
        - Multi-system workflow across Google Sheets, Gmail labels, and local state. It is
          intentionally not atomic; a later step may fail after earlier cleanup succeeds.
        """
        existing_applications = self.sheets.get_all()
        email_backed_applications = [
            application
            for application in existing_applications
            if str(application.get("source", "") or "").strip().lower() == "email"
        ]
        if not email_backed_applications:
            console.print("[yellow]No email-backed applications found to resync.[/yellow]")
            return

        appl_ids_to_resync = [
            str(application.get("appl_id", "") or "").strip()
            for application in email_backed_applications
            if str(application.get("appl_id", "") or "").strip()
        ]
        message_ids_to_resync: list[str] = []
        rows_without_message_ids = 0
        for application in email_backed_applications:
            email_ids = cast(list[str], safe_json_loads(
                application.get("email_ids") or "[]",
                f"email_ids for appl_id {application.get('appl_id', 'unknown')}",
                [],
            ))
            if email_ids:
                message_ids_to_resync.extend(email_ids)
            else:
                rows_without_message_ids += 1

        if not skip_confirmation:
            should_continue = Confirm.ask(
                "[bold yellow]Resync all email-backed applications? "
                f"This will delete [cyan]{len(appl_ids_to_resync)}[/cyan] current sheet rows, "
                "remove tracker processed labels from their Gmail messages when available, "
                "forget their local processed state, and then run sync again. "
                "Manual non-email rows will be kept.[/bold yellow]",
                default=False,
            )
            if not should_continue:
                console.print("[yellow]Resync all cancelled.[/yellow]")
                return

        console.rule("[bold yellow]Resync All Email-Backed Applications")
        console.print(
            f"  Targeting [green]{len(appl_ids_to_resync)}[/green] email-backed rows."
        )
        if rows_without_message_ids > 0:
            console.print(
                f"  Rows without stored Gmail message IDs: [yellow]{rows_without_message_ids}[/yellow]"
            )

        deleted_applications = self.sheets.delete_applications(appl_ids_to_resync)
        console.print(
            f"  Deleted [green]{len(deleted_applications)}[/green] current email-backed rows from Sheets."
        )

        cleared_message_count = self._clear_processed_tracker_state_for_message_ids(message_ids_to_resync)
        if cleared_message_count == 0:
            console.print("  No stored Gmail message IDs were available to clear.")

        console.print("  Running sync to rebuild applications from Gmail …")
        self.sync()

    def delete_applications(self, appl_ids: list[str], skip_confirmation: bool = False) -> None:
        """
        Delete one or more applications and clear their tracker-managed processed state.

        Inputs:
        - appl_ids: Application identifiers to remove from the sheet.
        - skip_confirmation: When True, do not prompt before deleting rows.

        Outputs:
        - Removes the matching rows from Google Sheets and forgets their processed Gmail state.

        Edge cases:
        - Blank and duplicate appl_ids are ignored.
        - Missing appl_ids cause the command to stop before making changes.
        - Applications without stored Gmail message IDs still have their sheet rows deleted.

        Atomicity / concurrency:
        - This is a multi-system destructive workflow across Google Sheets, Gmail labels, and
          local processed state. It intentionally does not run sync after cleanup.
        """
        normalized_appl_ids = deduplicate_preserving_order([
            str(appl_id or "").strip()
            for appl_id in appl_ids
            if str(appl_id or "").strip()
        ])
        if not normalized_appl_ids:
            raise TrackerError("delete-app requires at least one non-empty appl_id")

        existing_applications = self.sheets.get_all()
        existing_applications_by_id = {
            application.get("appl_id", ""): application
            for application in existing_applications
            if application.get("appl_id")
        }
        missing_appl_ids = [
            appl_id
            for appl_id in normalized_appl_ids
            if appl_id not in existing_applications_by_id
        ]
        if missing_appl_ids:
            raise TrackerError(
                "delete-app could not find these appl_ids in the sheet: "
                + ", ".join(missing_appl_ids)
            )

        if not skip_confirmation:
            application_summaries = "\n".join(
                f"  - {appl_id}: "
                f"{existing_applications_by_id[appl_id].get('company', '')} — "
                f"{existing_applications_by_id[appl_id].get('role', '')}"
                for appl_id in normalized_appl_ids
            )
            should_continue = Confirm.ask(
                "[bold red]Delete these applications and clear their tracker processed state? "
                "This removes the sheet rows, removes the Gmail processed label from their "
                "stored message IDs, and clears local processed-message state. It does not run sync.[/bold red]\n"
                f"{application_summaries}",
                default=False,
            )
            if not should_continue:
                console.print("[yellow]Application delete cancelled.[/yellow]")
                return

        console.rule("[bold red]Delete Applications")
        deleted_applications = self.sheets.delete_applications(normalized_appl_ids)
        deleted_applications_by_id = {
            application.get("appl_id", ""): application
            for application in deleted_applications
            if application.get("appl_id")
        }
        deleted_message_ids = deduplicate_preserving_order([
            message_id
            for deleted_application in deleted_applications
            for message_id in cast(list[str], safe_json_loads(
                deleted_application.get("email_ids") or "[]",
                f"email_ids for appl_id {deleted_application.get('appl_id', 'unknown')}",
                [],
            ))
            if str(message_id).strip()
        ])

        processed_label_name = self.processing_labels.get_stage_label_name("processed")
        user_label_ids_by_name = self.gmail.get_user_label_ids_by_name()
        processed_label_id = user_label_ids_by_name.get(processed_label_name)
        if processed_label_id and deleted_message_ids:
            self.gmail.remove_labels_from_messages(
                message_ids=deleted_message_ids,
                label_ids=[processed_label_id],
            )
            console.print(
                f"  Removed Gmail processed label from [green]{len(deleted_message_ids)}[/green] messages."
            )
        elif deleted_message_ids:
            console.print(
                f"  Gmail processed label [cyan]{processed_label_name}[/cyan] was not present."
            )

        lookback_days = int(self.cfg["gmail"].get("lookback_days", 90))
        state_retention_days = max(30, lookback_days)
        processed_message_state_snapshot = self.processed_message_state.load(
            retention_days=state_retention_days
        )
        self.processed_message_state.remove_processed_message_ids(
            existing_snapshot=processed_message_state_snapshot,
            message_ids=deleted_message_ids,
            retention_days=state_retention_days,
        )
        if deleted_message_ids:
            console.print("  Cleared local processed-message state for the deleted applications.")

        for appl_id in normalized_appl_ids:
            deleted_application = deleted_applications_by_id.get(appl_id)
            if deleted_application is None:
                continue
            console.print(
                f"  Deleted [cyan]{appl_id}[/cyan] — "
                f"{deleted_application.get('company', '')} — {deleted_application.get('role', '')}"
            )
        console.print(
            f"[bold green]  ✓ Deleted [green]{len(deleted_applications)}[/green] application rows[/bold green]"
        )

    def _delete_pending_gmail_drafts(self) -> None:
        """
        Delete Gmail drafts referenced by the local pending-actions file.

        Inputs:
        - None. Reads `pending_actions.json` when present.

        Outputs:
        - None. Gmail drafts referenced by pending actions are deleted when they still exist.

        Edge cases:
        - Missing pending-actions files are ignored.
        - Blank or duplicate draft IDs are ignored.
        - If the pending-actions file is malformed, the reset stops with context instead of
          silently skipping draft cleanup.

        Atomicity / concurrency:
        - Reads the pending-actions file once, then performs independent Gmail draft deletions.
          Later deletions may fail after earlier ones already succeeded.
        """
        if not PENDING_PATH.exists():
            return

        try:
            pending_actions = cast(list[PendingActionRecord], safe_json_loads(
                PENDING_PATH.read_text(encoding="utf-8"),
                f"pending actions file {PENDING_PATH}",
                [],
            ))
        except OSError as error:
            raise TrackerError(f"Failed to read pending actions from {PENDING_PATH}: {error}") from error

        pending_draft_ids = deduplicate_preserving_order([
            str(pending_action.get("draft_id", "")).strip()
            for pending_action in pending_actions
            if str(pending_action.get("draft_id", "")).strip()
        ])
        for pending_draft_id in pending_draft_ids:
            self.gmail.delete_draft(pending_draft_id)

        if pending_draft_ids:
            console.print(f"  Deleted pending Gmail drafts: [green]{len(pending_draft_ids)}[/green]")

    def _clear_pending_action_draft_references(self) -> None:
        """
        Clear stored sheet draft references for applications listed in pending_actions.json.

        Inputs:
        - None. Reads `pending_actions.json` when present.

        Outputs:
        - None. Matching application rows have their `draft_id` field cleared.

        Edge cases:
        - Missing pending-actions files are ignored.
        - Missing appl_ids and already-empty draft fields are skipped.

        Atomicity / concurrency:
        - Reads the pending-actions file once, then performs independent sheet field updates.
          Later updates may fail after earlier ones already succeeded.
        """
        if not PENDING_PATH.exists():
            return

        try:
            pending_actions = cast(list[PendingActionRecord], safe_json_loads(
                PENDING_PATH.read_text(encoding="utf-8"),
                f"pending actions file {PENDING_PATH}",
                [],
            ))
        except OSError as error:
            raise TrackerError(f"Failed to read pending actions from {PENDING_PATH}: {error}") from error

        pending_appl_ids = deduplicate_preserving_order([
            str(pending_action.get("appl_id", "")).strip()
            for pending_action in pending_actions
            if str(pending_action.get("appl_id", "")).strip()
        ])
        for pending_appl_id in pending_appl_ids:
            self.sheets.set_field(pending_appl_id, "draft_id", "")

        if pending_appl_ids:
            console.print(
                f"  Cleared stale sheet draft references: [green]{len(pending_appl_ids)}[/green]"
            )

    def _delete_pending_actions_file(self) -> None:
        """
        Delete the local pending-actions file.

        Inputs:
        - None.

        Outputs:
        - None. The file is removed when it exists.

        Edge cases:
        - Missing files are ignored because there is nothing to reset.

        Atomicity / concurrency:
        - Performs at most one filesystem delete with no cross-process coordination.
        """
        if not PENDING_PATH.exists():
            return

        try:
            PENDING_PATH.unlink()
        except OSError as error:
            raise TrackerError(f"Failed to delete pending actions file at {PENDING_PATH}: {error}") from error

    def debug_gmail_labels(self):
        """
        Print the authenticated Gmail account and every visible Gmail user label name.

        Inputs:
        - None.

        Outputs:
        - Writes a diagnostic label listing to the console.

        Edge cases:
        - Output reflects the Gmail Labels API exactly, which may differ from the Gmail UI.

        Atomicity / concurrency:
        - Read-only diagnostic flow with no side effects.
        """
        console.rule("[bold blue]Gmail Label Debug")
        authenticated_gmail_address = self.gmail.get_profile_email()
        if authenticated_gmail_address:
            console.print(
                f"  Authenticated Gmail account: [cyan]{authenticated_gmail_address}[/cyan]"
            )

        labels = self.gmail.list_labels()
        user_label_names = [label["name"] for label in labels if label["type"] == "USER"]
        console.print(f"  Gmail API returned [green]{len(labels)}[/green] total labels")
        console.print(f"  Gmail API returned [green]{len(user_label_names)}[/green] user labels")
        for label in labels:
            console.print(
                f"  - [cyan]{label['name'] or '<empty>'}[/cyan] "
                f"[dim](type={label['type']}, id={label['id']})[/dim]"
            )

    def _debug_email_grouping(
        self,
        email_messages: list[GmailMessage],
        existing_applications: list[ApplicationRecord],
        title: str,
    ) -> None:
        """
        Print detailed AI grouping diagnostics for a specific set of Gmail messages.

        Inputs:
        - email_messages: Parsed Gmail messages to inspect.
        - existing_applications: Existing application rows available for matching context.
        - title: Console title shown for the diagnostic section.

        Outputs:
        - Writes structured debug output to the console.

        Edge cases:
        - Empty email lists print a short diagnostic and return.
        - Missing, duplicate, and unexpected `email_id` values from Gemini are surfaced explicitly.

        Atomicity / concurrency:
        - Read-only diagnostic flow aside from the outbound Gemini request.
        """
        console.rule(f"[bold blue]{title}")
        if not email_messages:
            console.print("[yellow]No email messages were provided for debugging.[/yellow]")
            return

        inspection_result = self.ai.inspect_group_emails(email_messages, existing_applications)
        summaries = cast(list[JsonObject], inspection_result.get("summaries", []))
        results = cast(list[JsonObject], inspection_result.get("results", []))
        missing_email_ids = cast(list[str], inspection_result.get("missing_email_ids", []))
        duplicate_email_ids = cast(list[str], inspection_result.get("duplicate_email_ids", []))
        unexpected_email_ids = cast(list[str], inspection_result.get("unexpected_email_ids", []))

        console.print(f"  Emails inspected: [green]{len(email_messages)}[/green]")
        console.print(f"  Existing applications in context: [green]{len(existing_applications)}[/green]")
        if missing_email_ids:
            console.print(
                f"  [red]Gemini omitted {len(missing_email_ids)} email IDs:[/red] "
                + ", ".join(missing_email_ids)
            )
        if duplicate_email_ids:
            console.print(
                "  [red]Gemini returned duplicate email IDs:[/red] "
                + ", ".join(duplicate_email_ids)
            )
        if unexpected_email_ids:
            console.print(
                "  [red]Gemini returned unexpected email IDs:[/red] "
                + ", ".join(unexpected_email_ids)
            )

        summary_table = Table(title="Email Inputs", show_lines=True, expand=True)
        summary_table.add_column("ID", width=18, style="cyan")
        summary_table.add_column("ATS", width=5)
        summary_table.add_column("Sender Domain", width=28)
        summary_table.add_column("Company Hint", width=16)
        summary_table.add_column("Subject", width=50)
        for summary in summaries:
            summary_table.add_row(
                str(summary.get("id", "")),
                "yes" if summary.get("sender_is_known_ats") else "no",
                str(summary.get("sender_domain", "")),
                str(summary.get("sender_company_hint", "")),
                str(summary.get("subject", "")),
            )
        console.print(summary_table)

        result_table = Table(title="Gemini Results", show_lines=True, expand=True)
        result_table.add_column("Email ID", width=18, style="cyan")
        result_table.add_column("Action", width=14)
        result_table.add_column("App ID", width=12)
        result_table.add_column("Company", width=18)
        result_table.add_column("Role", width=24)
        result_table.add_column("Status", width=12)
        result_table.add_column("ATS", width=5)
        for result in results:
            extracted = cast(JsonObject, result.get("extracted", {}))
            result_table.add_row(
                str(result.get("email_id", "")),
                str(result.get("action", "")),
                str(result.get("appl_id", "")),
                str(extracted.get("company", "")),
                str(extracted.get("role", "")),
                str(extracted.get("status", "")),
                "yes" if extracted.get("is_ats") else "no",
            )
        console.print(result_table)
        console.print("[dim]Raw Gemini response:[/dim]")
        console.print(inspection_result.get("raw_response", ""))

    def debug_app_emails(self, appl_id: str) -> None:
        """
        Debug the AI grouping behavior for one existing application row.

        Inputs:
        - appl_id: Application identifier currently stored in the sheet.

        Outputs:
        - Loads the stored Gmail messages for that row and prints grouping diagnostics.

        Edge cases:
        - Missing application IDs raise TrackerError with guidance to use message-ID debugging.
        - Applications without stored Gmail message IDs cannot be debugged from the sheet row alone.

        Atomicity / concurrency:
        - Read-only diagnostic flow aside from Gmail reads and one outbound Gemini request.
        """
        normalized_appl_id = str(appl_id or "").strip()
        existing_applications = self.sheets.get_all()
        target_application = next(
            (application for application in existing_applications if application.get("appl_id") == normalized_appl_id),
            None,
        )
        if target_application is None:
            raise TrackerError(
                f"Application '{normalized_appl_id}' was not found in the sheet. "
                "If the row was deleted, use --debug-message-ids with the Gmail message IDs instead."
            )

        email_ids = cast(list[str], safe_json_loads(
            target_application.get("email_ids") or "[]",
            f"email_ids for appl_id {normalized_appl_id}",
            [],
        ))
        if not email_ids:
            raise TrackerError(
                f"Application '{normalized_appl_id}' has no stored Gmail message IDs to debug"
            )

        email_messages = self.gmail.get_messages_by_ids(email_ids)
        comparison_applications = [
            application
            for application in existing_applications
            if application.get("appl_id") != normalized_appl_id
        ]
        self._debug_email_grouping(
            email_messages=email_messages,
            existing_applications=comparison_applications,
            title=f"Debug App Emails: {normalized_appl_id}",
        )

    def debug_message_ids(self, message_ids: list[str]) -> None:
        """
        Debug the AI grouping behavior for an arbitrary set of Gmail message IDs.

        Inputs:
        - message_ids: Gmail internal message IDs to inspect.

        Outputs:
        - Loads the requested Gmail messages and prints grouping diagnostics.

        Edge cases:
        - Blank message IDs are ignored.
        - Duplicate message IDs are deduplicated before Gmail fetches.

        Atomicity / concurrency:
        - Read-only diagnostic flow aside from Gmail reads and one outbound Gemini request.
        """
        normalized_message_ids = deduplicate_preserving_order([
            str(message_id or "").strip()
            for message_id in message_ids
            if str(message_id or "").strip()
        ])
        if not normalized_message_ids:
            raise TrackerError("debug-message-ids requires at least one non-empty Gmail message ID")

        email_messages = self.gmail.get_messages_by_ids(normalized_message_ids)
        self._debug_email_grouping(
            email_messages=email_messages,
            existing_applications=self.sheets.get_all(),
            title="Debug Message IDs",
        )

    # ── digest ────────────────────────────────────────────────────────────────
    def _run_digest_after_sync(self) -> None:
        console.rule("[bold blue]Daily Digest")

        self._delete_pending_gmail_drafts()
        self._clear_pending_action_draft_references()
        self._delete_pending_actions_file()

        console.print("  Loading applications from Sheets …")
        apps    = self.sheets.get_all()
        console.print(f"  Evaluating pipeline actions for [green]{len(apps)}[/green] applications …")
        actions = self.engine.compute_actions(apps)
        console.print(f"  Actions due: [green]{len(actions)}[/green]")

        if not actions:
            console.print(Panel("[bold green]🎉 No actions due. You're all caught up![/bold green]",
                                expand=False))
            try:
                PENDING_PATH.write_text("[]", encoding="utf-8")
            except OSError as error:
                raise TrackerError(f"Failed to reset pending actions file at {PENDING_PATH}: {error}") from error
            return

        user_name    = self.cfg["user"]["name"]
        career_email = self.cfg["user"].get("career_email", "").strip()
        pending: list[PendingActionRecord] = []

        tbl = Table(title=f"Actions Due ({len(actions)})", show_lines=True, expand=True)
        tbl.add_column("#",       width=3,  style="dim")
        tbl.add_column("Action",  width=10)
        tbl.add_column("Company", width=18, style="cyan")
        tbl.add_column("Role",    width=22)
        tbl.add_column("Reason",  width=28, style="dim")
        tbl.add_column("Send To", width=32, style="green")
        tbl.add_column("Draft",   width=8,  style="dim")

        console.print("  Preparing draft emails …")
        for i, action in enumerate(actions, 1):
            app      = action["app"]
            atype    = action["type"]
            fu_n     = action.get("follow_up_n", 1)
            console.print(
                f"  Draft {i} / {len(actions)}: "
                f"[cyan]{app.get('company', '')}[/cyan] — {atype}"
            )

            # Resolve target email
            contact, should_search_privacy_contact = self._choose_existing_or_manual_privacy_contact(app, atype)
            if should_search_privacy_contact:
                contact = self._resolve_privacy_contact_after_search_choice(app, atype)
            if not contact:
                contact = self._resolve_company_contact_email(app, atype, log_progress=True)
            if contact:
                self._persist_resolved_contact_email(app, contact, action_type=atype)
            should_create_manual_draft = False

            if atype in ("withdraw", "deletion_request"):
                target = contact
                if atype == "withdraw":
                    subj, body = self.ai.generate_withdrawal(app, user_name)
                else:
                    subj, body = self.ai.generate_deletion_request(app, user_name)
            else:
                target = contact
                subj, body = self.ai.generate_follow_up(app, user_name, fu_n)

            draft_id = ""
            if not (target and "@" in target):
                target, requested_manual_draft, should_skip_action = self._resolve_missing_email_action(
                    app=app,
                    action_type=atype,
                )
                if should_skip_action:
                    console.print(f"  [yellow]Skipping {app.get('company', '')} — {atype}[/yellow]")
                    continue
                should_create_manual_draft = requested_manual_draft

            if target and "@" in target:
                try:
                    draft_id = self.gmail.create_draft(
                        target, subj, body,
                        from_addr=career_email,
                    )
                    self.sheets.set_field(app["appl_id"], "draft_id", draft_id)
                except Exception as e:
                    console.print(f"[red]  Draft failed for {app.get('company')}: {e}[/red]")
            elif should_create_manual_draft:
                try:
                    draft_id = self.gmail.create_draft(
                        "",
                        subj,
                        body,
                        from_addr=career_email,
                    )
                    self.sheets.set_field(app["appl_id"], "draft_id", draft_id)
                except Exception as e:
                    console.print(
                        f"[red]  Draft failed for {app.get('company')} "
                        f"(missing target email): {e}[/red]"
                    )

            pending.append({
                "type":         atype,
                "appl_id":       app["appl_id"],
                "company":      app.get("company"),
                "role":         app.get("role"),
                "target_email": target,
                "subject":      subj,
                "draft_id":     draft_id,
                "follow_up_n":  fu_n,
            })

            color  = "red" if atype == "withdraw" else ("magenta" if atype == "deletion_request" else "yellow")
            if target:
                target_disp = target
            elif should_create_manual_draft:
                target_disp = "[yellow]MANUAL ADDRESS NEEDED[/yellow]"
            else:
                target_disp = "[red]NOT FOUND[/red]"
            tbl.add_row(
                str(i),
                f"[{color}]{atype.upper()}[/{color}]",
                app.get("company", ""),
                app.get("role", ""),
                action["reason"],
                target_disp,
                "✓" if draft_id else "✗",
            )

        console.print(tbl)
        try:
            PENDING_PATH.write_text(json.dumps(pending, indent=2), encoding="utf-8")
        except OSError as error:
            raise TrackerError(f"Failed to write pending actions to {PENDING_PATH}: {error}") from error
        console.print(
            "\n  [dim]Drafts created in Gmail · pending_actions.json saved[/dim]\n"
            "  Run [bold cyan]python tracker.py --confirm[/bold cyan] to review and send."
        )

    def digest(self):
        self.sync(continue_to_digest=True)
        self._run_digest_after_sync()

    # ── confirm ───────────────────────────────────────────────────────────────
    def confirm(self):
        console.rule("[bold blue]Confirm & Send")
        if not PENDING_PATH.exists():
            console.print("[yellow]No pending_actions.json found — run --digest first.[/yellow]")
            return

        try:
            actions = cast(list[PendingActionRecord], safe_json_loads(
                PENDING_PATH.read_text(encoding="utf-8"),
                f"pending actions file {PENDING_PATH}",
                [],
            ))
        except OSError as error:
            raise TrackerError(f"Failed to read pending actions from {PENDING_PATH}: {error}") from error
        if not actions:
            console.print("[green]Nothing pending.[/green]")
            return

        tbl = Table(title="Ready to Send", show_lines=True)
        tbl.add_column("#", width=3, style="dim")
        tbl.add_column("Action",  width=10)
        tbl.add_column("Company", width=20, style="cyan")
        tbl.add_column("Role",    width=22)
        tbl.add_column("To",      width=34, style="green")
        tbl.add_column("Subject", style="dim")

        for i, a in enumerate(actions, 1):
            color = "red" if a["type"] == "withdraw" else ("magenta" if a["type"] == "deletion_request" else "yellow")
            tbl.add_row(
                str(i),
                f"[{color}]{a['type'].upper()}[/{color}]",
                a["company"], a["role"],
                a["target_email"] or "[red]MISSING[/red]",
                a["subject"],
            )

        console.print(tbl)

        if not Confirm.ask("\n[bold yellow]Send all these emails?[/bold yellow]"):
            console.print("[dim]Aborted. Drafts remain in Gmail.[/dim]")
            return

        today = datetime.now().strftime("%Y-%m-%d")
        sent  = 0

        for a in actions:
            if not a.get("draft_id"):
                console.print(f"  [red]✗ {a['company']} — no draft ID, skipping[/red]")
                continue

            validated_target_email, validation_problem = self._revalidate_pending_action_target_email(a)
            if not validated_target_email:
                console.print(
                    f"  [red]✗ {a['company']} — {validation_problem or 'no valid target email'}, "
                    "skipping (rerun --digest to recreate safely)[/red]"
                )
                continue

            queued_target_email = extract_email_address(str(a.get("target_email", "") or ""))
            if validated_target_email != queued_target_email:
                console.print(
                    f"  [yellow]↻ {a['company']} recipient changed from "
                    f"{queued_target_email or 'missing'} to {validated_target_email}; "
                    "skipping this stale draft. Rerun --digest to recreate it safely.[/yellow]"
                )
                continue
            try:
                self.gmail.send_draft(a["draft_id"])
                if a["type"] == "follow_up":
                    self.sheets.set_field(a["appl_id"], "follow_up_sent_date", today)
                    # Fetch current count and increment
                    curr = self.sheets.get_all()
                    rec = next(
                        (x for x in curr if x.get("appl_id") == a["appl_id"]),
                        None,
                    )
                    self.sheets.set_field(a["appl_id"], "follow_up_count",
                                          str(int((rec or {}).get("follow_up_count") or 0) + 1))
                elif a["type"] == "withdraw":
                    self.sheets.set_field(a["appl_id"], "withdrawal_sent_date", today)
                    self.sheets.set_field(a["appl_id"], "deletion_request_sent_date", today)
                    self.sheets.set_field(a["appl_id"], "withdraw_in_next_digest", "")
                    self.sheets.set_field(a["appl_id"], "status", "Withdrawn")
                elif a["type"] == "deletion_request":
                    self.sheets.set_field(a["appl_id"], "deletion_request_sent_date", today)
                self.sheets.set_field(a["appl_id"], "draft_id", "")
                console.print(f"  [green]✓ Sent — {a['company']} ({a['type']})[/green]")
                sent += 1
            except Exception as e:
                console.print(f"  [red]✗ Failed — {a['company']}: {e}[/red]")

        try:
            PENDING_PATH.write_text("[]", encoding="utf-8")
        except OSError as error:
            raise TrackerError(f"Failed to clear pending actions file at {PENDING_PATH}: {error}") from error
        console.print(f"\n[bold green]✓ {sent}/{len(actions)} emails sent.[/bold green]")

    # ── manage ────────────────────────────────────────────────────────────────
    def manage(self):
        console.rule("[bold blue]Manage Applications")
        apps = self.sheets.get_all()
        if not apps:
            console.print("[yellow]No applications found.[/yellow]")
            return

        # Search
        query = Prompt.ask("  Search (company or role, leave blank for all)").strip().lower()
        matches = [
            a for a in apps
            if not query
            or query in a.get("company", "").lower()
            or query in a.get("role", "").lower()
        ] if query else apps

        # Filter out already-terminal unless user wants them
        active = [a for a in matches if a.get("status") not in ("Withdrawn",)]
        if not active:
            console.print("[yellow]No matching applications.[/yellow]")
            return

        # Display picker table
        tbl = Table(show_lines=True, expand=True)
        tbl.add_column("#",        width=3,  style="dim")
        tbl.add_column("Company",  width=18, style="cyan")
        tbl.add_column("Role",     width=24)
        tbl.add_column("Status",   width=12)
        tbl.add_column("Contact",  width=28, style="green")
        tbl.add_column("Deferred", width=12, style="dim")

        for i, a in enumerate(active, 1):
            tbl.add_row(
                str(i),
                a.get("company", ""),
                a.get("role", ""),
                a.get("status", ""),
                resolve_outbound_target_email(a, "follow_up") or "[dim]—[/dim]",
                a.get("deferred_until", "") or "—",
            )
        console.print(tbl)

        raw = Prompt.ask("  Enter # to manage (or blank to exit)").strip()
        if not raw:
            return
        try:
            idx = int(raw) - 1
            app = active[idx]
        except (ValueError, IndexError):
            console.print("[red]Invalid selection.[/red]")
            return

        console.print(
            f"\n  Selected: [cyan]{app.get('company')}[/cyan] — {app.get('role')} "
            f"([dim]{app.get('status')}[/dim])\n"
        )

        # Action menu
        console.print(
            "  Actions: [cyan]defer (d)[/cyan], [cyan]pause (p)[/cyan], "
            "[cyan]resume (r)[/cyan], [cyan]email (e)[/cyan], "
            "[cyan]action opt-outs (o)[/cyan], "
            "[cyan]withdraw (w)[/cyan], [cyan]exit (c)[/cyan]"
        )
        action = self._normalize_manage_action(Prompt.ask("  Action", default="c"))
        if action is None:
            console.print(
                "[red]Invalid action. Use defer/pause/resume/email/policy/withdraw/exit "
                "or d/p/r/e/o/w/c.[/red]"
            )
            return

        if action == "defer":
            console.print("  Examples: [dim]7d  |  2w  |  2026-05-01[/dim]")
            raw_when = Prompt.ask("  Defer until").strip()
            defer_date = self._parse_defer(raw_when)
            if not defer_date:
                console.print("[red]  Could not parse date. Use 7d, 2w, or YYYY-MM-DD.[/red]")
                return
            self.sheets.set_field(app["appl_id"], "deferred_until", defer_date)
            console.print(f"  [green]✓ Deferred until {defer_date}[/green]")

        elif action == "pause":
            self.sheets.set_field(app["appl_id"], "status", "Paused")
            console.print("  [green]✓ Paused — removed from pipeline until manually resumed[/green]")

        elif action == "resume":
            self.sheets.set_field(app["appl_id"], "status", "Applied")
            self.sheets.set_field(app["appl_id"], "deferred_until", "")
            console.print("  [green]✓ Resumed — back in pipeline with status 'Applied'[/green]")

        elif action == "email":
            current = app.get("contact_email") or ""
            if current:
                console.print(f"  Current contact email: [cyan]{current}[/cyan]")
            new_email = Prompt.ask("  New contact email").strip()
            if "@" not in new_email:
                console.print("[red]  Doesn't look like a valid email.[/red]")
                return
            self.sheets.set_field(app["appl_id"], "contact_email", new_email)
            console.print(f"  [green]✓ Contact email set to {new_email}[/green]")

        elif action == "policy":
            self._manage_action_opt_outs(app)

        elif action == "withdraw":
            self.sheets.set_field(app["appl_id"], "withdraw_in_next_digest", "TRUE")
            self.sheets.set_field(app["appl_id"], "deferred_until", "")
            console.print("  [green]✓ Withdrawal queued for the next digest[/green]")

        else:
            console.print("  [dim]Cancelled.[/dim]")

    @classmethod
    def _normalize_manage_action(cls, raw: str) -> Optional[str]:
        return cls.MANAGE_ACTION_ALIASES.get(str(raw).strip().lower())

    @staticmethod
    def _parse_defer(raw: str) -> Optional[str]:
        """Parse '7d', '2w', or 'YYYY-MM-DD' → 'YYYY-MM-DD' string."""
        raw = raw.strip().lower()
        today = datetime.now().date()
        if re.match(r"^\d{4}-\d{2}-\d{2}$", raw):
            return raw
        m = re.match(r"^(\d+)([dw])$", raw)
        if m:
            n, unit = int(m.group(1)), m.group(2)
            delta = timedelta(days=n if unit == "d" else n * 7)
            return (today + delta).isoformat()
        return None

    # ── add-linkedin ──────────────────────────────────────────────────────────
    def add_linkedin(self):
        console.rule("[bold blue]Add LinkedIn Application")
        company  = Prompt.ask("  Company name")
        role     = Prompt.ask("  Role / job title")
        contact  = Prompt.ask("  LinkedIn contact (who you sent CV to)", default="")
        date_str = Prompt.ask("  Date applied (YYYY-MM-DD)",
                               default=datetime.now().strftime("%Y-%m-%d"))
        notes    = Prompt.ask("  Notes (optional)", default="")

        app = {
            "appl_id":               str(uuid.uuid4())[:8],
            "company":              company,
            "role":                 role,
            "status":               "Applied",
            "source":               "linkedin",
            "applied_date":         date_str,
            "last_activity_date":   date_str,
            "recruiter_name":       "",
            "recruiter_email":      "",
            "ats_email":            "",
            "contact_email":        "",
            "follow_up_sent_date":  "",
            "follow_up_count":      "0",
            "withdrawal_sent_date": "",
            "follow_up_opt_out":    "",
            "withdraw_in_next_digest": "",
            "notes":                notes,
            "linkedin_contact":     contact,
            "email_ids":            "[]",
            "thread_ids":           "[]",
            "internet_message_ids": "[]",
            "gmail_review_url":     "",
            "draft_id":             "",
        }
        self.sheets.upsert(app)
        console.print(f"  [bold green]✓ Added: {company} — {role}[/bold green]")


# ── Entry point ────────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(description="Job Application Tracker")
    p.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--sync",          action="store_true", help="Fetch emails and update Sheets")
    g.add_argument("--digest",        action="store_true", help="Sync + compute actions + create drafts")
    g.add_argument("--confirm",       action="store_true", help="Review and send pending drafts")
    g.add_argument("--manage",        action="store_true", help="Defer, pause, resume, or fix email for an application")
    g.add_argument("--add-linkedin",  action="store_true", help="Manually add a LinkedIn application")
    g.add_argument("--delete-app",    metavar="APPL_ID", nargs="+", help="Delete one or more application rows without syncing")
    g.add_argument("--debug-app-emails", metavar="APPL_ID", help="Inspect AI grouping for one existing application row")
    g.add_argument("--debug-message-ids", metavar="MESSAGE_ID", nargs="+", help="Inspect AI grouping for specific Gmail message IDs")
    g.add_argument("--resync-app",    metavar="APPL_ID", help="Delete one email-backed application row, clear its processed state, and sync it again")
    g.add_argument("--resync-all",    action="store_true", help="Delete all email-backed application rows, clear their processed state, and sync them again")
    g.add_argument("--full-reset",    action="store_true", help="Destructively clear tracker state and rebuild from Gmail")
    g.add_argument("--debug-gmail-labels", action="store_true", help="Print Gmail user labels returned by the Gmail API")
    g.add_argument("--resume-run",    metavar="RUN_ID", help="Resume a saved AI grouping run from state/grouping_runs")
    p.add_argument("--yes", action="store_true", help="Skip confirmation prompts for destructive commands")
    args = p.parse_args()

    try:
        t = Tracker()
        if args.sync:
            t.sync()
        elif args.digest:
            t.digest()
        elif args.confirm:
            t.confirm()
        elif args.manage:
            t.manage()
        elif args.add_linkedin:
            t.add_linkedin()
        elif args.delete_app:
            t.delete_applications(appl_ids=args.delete_app, skip_confirmation=args.yes)
        elif args.debug_app_emails:
            t.debug_app_emails(appl_id=args.debug_app_emails)
        elif args.debug_message_ids:
            t.debug_message_ids(message_ids=args.debug_message_ids)
        elif args.resync_app:
            t.resync_application(appl_id=args.resync_app, skip_confirmation=args.yes)
        elif args.resync_all:
            t.resync_all(skip_confirmation=args.yes)
        elif args.full_reset:
            t.full_reset(skip_confirmation=args.yes)
        elif args.debug_gmail_labels:
            t.debug_gmail_labels()
        elif args.resume_run:
            t.resume_grouping_run(args.resume_run)
    except TrackerError as error:
        fail_with_message(str(error))
    except KeyboardInterrupt:
        fail_with_message("Operation cancelled by user.")
    except Exception as error:
        fail_with_message(f"Unexpected error: {error}")

if __name__ == "__main__":
    main()
