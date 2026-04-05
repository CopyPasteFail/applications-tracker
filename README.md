# Job Application Tracker

Automatically syncs your Gmail inbox into a Google Sheet, groups fragmented ATS + recruiter emails into single application records using Gemini, and drafts follow-up and data erasure emails on a schedule.

## Features

- Gmail → Google Sheets sync via AI grouping (handles cross-thread ATS + recruiter emails)
- Configurable follow-up and withdrawal thresholds
- Daily digest with one-keystroke confirm-and-send
- Defer, pause, resume, or fix missing emails from the terminal or directly in Sheets
- LinkedIn applications via manual CLI entry
- Sends from your career email address via Gmail "Send mail as"
- Editable plain-text email templates — no code changes needed
- Cross-platform: Windows (Task Scheduler), macOS (launchd), Linux/WSL (cron)

---

## Project Structure

```
applications-tracker/
├── tracker.py                  # Main script
├── scheduler.py                # One-time scheduler setup
├── config.yaml                 # Your config (gitignored)
├── config.example.yaml         # Safe reference copy
├── templates/
│   ├── follow_up.txt
│   └── withdrawal.txt
├── credentials/
│   ├── google_credentials.json # Downloaded from Google Cloud (gitignored)
│   └── token.json              # Auto-created on first auth (gitignored)
├── logs/                       # Auto-created (gitignored)
├── pending_actions.json        # Draft send queue (gitignored)
└── state/
    └── processed_gmail_message_ids.json # Local Gmail dedupe backstop (gitignored)
```

---

## Setup

### 1. Clone and create a virtual environment

Copy `config.example.yaml` to `config.yaml`

```bash
cp config.example.yaml config.yaml
```

#### Windows (PowerShell)

```powershell
# Run once if script execution is blocked:
# Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser

python -m venv .venv
.venv\Scripts\Activate.ps1
```

#### macOS / Linux / WSL

```bash
python3 -m venv .venv
source .venv/bin/activate
```

Always activate the `.venv` before running any commands.

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

For development and tests:

```bash
pip install -r dev-requirements.txt
```

### 3. Create a Google Cloud project

1. Go to [console.cloud.google.com](https://console.cloud.google.com) → **New Project** → name it `applications-tracker`
2. **APIs & Services → Library** — enable:
   - Gmail API
   - Google Sheets API
   - Google Drive API
3. **APIs & Services → Credentials → Create Credentials → OAuth client ID**
   - Configure the OAuth consent screen first if prompted:
     - Add your personal Gmail as a test user
     - User type: **External**
     - Publishing status can stay **Testing** for personal use, but only listed test users can sign in
   - Application type: **Desktop app**
4. **APIs & Services → Credentials → Create Credentials → Data Access**
   - Scopes: `gmail.modify`, `gmail.labels`, `spreadsheets`, `drive.file`
5. Download the JSON → save as `credentials/google_credentials.json`
   - Make sure this file comes from the same Google Cloud project where you configured the consent screen and test users
6. In **Google Cloud Console → APIs & Services → OAuth consent screen**
   - click on **Audience**
   - Set as a **Test user** the email you intend to use in the `user.personal_email` field in `config.yaml`.

### 4. Create a Google Sheet

1. Go to [sheets.google.com](https://sheets.google.com) → create a new blank spreadsheet
2. Copy the spreadsheet ID from the URL:
   ```
   https://docs.google.com/spreadsheets/d/THIS_IS_THE_ID/edit
   ```
3. Paste it into `config.yaml` under `google_sheets.spreadsheet_id`

The `Applications` tab and all column headers are created automatically on first run.

### 5. Get a Gemini API key

Go to [aistudio.google.com/apikey](https://aistudio.google.com/apikey) — free, no credit card required. Paste the key into `config.yaml` under `gemini.api_key`.

> **Note:** On the free tier, prompts may be used to improve Google's models. This means email snippets (company names, recruiter emails, etc.) are included. If that's a concern, the paid tier disables this and costs fractions of a cent per month for this workload.

### 6. Configure config.yaml

Key settings to fill in:
- `user.name` — your full name, used in email sign-offs
- `user.personal_email` — the Gmail account the script authenticates as
- `user.career_email` — the address emails are sent *from* (must be set up in Gmail → Settings → Accounts and Import → **Send mail as**)
- `google_sheets.spreadsheet_id`
- `gemini.api_key`
- `gmail.query` — see Gmail label tips below
- `gmail.processing_labels` — optional Gmail labels used to mark pipeline stages after a successful sync

#### Gmail label query syntax

| Your label | Query to use |
|---|---|
| `JobSearch` | `label:JobSearch` |
| `Job Search` (with space) | `label:Job-Search` |
| `Job Search/Applications` (sub-label) | `label:Job-Search-Applications` |

Gmail converts spaces → hyphens and slashes → hyphens in query syntax. To confirm the exact string for your label, search by label in Gmail and copy the query it generates in the search bar.

You can also combine with filters to reduce noise:
```yaml
query: "label:Job-Search -unsubscribe -newsletter"
```

The label doesn't need to contain only job application emails — the AI grouping step filters out irrelevant messages before they reach your Sheet.

#### Processing labels

The tracker uses Gmail labels to mark message-level pipeline progress after a successful sync. By default it creates:

- `application-tracker`
- `application-tracker/processed`

You can override them in `config.yaml`:

```yaml
gmail:
  processing_labels:
    root: "application-tracker"
    stages:
      processed: "processed"
```

Short stage names such as `processed` are expanded under the root label, so the example above becomes `application-tracker/processed`.

Sync behavior:
- The tracker scans Gmail messages that match `gmail.query` within `lookback_days`.
- Messages already labeled `application-tracker/processed` are skipped at the Gmail query level.
- A local state file at `state/processed_gmail_message_ids.json` acts as a correctness backstop if label application fails after a successful Sheet write.
- Existing `email_ids` stored in the Sheet are still used as a migration-safe fallback dedupe source.

### 7. First run

```bash
python tracker.py --sync
```

A browser window opens for Google OAuth — approve it. On WSL, if the browser doesn't open, copy the URL from the terminal and open it in your Windows browser.

After auth, `credentials/token.json` is saved and you won't need to re-authenticate unless you revoke access.

If you already authenticated before upgrading to the Gmail label flow, delete `credentials/token.json` and run the tracker again so Google can grant the additional `gmail.labels` scope.

The first sync scans up to `lookback_days` of emails. Expect 2–5 minutes depending on inbox size.

### Full reset

If you want a destructive rebuild from Gmail using the current `lookback_days` window:

```bash
python tracker.py --full-reset
python tracker.py --full-reset --yes
```

`--full-reset` deletes tracker-managed state and then runs a fresh sync. It:

- Deletes tracker-managed Gmail processing labels such as `application-tracker/processed`
- Clears the configured `Applications` worksheet back to headers only
- Deletes pending Gmail drafts referenced by `pending_actions.json`
- Deletes `pending_actions.json`
- Deletes `state/processed_gmail_message_ids.json`

This is intentionally destructive. If you want to preserve the current sheet contents for comparison, duplicate the sheet first instead of using `--full-reset`.

### Resync one application

If you want to force one email-backed application to be rebuilt from its original Gmail messages:

```bash
python tracker.py --resync-app APPL_ID
python tracker.py --resync-app APPL_ID --yes
```

`--resync-app`:

- Deletes the current sheet row for that `appl_id`
- Removes the tracker processed label from the Gmail messages linked to that row plus closely related Gmail messages found by company/contact matching
- Removes those Gmail message IDs from `state/processed_gmail_message_ids.json`
- Runs a fresh sync so the application is re-derived from Gmail

This only works for applications that still have stored Gmail `email_ids`.

### Resync all email-backed applications

If you want to rebuild every Gmail-derived application without doing a full reset:

```bash
python tracker.py --resync-all
python tracker.py --resync-all --yes
```

`--resync-all`:

- Deletes all current sheet rows whose `source` is `email`
- Removes the tracker processed label from their stored Gmail messages when available
- Removes those Gmail message IDs from `state/processed_gmail_message_ids.json`
- Leaves non-email rows such as manually added LinkedIn applications in place
- Runs a fresh sync so email-backed applications are re-derived from Gmail

Unlike `--full-reset`, this does not delete pending drafts, pending actions, or the entire sheet.

### Delete applications without syncing

If you want to fully reset one or more application IDs without running sync:

```bash
python tracker.py --delete-app APPL_ID
python tracker.py --delete-app APPL_ID1 APPL_ID2 --yes
```

`--delete-app`:

- Deletes the matching rows from the `Applications` sheet
- Removes the tracker processed label from the Gmail messages linked to those rows
- Removes those Gmail message IDs from `state/processed_gmail_message_ids.json`
- Does not run sync automatically

### 8. Schedule background sync

Run once with your `.venv` activated — auto-detects your OS:

```bash
python scheduler.py                       # digest at 10:00 (default)
python scheduler.py --digest-time 08:30   # custom digest time
python scheduler.py --remove              # remove all scheduled jobs
```

This registers two jobs: `--sync` every 2 hours (silent), and `--digest` once daily at your chosen time.

---

## Daily workflow

```bash
# Morning — runs automatically at your scheduled time, or manually:
python tracker.py --digest    # syncs + shows due actions + creates Gmail drafts

# Review the table, then:
python tracker.py --confirm   # press Y to send all drafts

# Force one application to be rebuilt from Gmail:
python tracker.py --resync-app APPL_ID

# Delete one or more application rows without syncing:
python tracker.py --delete-app APPL_ID1 APPL_ID2

# Destructive rebuild from Gmail:
python tracker.py --full-reset
```

### Manage individual applications

```bash
python tracker.py --manage
```

Search by company or role, then choose an action:

| Action | What it does |
|---|---|
| `defer` | Skip until a future date. Accepts `7d`, `2w`, or `YYYY-MM-DD` |
| `pause` | Remove from pipeline permanently until manually resumed |
| `resume` | Clear deferral and set status back to Applied |
| `email` | Set or fix the contact email for follow-ups and withdrawals |

You can also do all of these directly in the Google Sheet, or via the **Job Tracker** menu (Apps Script — see below).

### Add a LinkedIn application

```bash
python tracker.py --add-linkedin
```

Prompts for company, role, date applied, and contact name. Treated identically to email-sourced applications by the follow-up engine.

Digest behavior:
- Active stale applications can generate a withdrawal + deletion request.
- Rejected applications can separately generate a deletion request without changing status to `Withdrawn`.

---

## Email templates

Templates live in `templates/` as plain `.txt` files. Edit them freely — no code changes needed.

Available variables: `{{user_name}}`, `{{company}}`, `{{role}}`, `{{recruiter_name}}`

Conditional block (renders only if the variable is non-empty):
```
{{#if recruiter_name}}Hi {{recruiter_name}},{{/if}}
```

If a template file is missing, Gemini generates the email body as a fallback.

---

## Google Sheets columns

| Column | Description |
|---|---|
| `appl_id` | Short unique ID |
| `company` | Company name |
| `role` | Job title |
| `status` | Applied / Screening / Interview / Assessment / Offer / Rejected / Withdrawn / Paused |
| `source` | `email` or `linkedin` |
| `applied_date` | First contact date |
| `last_activity_date` | Most recent email date |
| `recruiter_name` | Human sender name |
| `recruiter_email` | Human sender email |
| `ats_email` | ATS system email |
| `contact_email` | Address used for follow-ups, withdrawal requests, and deletion requests |
| `follow_up_sent_date` | Date last follow-up was sent |
| `follow_up_count` | Number of follow-ups sent |
| `withdrawal_sent_date` | Date withdrawal was sent |
| `deletion_request_sent_date` | Date a privacy / GDPR deletion request was sent |
| `deletion_request_opt_out` | Set to `yes` / `true` / `1` / `skip` to suppress deletion-request drafting for this row |
| `deferred_until` | Skip pipeline until this date (YYYY-MM-DD) |
| `notes` | Free text - edit manually |
| `linkedin_contact` | LinkedIn contact name |
| `email_ids` | JSON list of Gmail message IDs (internal) |
| `thread_ids` | JSON list of Gmail thread IDs (internal) |
| `internet_message_ids` | JSON list of RFC `Message-ID` header values used for Gmail search |
| `gmail_review_url` | Clickable Gmail link for reviewing the emails tied to this application |
| `draft_id` | Current pending Gmail draft ID (internal) |

---

## Apps Script (Google Sheets menu)

For manage operations directly in the Sheet without opening a terminal:

1. Open your Sheet → **Extensions → Apps Script**
2. Paste the contents of `JobTracker.gs`, save, and run `onOpen` once to grant permissions
3. Reload the Sheet — a **Job Tracker** menu appears in the toolbar

Select any row, then use the menu to defer, pause, resume, set a contact email, change status, or add a LinkedIn application.

---

## Troubleshooting

**`PERMISSION_DENIED` from gspread**
→ Make sure the Sheet is owned by or shared with the Google account you authenticated with.

**`Google Sheets API has not been used in project ... before or it is disabled`**
→ Enable **Google Sheets API** in the same Google Cloud project that produced `credentials/google_credentials.json`, wait a few minutes, then rerun the script.

**Browser doesn't open in WSL**
→ Run `export BROWSER=wslview` before the script, or paste the OAuth URL into your Windows browser.

**Wrong emails being matched**
→ Tighten `gmail.query` in `config.yaml`, or edit the company/role directly in the Sheet.

**Emails sent from personal address instead of career address**
→ Verify `user.career_email` in `config.yaml` exactly matches the address listed under Gmail → Settings → Accounts and Import → Send mail as.

**Gemini rate limit errors**
→ Reduce `gemini.grouping_batch_size` in `config.yaml`, lower `gemini.rate_limit_max_wait_seconds` if you want sync to stop sooner, or upgrade to a higher Gemini quota tier/model. The tracker now stops AI grouping early when Gemini reports quota exhaustion instead of continuing to spam failed batches.
