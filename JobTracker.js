// Job Tracker — Google Apps Script
// Paste this entire file into your Sheet's Apps Script editor:
//   Extensions → Apps Script → replace any existing code → Save → Run onOpen once

const SHEET_NAME = "Applications";

const COL = {
  app_id:               1,
  company:              2,
  role:                 3,
  status:               4,
  source:               5,
  applied_date:         6,
  last_activity_date:   7,
  recruiter_name:       8,
  recruiter_email:      9,
  ats_email:            10,
  contact_email:        11,
  follow_up_sent_date:  12,
  follow_up_count:      13,
  withdrawal_sent_date: 14,
  deferred_until:       15,
  notes:                16,
  linkedin_contact:     17,
  email_ids:            18,
  draft_id:             19,
};

// ── Menu ──────────────────────────────────────────────────────────────────────

function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu("Job Tracker")
    .addItem("Defer…",             "menuDefer")
    .addItem("Pause",              "menuPause")
    .addItem("Resume",             "menuResume")
    .addSeparator()
    .addItem("Set contact email…", "menuSetEmail")
    .addItem("Set status…",        "menuSetStatus")
    .addSeparator()
    .addItem("Add LinkedIn app…",  "menuAddLinkedin")
    .toUi();
}

// ── Helpers ───────────────────────────────────────────────────────────────────

function getSheet() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  return ss.getSheetByName(SHEET_NAME);
}

function getSelectedRow() {
  const sheet = getSheet();
  const row   = sheet.getActiveRange().getRow();
  if (row <= 1) {
    SpreadsheetApp.getUi().alert("Select a data row first (not the header).");
    return null;
  }
  return row;
}

function getCell(row, colName) {
  return getSheet().getRange(row, COL[colName]);
}

function getCellValue(row, colName) {
  return getSheet().getRange(row, COL[colName]).getValue();
}

function setCellValue(row, colName, value) {
  getSheet().getRange(row, COL[colName]).setValue(value);
}

function rowSummary(row) {
  const company = getCellValue(row, "company");
  const role    = getCellValue(row, "role");
  const status  = getCellValue(row, "status");
  return `${company} — ${role} (${status})`;
}

// Parse "7d", "2w", or "YYYY-MM-DD" → Date object. Returns null if invalid.
function parseDefer(raw) {
  raw = raw.trim();
  const absMatch = raw.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (absMatch) {
    const d = new Date(raw);
    return isNaN(d) ? null : d;
  }
  const relMatch = raw.match(/^(\d+)([dw])$/i);
  if (relMatch) {
    const n    = parseInt(relMatch[1]);
    const unit = relMatch[2].toLowerCase();
    const d    = new Date();
    d.setDate(d.getDate() + (unit === "w" ? n * 7 : n));
    return d;
  }
  return null;
}

function formatDate(date) {
  return Utilities.formatDate(date, Session.getScriptTimeZone(), "yyyy-MM-dd");
}

// ── Menu actions ──────────────────────────────────────────────────────────────

function menuDefer() {
  const row = getSelectedRow();
  if (!row) return;
  const ui  = SpreadsheetApp.getUi();

  const res = ui.prompt(
    `Defer — ${rowSummary(row)}`,
    "Enter duration (7d, 2w) or a date (YYYY-MM-DD):",
    ui.ButtonSet.OK_CANCEL
  );
  if (res.getSelectedButton() !== ui.Button.OK) return;

  const date = parseDefer(res.getResponseText());
  if (!date) {
    ui.alert("Could not parse that. Use 7d, 2w, or YYYY-MM-DD.");
    return;
  }

  setCellValue(row, "deferred_until", formatDate(date));
  ui.alert(`Deferred until ${formatDate(date)}.`);
}

function menuPause() {
  const row = getSelectedRow();
  if (!row) return;
  const ui  = SpreadsheetApp.getUi();

  const res = ui.alert(
    `Pause — ${rowSummary(row)}`,
    "Remove from pipeline until manually resumed?",
    ui.ButtonSet.YES_NO
  );
  if (res !== ui.Button.YES) return;

  setCellValue(row, "status", "Paused");
  ui.alert("Paused. Use Job Tracker → Resume to bring it back.");
}

function menuResume() {
  const row = getSelectedRow();
  if (!row) return;
  const ui  = SpreadsheetApp.getUi();

  const res = ui.alert(
    `Resume — ${rowSummary(row)}`,
    "Clear deferral and set status back to Applied?",
    ui.ButtonSet.YES_NO
  );
  if (res !== ui.Button.YES) return;

  setCellValue(row, "status",        "Applied");
  setCellValue(row, "deferred_until", "");
  ui.alert("Resumed. Back in pipeline with status Applied.");
}

function menuSetEmail() {
  const row = getSelectedRow();
  if (!row) return;
  const ui      = SpreadsheetApp.getUi();
  const current = getCellValue(row, "contact_email") || "";

  const res = ui.prompt(
    `Set contact email — ${rowSummary(row)}`,
    current ? `Current: ${current}\n\nNew email:` : "Enter contact email:",
    ui.ButtonSet.OK_CANCEL
  );
  if (res.getSelectedButton() !== ui.Button.OK) return;

  const email = res.getResponseText().trim();
  if (!email.includes("@")) {
    ui.alert("That doesn't look like a valid email address.");
    return;
  }

  setCellValue(row, "contact_email", email);
  ui.alert(`Contact email set to ${email}.`);
}

function menuSetStatus() {
  const row      = getSelectedRow();
  if (!row) return;
  const ui       = SpreadsheetApp.getUi();
  const statuses = ["Applied","Screening","Interview","Assessment","Offer","Rejected","Withdrawn","Paused"];
  const current  = getCellValue(row, "status");

  const res = ui.prompt(
    `Set status — ${rowSummary(row)}`,
    `Current: ${current}\n\nChoose: ${statuses.join(" | ")}`,
    ui.ButtonSet.OK_CANCEL
  );
  if (res.getSelectedButton() !== ui.Button.OK) return;

  const input  = res.getResponseText().trim();
  const match  = statuses.find(s => s.toLowerCase() === input.toLowerCase());
  if (!match) {
    ui.alert(`Invalid status. Choose one of:\n${statuses.join(", ")}`);
    return;
  }

  setCellValue(row, "status", match);
  ui.alert(`Status set to ${match}.`);
}

function menuAddLinkedin() {
  const ui = SpreadsheetApp.getUi();

  const company = ui.prompt("Add LinkedIn application", "Company name:", ui.ButtonSet.OK_CANCEL);
  if (company.getSelectedButton() !== ui.Button.OK) return;

  const role = ui.prompt("Add LinkedIn application", "Role / job title:", ui.ButtonSet.OK_CANCEL);
  if (role.getSelectedButton() !== ui.Button.OK) return;

  const contact = ui.prompt("Add LinkedIn application", "LinkedIn contact name (optional):", ui.ButtonSet.OK_CANCEL);
  if (contact.getSelectedButton() !== ui.Button.OK) return;

  const today    = formatDate(new Date());
  const sheet    = getSheet();
  const newAppId = Utilities.getUuid().substring(0, 8);

  const row = new Array(Object.keys(COL).length).fill("");
  row[COL.app_id             - 1] = newAppId;
  row[COL.company            - 1] = company.getResponseText().trim();
  row[COL.role               - 1] = role.getResponseText().trim();
  row[COL.status             - 1] = "Applied";
  row[COL.source             - 1] = "linkedin";
  row[COL.applied_date       - 1] = today;
  row[COL.last_activity_date - 1] = today;
  row[COL.follow_up_count    - 1] = 0;
  row[COL.linkedin_contact   - 1] = contact.getResponseText().trim();
  row[COL.email_ids          - 1] = "[]";

  sheet.appendRow(row);
  ui.alert(`Added: ${company.getResponseText().trim()} — ${role.getResponseText().trim()}`);
}
