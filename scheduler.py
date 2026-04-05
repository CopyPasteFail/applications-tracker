#!/usr/bin/env python3
"""
Cross-platform scheduler setup for applications-tracker.

Registers two scheduled jobs:
  1. --sync   every 2 hours  (silent background)
  2. --digest once per day   at --digest-time (default 10:00)

Re-running this script is safe — it replaces existing entries, never duplicates.

Usage:
  python scheduler.py                        # digest at 10:00 (default)
  python scheduler.py --digest-time 08:30   # digest at 08:30
  python scheduler.py --remove              # remove all applications-tracker schedules

Windows  → Task Scheduler via schtasks
Linux    → crontab
macOS    → launchd plists in ~/Library/LaunchAgents
"""

import sys
import subprocess
import argparse
import ctypes
from pathlib import Path
from typing import Optional

BASE_DIR   = Path(__file__).parent.resolve()
PYTHON_EXE = Path(sys.executable).resolve()
SCRIPT     = BASE_DIR / "tracker.py"
LOG_DIR    = BASE_DIR / "logs"

# Stable identifiers used to find & replace our entries across runs
TASK_SYNC         = "JobTrackerSync"
TASK_DIGEST       = "JobTrackerDigest"
CRON_MARKER_SYNC  = "# applications-tracker:sync"
CRON_MARKER_DIGEST= "# applications-tracker:digest"
PLIST_SYNC        = "com.jobtracker.sync"
PLIST_DIGEST      = "com.jobtracker.digest"


def parse_args():
    p = argparse.ArgumentParser(description="Schedule applications-tracker jobs")
    p.add_argument(
        "--digest-time", default="10:00", metavar="HH:MM",
        help="Time to run daily digest (24h format, default: 10:00)"
    )
    p.add_argument(
        "--remove", action="store_true",
        help="Remove all applications-tracker scheduled jobs"
    )
    return p.parse_args()


def parse_time(t: str) -> tuple[int, int]:
    """Parse 'HH:MM' → (hour, minute). Exits on invalid input."""
    try:
        h, m = t.split(":")
        hour, minute = int(h), int(m)
        assert 0 <= hour <= 23 and 0 <= minute <= 59
        return hour, minute
    except (ValueError, AssertionError):
        print(f"✗ Invalid time '{t}' — use HH:MM format (e.g. 09:30)")
        sys.exit(1)


# ── Platform dispatch ──────────────────────────────────────────────────────────

def main():
    args   = parse_args()
    hour, minute = parse_time(args.digest_time)
    LOG_DIR.mkdir(exist_ok=True)

    plat = sys.platform
    if plat == "win32":
        if args.remove:
            remove_windows()
        else:
            setup_windows(hour, minute)
    elif plat == "darwin":
        if args.remove:
            remove_macos()
        else:
            setup_macos(hour, minute)
    elif plat.startswith("linux"):
        if args.remove:
            remove_linux()
        else:
            setup_linux(hour, minute)
    else:
        print(f"Unsupported platform: {plat}")
        sys.exit(1)


# ── Windows ────────────────────────────────────────────────────────────────────

def _schtasks_create(task_name: str, tr: str, schedule_args: list[str]) -> bool:
    """Create or replace a Task Scheduler task. Returns True on success."""
    cmd = [
        "schtasks", "/Create",
        "/TN", task_name,
        "/TR", tr,
        "/F",           # overwrite existing — this is what prevents duplicates
        *schedule_args,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"  ✗ schtasks error: {result.stderr.strip()}")
    return result.returncode == 0


def _is_current_user_windows_admin() -> bool:
    """
    Return whether the current Windows user has administrative privileges.

    Inputs:
    - None.

    Outputs:
    - True when the current user is an administrator.

    Edge cases:
    - Returns False when the Windows-specific ctypes entry point is unavailable.

    Atomicity / concurrency:
    - Pure platform probe with no side effects.
    """
    windll = getattr(ctypes, "windll", None)
    if windll is None:
        return False
    return bool(windll.shell32.IsUserAnAdmin())


def setup_windows(hour: int, minute: int):
    if not _is_current_user_windows_admin():
        print("Note: registering tasks for current user only (no admin required).")

    sync_tr   = f'"{PYTHON_EXE}" "{SCRIPT}" --sync'
    digest_tr = f'"{PYTHON_EXE}" "{SCRIPT}" --digest'
    digest_st = f"{hour:02d}:{minute:02d}"

    print("Registering Task Scheduler jobs …\n")

    ok_sync = _schtasks_create(
        TASK_SYNC, sync_tr,
        ["/SC", "HOURLY", "/MO", "2", "/ST", "09:00"],
    )
    ok_digest = _schtasks_create(
        TASK_DIGEST, digest_tr,
        ["/SC", "DAILY", "/ST", digest_st],
    )

    if ok_sync:
        print(f"  ✓ {TASK_SYNC}   — every 2 hours")
    if ok_digest:
        print(f"  ✓ {TASK_DIGEST} — daily at {digest_st}")

    print("\n  To remove:   python scheduler.py --remove")
    print("  To change time: python scheduler.py --digest-time HH:MM")
    print("  View in UI:  taskschd.msc")


def remove_windows():
    for name in (TASK_SYNC, TASK_DIGEST):
        result = subprocess.run(
            ["schtasks", "/Delete", "/TN", name, "/F"],
            capture_output=True, text=True
        )
        if result.returncode == 0:
            print(f"  ✓ Removed: {name}")
        else:
            # Task may not exist — not an error worth surfacing
            print(f"  – Not found (already removed?): {name}")


# ── macOS ──────────────────────────────────────────────────────────────────────

def _write_plist(
    label: str,
    args_list: list[str],
    interval_sec: Optional[int] = None,
    hour: Optional[int] = None,
    minute: Optional[int] = None,
) -> Path:
    """
    Write a launchd plist using either StartInterval or StartCalendarInterval.

    Inputs:
    - label: launchd label name.
    - args_list: Program arguments written into the plist.
    - interval_sec: Interval for recurring jobs.
    - hour: Hour for calendar-based jobs.
    - minute: Minute for calendar-based jobs.

    Outputs:
    - Path to the written plist file.

    Edge cases:
    - Calendar-based jobs require both hour and minute.

    Atomicity / concurrency:
    - Writes one plist file on disk with no cross-process coordination.
    """
    agents_dir = Path.home() / "Library" / "LaunchAgents"
    agents_dir.mkdir(parents=True, exist_ok=True)
    path = agents_dir / f"{label}.plist"

    prog_args = "\n".join(f"        <string>{a}</string>" for a in args_list)

    if interval_sec:
        trigger = f"    <key>StartInterval</key>\n    <integer>{interval_sec}</integer>"
    else:
        if hour is None or minute is None:
            raise ValueError("hour and minute are required when interval_sec is not provided")
        trigger = (
            f"    <key>StartCalendarInterval</key>\n"
            f"    <dict>\n"
            f"        <key>Hour</key><integer>{hour}</integer>\n"
            f"        <key>Minute</key><integer>{minute}</integer>\n"
            f"    </dict>"
        )

    plist = f"""<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>{label}</string>
    <key>ProgramArguments</key>
    <array>
{prog_args}
    </array>
{trigger}
    <key>RunAtLoad</key>
    <false/>
    <key>StandardOutPath</key>
    <string>{LOG_DIR}/{label}.log</string>
    <key>StandardErrorPath</key>
    <string>{LOG_DIR}/{label}.err</string>
</dict>
</plist>
"""
    path.write_text(plist)
    return path


def _launchctl_load(plist_path: Path):
    subprocess.run(["launchctl", "unload", str(plist_path)], capture_output=True)
    result = subprocess.run(["launchctl", "load", str(plist_path)],
                            capture_output=True, text=True)
    return result.returncode == 0


def setup_macos(hour: int, minute: int):
    print("Installing launchd agents …\n")

    p_sync = _write_plist(
        PLIST_SYNC,
        [str(PYTHON_EXE), str(SCRIPT), "--sync"],
        interval_sec=7200,
    )
    p_digest = _write_plist(
        PLIST_DIGEST,
        [str(PYTHON_EXE), str(SCRIPT), "--digest"],
        hour=hour, minute=minute,
    )

    if _launchctl_load(p_sync):
        print(f"  ✓ {PLIST_SYNC}   — every 2 hours")
    if _launchctl_load(p_digest):
        print(f"  ✓ {PLIST_DIGEST} — daily at {hour:02d}:{minute:02d}")

    print(f"\n  Logs: {LOG_DIR}/")
    print("  To remove: python scheduler.py --remove")
    print("  To change time: python scheduler.py --digest-time HH:MM")


def remove_macos():
    agents_dir = Path.home() / "Library" / "LaunchAgents"
    for label in (PLIST_SYNC, PLIST_DIGEST):
        path = agents_dir / f"{label}.plist"
        if path.exists():
            subprocess.run(["launchctl", "unload", str(path)], capture_output=True)
            path.unlink()
            print(f"  ✓ Removed: {label}")
        else:
            print(f"  – Not found (already removed?): {label}")


# ── Linux / WSL ────────────────────────────────────────────────────────────────

def _read_crontab() -> str:
    r = subprocess.run(["crontab", "-l"], capture_output=True, text=True)
    return r.stdout if r.returncode == 0 else ""


def _write_crontab(content: str):
    subprocess.run(["crontab", "-"], input=content, text=True, check=True)


def _strip_job_lines(crontab: str) -> str:
    """Remove all lines belonging to applications-tracker (line + its marker comment)."""
    out: list[str] = []
    lines = crontab.splitlines()
    i = 0
    while i < len(lines):
        line = lines[i]
        if line.strip() in (CRON_MARKER_SYNC, CRON_MARKER_DIGEST):
            i += 2  # skip marker + the cron line after it
        else:
            out.append(line)
            i += 1
    return "\n".join(out).strip()


def setup_linux(hour: int, minute: int):
    log_sync   = LOG_DIR / "sync.log"
    log_digest = LOG_DIR / "digest.log"

    sync_line   = f"0 */2 * * * {PYTHON_EXE} {SCRIPT} --sync >> {log_sync} 2>&1"
    digest_line = f"{minute} {hour} * * * {PYTHON_EXE} {SCRIPT} --digest >> {log_digest} 2>&1"

    existing = _read_crontab()
    cleaned  = _strip_job_lines(existing)  # remove old entries regardless

    new_crontab = (
        cleaned.rstrip("\n") + "\n\n"
        + f"{CRON_MARKER_SYNC}\n{sync_line}\n"
        + f"{CRON_MARKER_DIGEST}\n{digest_line}\n"
    )

    _write_crontab(new_crontab)

    print(f"  ✓ Sync   — every 2 hours      ({log_sync})")
    print(f"  ✓ Digest — daily at {hour:02d}:{minute:02d}  ({log_digest})")
    print("\n  To remove: python scheduler.py --remove")
    print("  To change time: python scheduler.py --digest-time HH:MM")
    print("  To inspect: crontab -l")


def remove_linux():
    existing = _read_crontab()
    cleaned  = _strip_job_lines(existing)
    _write_crontab(cleaned + "\n")
    print("  ✓ Removed all applications-tracker crontab entries")


if __name__ == "__main__":
    main()
