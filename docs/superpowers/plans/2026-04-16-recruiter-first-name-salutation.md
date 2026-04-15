# Recruiter First-Name Salutation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make outbound emails greet recruiters as `Hi <FIRST_NAME>` only when the resolved send target is the application's `recruiter_email`.

**Architecture:** Keep template rendering unchanged and move the behavior into the salutation resolver in `tracker.py`. The generator methods will pass the resolved outbound target email into the resolver so it can decide whether to use recruiter first name or keep the existing team/company fallback.

**Tech Stack:** Python, unittest, pytest

---

### Task 1: Capture the greeting behavior in tests

**Files:**
- Modify: `tests/test_follow_up_templates.py`
- Test: `tests/test_follow_up_templates.py`

- [ ] **Step 1: Write the failing tests**

```python
    def test_recruiter_target_uses_first_name_greeting(self) -> None:
        grouper = AIGrouper.__new__(AIGrouper)
        app = {
            "recruiter_name": "Jordan Cohen",
            "recruiter_email": "jordan@example.com",
            "contact_email": "jordan@example.com",
        }

        salutation = AIGrouper._resolve_outreach_salutation_name(
            grouper,
            app,
            "Acme",
            "jordan@example.com",
        )

        self.assertEqual(salutation, "Jordan")

    def test_non_recruiter_target_keeps_existing_company_fallback(self) -> None:
        grouper = AIGrouper.__new__(AIGrouper)
        app = {
            "recruiter_name": "Jordan Cohen",
            "recruiter_email": "jordan@example.com",
            "contact_email": "talent@example.com",
        }

        salutation = AIGrouper._resolve_outreach_salutation_name(
            grouper,
            app,
            "Acme",
            "talent@example.com",
        )

        self.assertEqual(salutation, "Acme Hiring Team")
```

- [ ] **Step 2: Run the test file to verify it fails**

Run: `python -m pytest tests/test_follow_up_templates.py -q`
Expected: `FAIL` because `_resolve_outreach_salutation_name` does not yet accept the outbound target email and still returns the full recruiter name.

- [ ] **Step 3: Write the minimal implementation**

```python
resolved_target_email = resolve_outbound_target_email(app, "follow_up")
salutation_name = self._resolve_outreach_salutation_name(
    app,
    company_display_name,
    resolved_target_email,
)
```

```python
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
```

- [ ] **Step 4: Run the test file to verify it passes**

Run: `python -m pytest tests/test_follow_up_templates.py -q`
Expected: `PASS`

- [ ] **Step 5: Commit**

```bash
git add tests/test_follow_up_templates.py tracker.py
git commit -m "fix: use recruiter first name for recruiter-email greetings"
```

### Task 2: Verify no regression in template selection

**Files:**
- Modify: `tracker.py`
- Test: `tests/test_templates.py`
- Test: `tests/test_follow_up_templates.py`

- [ ] **Step 1: Run the related template tests**

Run: `python -m pytest tests/test_templates.py tests/test_follow_up_templates.py -q`
Expected: `PASS`

- [ ] **Step 2: Confirm no template file changes are required**

```text
The greeting behavior is driven by `salutation_name` in code.
Do not modify `templates/follow_up.txt` in this task because it already contains
the user-customized greeting text in the worktree.
```

- [ ] **Step 3: Commit if verification uncovers a needed follow-up fix**

```bash
git add tracker.py tests/test_templates.py tests/test_follow_up_templates.py
git commit -m "test: cover recruiter-email salutation behavior"
```
