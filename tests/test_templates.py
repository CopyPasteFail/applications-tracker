import tempfile
import unittest
from pathlib import Path

from tracker import render_template


class RenderTemplateTests(unittest.TestCase):
    def test_supports_if_else_for_follow_up_greeting(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            template_path = Path(temp_dir) / "follow_up.txt"
            template_path.write_text(
                "Subject: Following Up - {{role}} Application\n\n"
                "{{#if recruiter_name}}Dear {{recruiter_name}},{{else}}Dear Hiring Team,{{/if}}\n",
                encoding="utf-8",
            )

            result = render_template(
                template_path,
                {
                    "role": "DevOps Engineer",
                    "recruiter_name": "",
                },
            )

            self.assertIsNotNone(result)
            subject, body = result or ("", "")
            self.assertEqual(subject, "Following Up - DevOps Engineer Application")
            self.assertEqual(body, "Dear Hiring Team,")

    def test_uses_truthy_if_branch_when_recruiter_name_present(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            template_path = Path(temp_dir) / "follow_up.txt"
            template_path.write_text(
                "Subject: Following Up - {{role}} Application\n\n"
                "{{#if recruiter_name}}Dear {{recruiter_name}},{{else}}Dear Hiring Team,{{/if}}\n",
                encoding="utf-8",
            )

            result = render_template(
                template_path,
                {
                    "role": "DevOps Engineer",
                    "recruiter_name": "Softech Recruiting Team",
                },
            )

            self.assertIsNotNone(result)
            subject, body = result or ("", "")
            self.assertEqual(subject, "Following Up - DevOps Engineer Application")
            self.assertEqual(body, "Dear Softech Recruiting Team,")


if __name__ == "__main__":
    unittest.main()
