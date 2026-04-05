import unittest

from tracker import (
    build_company_search_tokens,
    choose_best_contact_email_for_action,
    choose_preferred_role_title,
    detect_text_language,
    extract_scheduled_interview_date_from_email_message,
    extract_email_addresses_from_text,
    extract_first_json_object,
    extract_company_name_from_email_message,
    extract_role_titles_from_email_message,
    extract_status_from_email_message,
    get_base_domain,
    get_primary_domain_label,
    has_strong_application_signal,
    extract_linkedin_company_urls_from_text,
    extract_linkedin_job_urls_from_text,
    is_low_confidence_company_name,
    is_search_result_likely_related_to_company,
    is_unusable_outbound_email,
    is_unsolicited_recruiter_outreach_email,
    score_contact_email_for_action,
)


def make_email_message(
    *,
    from_header: str,
    subject: str,
    body: str,
    snippet: str = "",
    reply_to: str = "",
    attachment_text: str = "",
) -> dict[str, str]:
    return {
        "id": "msg-1",
        "thread_id": "thread-1",
        "from": from_header,
        "reply_to": reply_to,
        "subject": subject,
        "snippet": snippet or body[:160],
        "body": body,
        "attachment_text": attachment_text,
        "date": "2026-03-26",
        "internet_message_id": "<msg-1@example.test>",
    }


class OutreachDetectionTests(unittest.TestCase):
    def test_linkedin_recruiter_inmail_is_treated_as_outreach(self) -> None:
        message = make_email_message(
            from_header="Recruiter Name <inmail-hit-reply@linkedin.com>",
            subject="Job-Opportunity at Example Robotics: Platform Engineer for Software as a Service (SaaS)",
            body=(
                "Hi Candidate,\n\n"
                "I just wanted to follow up to check whether you had a chance to see my previous "
                "message regarding the Platform Engineer for Software as a Service (SaaS) position "
                "at Example Robotics.\n\n"
                "The role might be of interest to you. If you have any questions or would like "
                "more information, feel free to reach out anytime.\n\n"
                "You can find the job description and application link again here."
            ),
        )

        self.assertTrue(is_unsolicited_recruiter_outreach_email(message))
        self.assertFalse(has_strong_application_signal(message))

    def test_cold_recruiter_email_is_treated_as_outreach(self) -> None:
        message = make_email_message(
            from_header="Agency Recruiter <recruiter@staffing-example.com>",
            subject="Freelance embedded linux - Start ASAP - 12 months + - Remote / Berlin",
            body=(
                "Hi Candidate,\n\n"
                "A client of mine based in Berlin is looking for a FREELANCE Embedded "
                "engineer to join the team for a 12-month initial contract.\n\n"
                "If you wish to be considered for the position available, please e-mail an "
                "up-to-date CV with a contact number to recruiter@staffing-example.com.\n\n"
                "Please pass this advert onto anyone else who might be suitable for this role."
            ),
        )

        self.assertTrue(is_unsolicited_recruiter_outreach_email(message))
        self.assertFalse(has_strong_application_signal(message))

    def test_application_confirmation_is_not_treated_as_outreach(self) -> None:
        message = make_email_message(
            from_header="Example Recruiting <no-reply@greenhouse.io>",
            subject="Example Corp! We've received your application",
            body=(
                "Hi Candidate,\n\n"
                "We've received your application for the Senior Software Engineer position at "
                "Example Corp. Our team will review your application and reach out about next steps "
                "in the interview process."
            ),
        )

        self.assertTrue(has_strong_application_signal(message))
        self.assertFalse(is_unsolicited_recruiter_outreach_email(message))

    def test_interview_invite_exposes_scheduled_activity_date(self) -> None:
        message = make_email_message(
            from_header="Recruiter Name <recruiter@example.teamtailor-mail.com>",
            subject="Time confirmed: Example Corp | Interview invite",
            body=(
                "Hi Candidate,\n\n"
                "Thank you for choosing a time.\n"
                "Date/time Updated\n"
                "2026-03-20 13:30 - 14:00 CET\n"
                "Your meeting will be a video call on Google Meet."
            ),
        )

        self.assertEqual(
            extract_scheduled_interview_date_from_email_message(message),
            "2026-03-20",
        )

    def test_linkedin_jobs_noreply_is_unusable_for_outbound_email(self) -> None:
        self.assertTrue(is_unusable_outbound_email("jobs-noreply@linkedin.com"))

    def test_company_scoped_comeet_reply_address_is_usable(self) -> None:
        self.assertFalse(
            is_unusable_outbound_email(
                "ariel.delouya-notifications+reply-y3oghlrtff2b-93.47939@xmcyber.comeet-notifications.com"
            )
        )

    def test_generic_comeet_noreply_address_is_still_unusable(self) -> None:
        self.assertTrue(
            is_unusable_outbound_email(
                "no-reply@xmcyber.comeet-notifications.com"
            )
        )

    def test_generic_company_noreply_addresses_are_unusable(self) -> None:
        self.assertTrue(is_unusable_outbound_email("no-reply@figma.com"))
        self.assertTrue(is_unusable_outbound_email("no-reply@screenloop.com"))
        self.assertTrue(is_unusable_outbound_email("no-reply@us.greenhouse-mail.io"))

    def test_extract_email_addresses_from_text_returns_all_addresses(self) -> None:
        self.assertEqual(
            extract_email_addresses_from_text(
                "Contact recruiting@example.com or privacy@example.com."
            ),
            ["recruiting@example.com", "privacy@example.com"],
        )

    def test_detect_text_language_identifies_german_text(self) -> None:
        self.assertEqual(
            detect_text_language(
                "Leider müssen wir Ihnen mitteilen, dass wir uns für andere Bewerber entschieden haben."
            ),
            "de",
        )

    def test_extract_status_uses_german_pdf_attachment_text(self) -> None:
        message = make_email_message(
            from_header="Recruiting <jobs@example.de>",
            subject="Ihre Bewerbung",
            body="Bitte beachten Sie den Anhang.",
            attachment_text=(
                "Absage\n\n"
                "Leider müssen wir Ihnen mitteilen, dass wir uns für andere Bewerber "
                "entschieden haben. Die Position ist bereits besetzt."
            ),
        )
        self.assertEqual(extract_status_from_email_message(message), "Rejected")

    def test_follow_up_prefers_hiring_contact_over_privacy_contact(self) -> None:
        self.assertEqual(
            choose_best_contact_email_for_action(
                ["privacy@example.com", "recruiting@example.com"],
                "follow_up",
            ),
            "recruiting@example.com",
        )

    def test_deletion_prefers_privacy_contact_over_hiring_contact(self) -> None:
        self.assertEqual(
            choose_best_contact_email_for_action(
                ["recruiting@example.com", "privacy@example.com"],
                "deletion_request",
            ),
            "privacy@example.com",
        )

    def test_follow_up_prefers_person_mailbox_over_generic_contact(self) -> None:
        self.assertEqual(
            choose_best_contact_email_for_action(
                ["contact@example.com", "jane.doe@example.com"],
                "follow_up",
            ),
            "jane.doe@example.com",
        )

    def test_privacy_scores_higher_for_deletion_than_follow_up(self) -> None:
        self.assertGreater(
            score_contact_email_for_action("privacy@example.com", "deletion_request"),
            score_contact_email_for_action("privacy@example.com", "follow_up"),
        )

    def test_extract_first_json_object_handles_fenced_json(self) -> None:
        self.assertEqual(
            extract_first_json_object(
                "```json\n"
                '{"email":"privacy@example.com","source_domain":"example.com"}\n'
                "```"
            ),
            {
                "email": "privacy@example.com",
                "source_domain": "example.com",
            },
        )

    def test_extract_first_json_object_ignores_leading_prose(self) -> None:
        self.assertEqual(
            extract_first_json_object(
                'Found a grounded result.\n{"email":"jobs@example.com","source_title":"Careers"}'
            ),
            {
                "email": "jobs@example.com",
                "source_title": "Careers",
            },
        )

    def test_get_base_domain_handles_common_suffixes(self) -> None:
        self.assertEqual(get_base_domain("jobs.example.co.uk"), "example.co.uk")

    def test_get_primary_domain_label_normalizes_hyphenated_domains(self) -> None:
        self.assertEqual(get_primary_domain_label("babbel-giftshop.com"), "babbelgiftshop")

    def test_build_company_search_tokens_adds_core_joined_token_without_legal_suffix(self) -> None:
        self.assertIn("coinmarketcap", build_company_search_tokens("Coin Market Cap Ltd"))

    def test_extract_linkedin_job_urls_from_text(self) -> None:
        self.assertEqual(
            extract_linkedin_job_urls_from_text(
                "Apply here: https://www.linkedin.com/comm/jobs/view/4384454612/?trackingId=abc and ignore https://example.com"
            ),
            ["https://www.linkedin.com/comm/jobs/view/4384454612/?trackingId=abc"],
        )

    def test_extract_linkedin_company_urls_from_text(self) -> None:
        self.assertEqual(
            extract_linkedin_company_urls_from_text(
                "Company page: https://www.linkedin.com/company/example-corp/ and another https://www.linkedin.com/comm/company/example-corp/"
            ),
            [
                "https://www.linkedin.com/company/example-corp/",
                "https://www.linkedin.com/comm/company/example-corp/",
            ],
        )

    def test_search_result_relevance_rejects_unrelated_domains(self) -> None:
        self.assertFalse(
            is_search_result_likely_related_to_company(
                candidate_url="https://maps.google.fr/maps/about/#!/",
                company_name_hints=["Otera"],
                known_domains=set(),
            )
        )
        self.assertTrue(
            is_search_result_likely_related_to_company(
                candidate_url="https://www.infogain.com/careers/",
                company_name_hints=["Infogain"],
                known_domains=set(),
            )
        )

    def test_choose_preferred_role_title_rejects_low_quality_ats_subjects(self) -> None:
        self.assertEqual(
            choose_preferred_role_title(
                "Taboola! We've received your application for our Senior Release Engineer – Infrastructure",
                "Senior Release Engineer - Infrastructure",
            ),
            "Senior Release Engineer - Infrastructure",
        )

    def test_low_confidence_company_name_rejects_generic_the(self) -> None:
        self.assertTrue(is_low_confidence_company_name("the"))

    def test_company_extraction_rejects_generic_company_fragments(self) -> None:
        message = make_email_message(
            from_header="No Reply <no-reply@example.com>",
            subject="Your application to the has been received",
            body="Thanks for applying.",
        )
        self.assertEqual(extract_company_name_from_email_message(message), "")

    def test_role_extraction_rejects_open_placeholder(self) -> None:
        message = make_email_message(
            from_header="No Reply <no-reply@example.com>",
            subject="Your application for the open role",
            body="Thanks for applying.",
        )
        self.assertEqual(extract_role_titles_from_email_message(message), [])


if __name__ == "__main__":
    unittest.main()
