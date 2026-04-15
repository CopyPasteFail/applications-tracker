import unittest

from tracker import Tracker


class GroupingClusterTests(unittest.TestCase):
    def test_cluster_builder_allows_emergency_overflow_by_two(self) -> None:
        tracker = Tracker.__new__(Tracker)
        existing_apps = []
        new_emails = [
            {
                "id": f"msg-{index}",
                "timestamp": index,
                "thread_id": "thread-1",
                "subject": f"Re: Example application update {index}",
                "from": "Recruiter <recruiter@example.com>",
                "body": "Following up on your application for Platform Engineer at Example.",
                "snippet": "application update",
                "attachment_text": "",
            }
            for index in range(13)
        ]

        clusters = tracker._build_grouping_clusters(
            new_emails=new_emails,
            existing_apps=existing_apps,
            transport_window_size=25,
            cluster_size_limit=10,
            cluster_emergency_margin=2,
        )

        self.assertEqual([cluster["email_count"] for cluster in clusters], [12, 1])
        self.assertTrue(clusters[0]["emergency_overflow_used"])
        self.assertFalse(clusters[1]["emergency_overflow_used"])


if __name__ == "__main__":
    unittest.main()
