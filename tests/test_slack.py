from requests.exceptions import RequestException
import unittest
from unittest.mock import patch

from s3_upload.utils import slack


class TestFormatCompleteMessage(unittest.TestCase):
    def test_only_completed_runs_message_correct(self):
        compiled_message = slack.format_complete_message(
            completed=["run_1", "run_2"]
        )

        expected_message = (
            "Completed run uploads. 2 successfully uploaded. 0"
            " failed uploading.\n\nSuccessfully uploaded"
            " runs\n\t:black_square: run_1\n\t:black_square: run_2"
        )

        self.assertEqual(compiled_message, expected_message)

    def test_only_failed_runs_message_correct(self):
        compiled_message = slack.format_complete_message(
            failed=["run_1", "run_2"]
        )

        expected_message = (
            "Completed run uploads. 0 successfully uploaded. "
            "2 failed uploading.\n\nFailed uploading runs"
            "\n\t:black_square: run_1\n\t:black_square: run_2"
        )

        self.assertEqual(compiled_message, expected_message)

    def test_completed_and_failed_runs_in_same_message(self):
        compiled_message = slack.format_complete_message(
            completed=["run_1", "run_2"], failed=["run_3", "run_4"]
        )

        expected_message = (
            "Completed run uploads. 2 successfully uploaded. 2"
            " failed uploading.\n\nSuccessfully uploaded"
            " runs\n\t:black_square: run_1\n\t:black_square: run_2"
            "\n\nFailed uploading runs"
            "\n\t:black_square: run_3\n\t:black_square: run_4"
        )

        self.assertEqual(compiled_message, expected_message)


@patch("s3_upload.utils.slack.requests.post")
class TestPostMessage(unittest.TestCase):
    def test_params_set_to_requests_post(self, mock_post):
        slack.post_message(
            url="https://hooks.slack.com/services/00001",
            message="test message",
        )

        expected_call_args = {
            "url": "https://hooks.slack.com/services/00001",
            "data": '{"text": ":arrow_up: S3 Upload\\n\\ntest message"}',
            "headers": {"content-type": "application/json"},
            "timeout": 30,
        }

        self.assertDictEqual(dict(mock_post.call_args[1]), expected_call_args)

    def test_error_logged_when_non_200_response_received(self, mock_post):
        mock_post.return_value.status_code = 400
        mock_post.return_value.text = "message failed to send :sadpanda:"

        with self.assertLogs("s3_upload", level="DEBUG") as log:
            slack.post_message(
                url="https://hooks.slack.com/services/00001",
                message="test message",
            )

            expected_log_error = (
                "Error in post request to Slack (400): message failed to send"
                " :sadpanda:"
            )

            self.assertIn(expected_log_error, "".join(log.output))

    def test_error_logged_when_posting_raises_a_request_exception(
        self, mock_post
    ):
        mock_post.side_effect = RequestException("failed to post")

        with self.assertLogs("s3_upload", level="DEBUG") as log:
            slack.post_message(
                url="https://hooks.slack.com/services/00001",
                message="test message",
            )

            expected_log_error = (
                "Error in post request to Slack: failed to post"
            )

            self.assertIn(expected_log_error, "".join(log.output))
