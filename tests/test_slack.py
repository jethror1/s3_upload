import os
import unittest
from unittest.mock import patch

from tests import TEST_DATA_DIR
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
