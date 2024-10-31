from argparse import Namespace
from copy import deepcopy
from datetime import datetime
from glob import glob
import json
from pathlib import Path
import unittest
from unittest.mock import patch
import os
import shutil

import boto3

from e2e import BASE_CONFIG, S3_BUCKET, TEST_DATA_DIR
from e2e.helper import create_files
from s3_upload.s3_upload import main as s3_upload_main


class TestTwoCompleteRunsInSeparateMonitorDirectories(unittest.TestCase):
    """
    End to end tests for uploading 2 completed sequencing runs that are
    in separate monitored directories and being uploaded into separate
    remote paths in the same specified bucket.

    We will create locally `sequencer_a/run_1` and `sequencer_b/run_2`
    which we then expect to upload to `s3_upload_e2e_test/{now}/
    sequencer_a/run_1` and ``s3_upload_e2e_test/{now}/sequencer_b/run_2`
    respectively.
    """

    @classmethod
    def setUpClass(cls):
        # create test sequencing runs in set monitored directories
        cls.run_1 = os.path.join(TEST_DATA_DIR, "sequencer_a", "run_1")
        cls.run_2 = os.path.join(TEST_DATA_DIR, "sequencer_b", "run_2")

        # create as a complete runs with some example files
        for run in (cls.run_1, cls.run_2):
            create_files(
                run,
                "RunInfo.xml",
                "CopyComplete.txt",
                "Config/Options.cfg",
                "InterOp/EventMetricsOut.bin",
            )

            shutil.copy(
                os.path.join(TEST_DATA_DIR, "example_samplesheet.csv"),
                os.path.join(run, "samplesheet.csv"),
            )

        # define full unique path to upload test runs to
        now = datetime.now().strftime("%y%m%d_%H%M%S")
        cls.run_1_remote_path = f"s3_upload_e2e_test/{now}/sequencer_a"
        cls.run_2_remote_path = f"s3_upload_e2e_test/{now}/sequencer_b"

        # add in the sequencer directories to monitor with test run
        config_file = os.path.join(TEST_DATA_DIR, "test_config.json")
        config = deepcopy(BASE_CONFIG)
        config["log_dir"] = os.path.join(TEST_DATA_DIR, "logs")
        config["monitor"].extend(
            [
                {
                    "monitored_directories": [
                        os.path.join(TEST_DATA_DIR, "sequencer_a"),
                    ],
                    "bucket": S3_BUCKET,
                    "remote_path": cls.run_1_remote_path,
                },
                {
                    "monitored_directories": [
                        os.path.join(TEST_DATA_DIR, "sequencer_b"),
                    ],
                    "bucket": S3_BUCKET,
                    "remote_path": cls.run_2_remote_path,
                },
            ]
        )

        with open(config_file, "w") as fh:
            json.dump(config, fh)

        # mock command line args that would be set pointing to the config
        cls.mock_args = patch("s3_upload.s3_upload.parse_args").start()
        cls.mock_args.return_value = Namespace(
            config=config_file,
            dry_run=False,
            mode="monitor",
        )

        # mock the file lock that stops concurrent uploads as this breaks
        # when running unittest
        cls.mock_flock = patch("s3_upload.s3_upload.acquire_lock").start()

        cls.mock_slack = patch(
            "s3_upload.s3_upload.slack.post_message"
        ).start()

        # call the main entry point to run the upload
        s3_upload_main()

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(Path(cls.run_1).parent)
        shutil.rmtree(Path(cls.run_2).parent)

        os.remove(
            os.path.join(TEST_DATA_DIR, "logs/uploads/run_1.upload.log.json")
        )
        os.remove(
            os.path.join(TEST_DATA_DIR, "logs/uploads/run_2.upload.log.json")
        )

        os.remove(os.path.join(TEST_DATA_DIR, "test_config.json"))

        # delete the logger log files
        for log_file in glob(os.path.join(TEST_DATA_DIR, "logs", "*log*")):
            os.remove(log_file)

        # clean up the remote files we just uploaded
        bucket = boto3.resource("s3").Bucket(S3_BUCKET)
        objects = bucket.objects.filter(
            Prefix=cls.run_1_remote_path.replace("/sequencer_a", "")
        )
        bucket.delete_objects(
            Delete={"Objects": [{"Key": obj.key} for obj in objects]}
        )

        cls.mock_args.stop()
        cls.mock_flock.stop()
        cls.mock_slack.stop()

    def test_remote_file_uploaded_correctly(self):
        """
        Test that both runs upload correctly into the correct locations
        """
        pass

    def test_upload_log_files_for_fully_uploaded_runs_correct(self):
        """
        Test the upload logs for both runs are as expected
        """
        pass

    def test_slack_post_message_after_uploading_as_expected(self):
        with self.subTest("only one Slack message sent"):
            self.assertEqual(self.mock_slack.call_count, 1)

        with self.subTest("correct webhook used"):
            self.assertEqual(
                self.mock_slack.call_args[1]["url"],
                BASE_CONFIG["slack_log_webhook"],
            )

        with self.subTest("message formatted as expected"):
            expected_message = (
                ":white_check_mark: S3 Upload: Successfully uploaded 2"
                " runs\n\t:black_square: run_1\n\t:black_square: run_2"
            )

            self.assertEqual(
                self.mock_slack.call_args[1]["message"], expected_message
            )

    @patch("s3_upload.s3_upload.sys.exit")
    def test_uploaded_run_not_picked_up_to_upload_again(self, mock_exit):
        """
        Test that when monitor runs again that the uploaded run does not
        get triggered to upload again
        """
        with self.assertLogs("s3_upload") as log:
            s3_upload_main()

            self.assertIn(
                "No sequencing runs requiring upload found. Exiting now.",
                "".join(log.output),
            )

    @patch("s3_upload.s3_upload.sys.exit")
    def test_when_nothing_to_upload_that_exit_code_is_zero(self, mock_exit):
        """
        Test that when there is nothing to upload that we cleanly exit
        """
        s3_upload_main()

        with self.subTest("exit called"):
            self.assertEqual(mock_exit.call_count, 1)

        with self.subTest("exit code"):
            self.assertEqual(mock_exit.call_args[0][0], 0)
