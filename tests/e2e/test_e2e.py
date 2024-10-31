"""
End to end tests for running the upload in monitor mode.

The below tests will fully simulate the upload process of a sequencing
run by setting up test run structure, calling the main entry point and
testing the upload behaviour. This requires that the script is able to
authenticate with AWS as files are uploaded, and that a bucket is
provided as the environment variable `E2E_TEST_S3_BUCKET`.
"""

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

from e2e import TEST_DATA_DIR
from s3_upload.s3_upload import main as s3_upload_main


S3_BUCKET = os.environ.get("E2E_TEST_S3_BUCKET")

if not S3_BUCKET:
    raise AttributeError(
        "Required E2E_TEST_S3_Bucket not set as environment variable"
    )

BASE_CONFIG = {
    "max_cores": 4,
    "max_threads": 8,
    "log_level": "DEBUG",
    "log_dir": "",
    "slack_log_webhook": "https://slack_webhook_log_channel",
    "slack_alert_webhook": "https://slack_webhook_log_channel",
    "monitor": [],
}


def create_files(run_dir, *files):
    """
    Create the given files and intermediate paths provided from the
    given test run directory

    Parameters
    ----------
    run_dir : str
        path to test run directory

    files : list
        files and relative paths to create
    """
    for file_path in files:
        full_path = os.path.join(run_dir, file_path)
        parent_dir = Path(full_path).parent

        os.makedirs(parent_dir, exist_ok=True)
        open(full_path, encoding="utf-8", mode="a").close()


class TestSingleCompleteRun(unittest.TestCase):
    """
    End to end tests for a single complete run in a monitored directory
    being correctly uploaded into the specified bucket and remote path.
    """

    @classmethod
    def setUpClass(cls):
        # create test sequencing run in set monitored directory
        cls.run_1 = os.path.join(TEST_DATA_DIR, "sequencer_a", "run_1")

        # create as a complete run with some example files
        create_files(
            cls.run_1,
            "RunInfo.xml",
            "CopyComplete.txt",
            "Config/Options.cfg",
            "InterOp/EventMetricsOut.bin",
        )

        shutil.copy(
            os.path.join(TEST_DATA_DIR, "example_samplesheet.csv"),
            os.path.join(cls.run_1, "run_1_samplesheet.csv"),
        )

        # define full unique path to upload test runs to
        now = datetime.now().strftime("%y%m%d_%H%M%S")
        cls.remote_path = f"s3_upload_e2e_test/{now}/sequencer_a"

        # add in the sequencer to monitor with test run
        config_file = os.path.join(TEST_DATA_DIR, "test_config.json")
        config = deepcopy(BASE_CONFIG)
        config["log_dir"] = os.path.join(TEST_DATA_DIR, "logs")
        config["monitor"].append(
            {
                "monitored_directories": [
                    os.path.join(TEST_DATA_DIR, "sequencer_a")
                ],
                "bucket": S3_BUCKET,
                "remote_path": cls.remote_path,
            }
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
        shutil.rmtree(cls.run_1)

        os.remove(
            os.path.join(TEST_DATA_DIR, "logs/uploads/run_1.upload.log.json")
        )

        os.remove(os.path.join(TEST_DATA_DIR, "test_config.json"))

        # delete the logger log files
        for log_file in glob(os.path.join(TEST_DATA_DIR, "logs", "*log*")):
            os.remove(log_file)

        # clean up the remote files we just uploaded
        bucket = boto3.resource("s3").Bucket(S3_BUCKET)
        objects = bucket.objects.filter(Prefix=cls.remote_path)
        bucket.delete_objects(
            Delete={"Objects": [{"Key": obj.key} for obj in objects]}
        )

        cls.mock_args.stop()
        cls.mock_flock.stop()
        cls.mock_slack.stop()

    def test_remote_files_upload_correctly(self):
        """
        Test that the remote files are uploaded with the same directory
        structure as local and to the specified bucket:/path from the
        config file
        """
        local_files = [
            "RunInfo.xml",
            "CopyComplete.txt",
            "run_1_samplesheet.csv",
            "Config/Options.cfg",
            "InterOp/EventMetricsOut.bin",
        ]
        expected_remote_files = sorted(
            [os.path.join(self.remote_path, "run_1", f) for f in local_files]
        )

        bucket = boto3.resource("s3").Bucket(S3_BUCKET)
        uploaded_objects = bucket.objects.filter(Prefix=self.remote_path)
        uploaded_files = sorted([x.key for x in uploaded_objects])

        self.assertEqual(uploaded_files, expected_remote_files)

    def test_upload_log_file_for_fully_uploaded_run_correct(self):
        """
        Test that the log file that details what has been uploaded and
        marks a run as complete is correct.

        The uploaded files contains ETag IDs return from S3, so we will
        check separately just for the logged uploaded files
        """
        expected_top_level_log_contents = {
            "run_id": "run_1",
            "run path": "/home/jethro/Projects/s3_upload/tests/e2e/test_data/sequencer_a/run_1",
            "completed": True,
            "total_local_files": 5,
            "total_uploaded_files": 5,
            "total_failed_upload": 0,
            "failed_upload_files": [],
        }
        expected_uploaded_files = [
            "/home/jethro/Projects/s3_upload/tests/e2e/test_data/sequencer_a/run_1/CopyComplete.txt",
            "/home/jethro/Projects/s3_upload/tests/e2e/test_data/sequencer_a/run_1/RunInfo.xml",
            "/home/jethro/Projects/s3_upload/tests/e2e/test_data/sequencer_a/run_1/run_1_samplesheet.csv",
            "/home/jethro/Projects/s3_upload/tests/e2e/test_data/sequencer_a/run_1/Config/Options.cfg",
            "/home/jethro/Projects/s3_upload/tests/e2e/test_data/sequencer_a/run_1/InterOp/EventMetricsOut.bin",
        ]

        upload_log = os.path.join(
            TEST_DATA_DIR, "logs/uploads/run_1.upload.log.json"
        )

        with open(upload_log, "r") as fh:
            log_contents = json.load(fh)

        with self.subTest("correct top level of log"):
            self.assertDictContainsSubset(
                expected_top_level_log_contents, log_contents
            )

        with self.subTest("correct local files uploaded in log"):
            uploaded_files = sorted(log_contents["uploaded_files"].keys())

            self.assertEqual(sorted(expected_uploaded_files), uploaded_files)

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
                ":white_check_mark: S3 Upload: Successfully uploaded 1"
                " runs\n\t:black_square: run_1"
            )

            self.assertEqual(
                self.mock_slack.call_args[1]["message"], expected_message
            )


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
        cls.remote_path = f"s3_upload_e2e_test/{now}/sequencer_a"

        # add in the sequencer to monitor with test run
        config_file = os.path.join(TEST_DATA_DIR, "test_config.json")
        config = deepcopy(BASE_CONFIG)
        config["log_dir"] = os.path.join(TEST_DATA_DIR, "logs")
        config["monitor"].append(
            {
                "monitored_directories": [
                    os.path.join(TEST_DATA_DIR, "sequencer_a"),
                    os.path.join(TEST_DATA_DIR, "sequencer_b"),
                ],
                "bucket": S3_BUCKET,
                "remote_path": cls.remote_path,
            }
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
        shutil.rmtree(cls.run_1)

        os.remove(
            os.path.join(TEST_DATA_DIR, "logs/uploads/run_1.upload.log.json")
        )

        os.remove(os.path.join(TEST_DATA_DIR, "test_config.json"))

        # delete the logger log files
        for log_file in glob(os.path.join(TEST_DATA_DIR, "logs", "*log*")):
            os.remove(log_file)

        # clean up the remote files we just uploaded
        bucket = boto3.resource("s3").Bucket(S3_BUCKET)
        objects = bucket.objects.filter(Prefix=cls.remote_path)
        bucket.delete_objects(
            Delete={"Objects": [{"Key": obj.key} for obj in objects]}
        )

        cls.mock_args.stop()
        cls.mock_flock.stop()
        cls.mock_slack.stop()
