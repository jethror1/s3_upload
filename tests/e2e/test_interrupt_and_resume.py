"""
End to end test for an upload being interrupted and failing to upload
all files, then resuming on the next run.
"""

from argparse import Namespace
from copy import deepcopy
from datetime import datetime
from glob import glob
import json
import unittest
from unittest.mock import patch
import os
import shutil

import boto3

from e2e import BASE_CONFIG, S3_BUCKET, TEST_DATA_DIR
from e2e.helper import create_files
from s3_upload.s3_upload import main as s3_upload_main
from s3_upload.utils.upload import upload_single_file


class TestInterruptAndResume(unittest.TestCase):
    @staticmethod
    def upload_side_effect(**kwargs):
        """
        Helper function to pass to the side_effect param when mocking
        the upload_single_file function.

        This allows us to simulate failing the upload of a single file
        whilst still uploading the other files, resulting in a partially
        uploaded run.

        For all files except the RunInfo.xml we will simply pass through
        the call to upload_single_file with the provided arguments.
        """
        if kwargs["local_file"].endswith("RunInfo.xml"):
            raise RuntimeError("Interrupting upload of RunInfo.xml")

        return upload_single_file(**kwargs)

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
            os.path.join(cls.run_1, "samplesheet.csv"),
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

        # call the main entry point to run the upload, with a side effect
        # of failing to upload the RunInfo.xml file
        with patch(
            "s3_upload.utils.upload.upload_single_file",
            side_effect=cls.upload_side_effect,
        ) as mock_upload:
            s3_upload_main()

        # read in the upload log after a partial upload to test state
        with open(
            os.path.join(TEST_DATA_DIR, "logs/uploads/run_1.upload.log.json"),
            encoding="utf8",
            mode="r",
        ) as fh:
            cls.partial_upload_log = json.load(fh)

        # capture the stdout/stderr logs written to log file for testing
        with open(
            os.path.join(TEST_DATA_DIR, "logs/s3_upload.log"),
            encoding="utf8",
            mode="r",
        ) as fh:
            cls.upload_log = fh.read().splitlines()

        # call the upload again the simulate running on the next hour
        s3_upload_main()

        # read in the upload log after a upload should have completed
        with open(
            os.path.join(TEST_DATA_DIR, "logs/uploads/run_1.upload.log.json"),
            encoding="utf8",
            mode="r",
        ) as fh:
            cls.complete_upload_log = json.load(fh)

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
        objects = [{"Key": obj.key} for obj in objects]

        if objects:
            bucket.delete_objects(Delete={"Objects": objects})

        cls.mock_args.stop()
        cls.mock_flock.stop()
        cls.mock_slack.stop()

    def test_partially_uploaded_log_file_as_expected(self):
        """
        Test that the run log file after we failed to upload RunInfo.xml
        is as expected and indicates a partially uploaded run to still upload
        """
        run_dir = os.path.join(TEST_DATA_DIR, "sequencer_a/run_1")
        expected_top_level_contents = {
            "run_id": "run_1",
            "run_path": run_dir,
            "completed": False,
            "total_local_files": 5,
            "total_uploaded_files": 4,
            "total_failed_upload": 1,
        }

        expected_uploaded_files = [
            "CopyComplete.txt",
            "samplesheet.csv",
            "Config/Options.cfg",
            "InterOp/EventMetricsOut.bin",
        ]
        expected_uploaded_files = sorted(
            [os.path.join(run_dir, f) for f in expected_uploaded_files]
        )

        expected_failed_files = [os.path.join(run_dir, "RunInfo.xml")]

        with self.subTest("top level correct"):
            self.assertEqual(
                self.partial_upload_log,
                {**self.partial_upload_log, **expected_top_level_contents},
            )

        with self.subTest("uploaded_files_correct"):
            self.assertEqual(
                sorted(expected_uploaded_files),
                sorted(self.partial_upload_log["uploaded_files"]),
            )

        with self.subTest("failed upload files correct"):
            self.assertEqual(
                expected_failed_files,
                self.partial_upload_log["failed_upload_files"],
            )

    def test_complete_upload_log_file_as_expected(self):
        """
        Test that the upload log after running the upload again is as
        expected and that the upload has completed
        """
        run_dir = os.path.join(TEST_DATA_DIR, "sequencer_a/run_1")

        expected_top_level_contents = {
            "run_id": "run_1",
            "run_path": run_dir,
            "completed": True,
            "total_local_files": 5,
            "total_uploaded_files": 5,
            "total_failed_upload": 0,
            "failed_upload_files": [],
        }

        expected_uploaded_files = [
            "RunInfo.xml",
            "CopyComplete.txt",
            "samplesheet.csv",
            "Config/Options.cfg",
            "InterOp/EventMetricsOut.bin",
        ]
        expected_uploaded_files = sorted(
            [os.path.join(run_dir, f) for f in expected_uploaded_files]
        )

        with self.subTest("top level correct"):
            self.assertEqual(
                self.complete_upload_log,
                {**self.complete_upload_log, **expected_top_level_contents},
            )

        with self.subTest("uploaded_files_correct"):
            self.assertEqual(
                sorted(expected_uploaded_files),
                sorted(self.complete_upload_log["uploaded_files"]),
            )

        with self.subTest("failed upload files correct"):
            self.assertEqual(
                [],
                self.complete_upload_log["failed_upload_files"],
            )
