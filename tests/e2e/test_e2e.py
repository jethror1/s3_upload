from argparse import Namespace
from copy import deepcopy
from datetime import datetime
from glob import glob
import json
from unittest.mock import patch
import os
import shutil
import sys
from uuid import uuid4
import unittest
from unittest.mock import Mock, patch

import boto3
import pytest

from e2e import TEST_DATA_DIR
from s3_upload.s3_upload import main as s3_upload_main
from s3_upload.utils.upload import as_completed


BASE_CONFIG = {
    "max_cores": 4,
    "max_threads": 8,
    "log_level": "DEBUG",
    "log_dir": "",
    "slack_log_webhook": "log_webhook",
    "slack_alert_webhook": "alert_webhook",
    "monitor": [
        {
            "monitored_directories": [],
            "bucket": "jethro-s3-test-v2",
            "remote_path": f"s3_upload_e2e_test/{datetime.now().strftime('%y%m%d_%H%M%S')}/sequencer_a",
        }
    ],
}


class TestE2ESingleSuccessfulRun(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # create test sequencing run in set monitored directory
        cls.run_1 = os.path.join(TEST_DATA_DIR, "sequencer_a", "run_1")
        os.makedirs(cls.run_1, exist_ok=True)

        # create as a complete run
        open(os.path.join(cls.run_1, "RunInfo.xml"), "w").close()
        open(os.path.join(cls.run_1, "CopyComplete.txt"), "w").close()
        shutil.copy(
            os.path.join(TEST_DATA_DIR, "example_samplesheet.csv"),
            os.path.join(cls.run_1, "run_1_samplesheet.csv"),
        )

        # add in the sequencer to monitor with test run
        config = deepcopy(BASE_CONFIG)
        config["log_dir"] = os.path.join(TEST_DATA_DIR, "logs")
        config["monitor"][0]["monitored_directories"] = [
            os.path.join(TEST_DATA_DIR, "sequencer_a")
        ]

        cls.bucket = config["monitor"][0]["bucket"]
        cls.remote_path = config["monitor"][0]["remote_path"]

        with open(os.path.join(TEST_DATA_DIR, "config1.json"), "w") as fh:
            json.dump(config, fh)

        # mock command line args that would be set pointing to the config
        cls.mock_args = patch("s3_upload.s3_upload.parse_args").start()
        cls.mock_args.return_value = Namespace(
            config=os.path.join(TEST_DATA_DIR, "config1.json"),
            dry_run=False,
            mode="monitor",
        )

        cls.mock_flock = patch("s3_upload.s3_upload.acquire_lock").start()

        s3_upload_main()

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.run_1)

        os.remove(
            os.path.join(TEST_DATA_DIR, "logs/uploads/run_1.upload.log.json")
        )

        # # delete the logger log files
        # for log_file in glob(os.path.join(TEST_DATA_DIR, "logs", "*log*")):
        #     os.remove(log_file)

        # clean up the remote files we just uploaded
        bucket = boto3.resource("s3").Bucket(cls.bucket)
        objects = bucket.objects.filter(Prefix=cls.remote_path)
        bucket.delete_objects(
            Delete={"Objects": [{"Key": obj.key} for obj in objects]}
        )

    def test_single_complete_run_uploads_as_expected(self):
        """
        Test for our completed test sequencing run that when
        s3_upload is called pointing to that directory that it is
        correctly uploaded
        """
        # s3_upload_main()
        pass

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
        ]
        expected_remote_files = [
            os.path.join(self.remote_path, f) for f in local_files
        ]

        bucket = boto3.resource("s3").Bucket(self.bucket)
        objects = bucket.objects.filter(Prefix=self.remote_path)
        print(self.remote_path)
        print(objects)
        print([o for o in objects])
        # exit(1).start()

    def test_upload_log_file_for_fully_uploaded_run_correct(self):
        """
        Test that the log file that details what has been uploaded and
        marks a run as complete is correct
        """
        pass

    def test_uploaded_run_not_picked_up_to_upload_again(self):
        """
        Test that when monitor runs again that the uploaded run does not
        get triggered to upload again
        """

    def test_when_nothing_to_upload_that_exit_is_clean(self):
        """
        Test that when there is nothing to upload that we cleanly exit
        pass
        """
        pass
