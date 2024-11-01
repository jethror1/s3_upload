"""
End to end tests for monitoring runs to upload with regex patterns
defined in the config file to limit what runs are uploaded

We will create 3 runs in the same directory with different samplesheets,
where the sample names in each inform what assay the sequencing is for.
We will simulate monitoring with 2 patterns to upload from the same
directory but to upload to 2 different remote paths, with one run not
matching either pattern and should be ignored
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

from e2e import BASE_CONFIG, S3_BUCKET, TEST_DATA_DIR
from e2e.helper import create_files
from s3_upload.s3_upload import main as s3_upload_main


class TestConfigRegexPatternsAgainstSampleNames(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # create test sequencing runs in one sequencer output directory
        cls.run_1 = os.path.join(TEST_DATA_DIR, "sequencer_a", "run_1")
        cls.run_2 = os.path.join(TEST_DATA_DIR, "sequencer_a", "run_2")
        cls.run_3 = os.path.join(TEST_DATA_DIR, "sequencer_a", "run_3")

        codes = ["assay_1", "assay_2", "assay_3"]

        with open(os.path.join(TEST_DATA_DIR, "example_samplesheet.csv")) as f:
            base_samplesheet = f.read().splitlines()

        # create as a complete runs with some example files
        for run_dir, code in zip((cls.run_1, cls.run_2, cls.run_3), codes):
            create_files(
                run_dir,
                "RunInfo.xml",
                "CopyComplete.txt",
                "Config/Options.cfg",
                "InterOp/EventMetricsOut.bin",
            )

            # example samplesheet has `assay_1` for all samples, replace
            # this with the current assay code we want to write in
            run_samplesheet = deepcopy(base_samplesheet)
            run_samplesheet = [
                x.replace("assay_1", code) for x in run_samplesheet
            ]

            with open(os.path.join(run_dir, "samplesheet.csv")) as fh:
                fh.write(run_samplesheet)

        # define separate full unique paths to upload test runs to
        now = datetime.now().strftime("%y%m%d_%H%M%S")
        cls.parent_remote_path = f"s3_upload_e2e_test/{now}"
        cls.run_1_remote_path = f"{cls.parent_remote_path}/assay_1"
        cls.run_2_remote_path = f"{cls.parent_remote_path}/assay_2"
        cls.run_3_remote_path = f"{cls.parent_remote_path}/assay_3"

        config_file = os.path.join(TEST_DATA_DIR, "test_config.json")

        with open(config_file, "w") as fh:
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
                        "sample_regex": "assay_1",
                    },
                    {
                        "monitored_directories": [
                            os.path.join(TEST_DATA_DIR, "sequencer_a"),
                        ],
                        "bucket": S3_BUCKET,
                        "remote_path": cls.run_2_remote_path,
                        "sample_regex": "assay_2",
                    },
                    {
                        "monitored_directories": [
                            os.path.join(TEST_DATA_DIR, "sequencer_a"),
                        ],
                        "bucket": S3_BUCKET,
                        "remote_path": cls.run_3_remote_path,
                        "sample_regex": "assay_3",
                    },
                ]
            )
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
        """Clear up all the generated test data locally and in the bucket"""
        shutil.rmtree(Path(cls.run_1).parent)

        os.remove(os.path.join(TEST_DATA_DIR, "test_config.json"))

        # delete the per run log files
        for log_file in glob(
            os.path.join(TEST_DATA_DIR, "logs/uploads", "*log.json")
        ):
            os.remove(log_file)

        # delete the logger log files
        for log_file in glob(os.path.join(TEST_DATA_DIR, "logs", "*log*")):
            os.remove(log_file)

        # clean up the remote files we just uploaded
        bucket = boto3.resource("s3").Bucket(S3_BUCKET)
        objects = bucket.objects.filter(Prefix=cls.parent_remote_path)
        bucket.delete_objects(
            Delete={"Objects": [{"Key": obj.key} for obj in objects]}
        )

        cls.mock_args.stop()
        cls.mock_flock.stop()
        cls.mock_slack.stop()
