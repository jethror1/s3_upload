from argparse import Namespace
import json
from unittest.mock import patch
import os
import shutil
import sys
from uuid import uuid4
from unittest.mock import patch

import pytest

from e2e import TEST_DATA_DIR
from s3_upload.s3_upload import main as s3_upload_main


def setup_test_directories():

    run_1 = os.path.join(TEST_DATA_DIR, "sequencer_a", "run_1")

    os.makedirs(run_1, exist_ok=True)

    # create as a complete run
    open(os.path.join(run_1, "RunInfo.xml"), "w").close()
    open(os.path.join(run_1, "CopyComplete.txt"), "w").close()

    shutil.copy(
        os.path.join(TEST_DATA_DIR, "example_samplesheet.csv"),
        os.path.join(run_1, "run_1_samplesheet.csv"),
    )


def create_config():
    pass


@patch("s3_upload.utils.upload.upload_single_file")
@patch("s3_upload.s3_upload.check_buckets_exist")
@patch("s3_upload.s3_upload.check_aws_access")
@patch("s3_upload.s3_upload.parse_args")
def test_foo(mock_args, mock_access, mock_bucket, mock_upload):
    setup_test_directories()

    config = {
        "max_cores": 4,
        "max_threads": 8,
        "log_level": "DEBUG",
        "log_dir": os.path.join(TEST_DATA_DIR, "logs"),
        "slack_log_webhook": "log_webhook",
        "slack_alert_webhook": "alert_webhook",
        "monitor": [
            {
                "monitored_directories": [
                    os.path.join(TEST_DATA_DIR, "sequencer_a")
                ],
                "bucket": "bucket_A",
                "remote_path": "/sequencer_a",
            }
        ],
    }

    mock_upload.side_effect = [
        ("file1", "abc"),
        ("file3", "abc"),
        ("file2", "abc"),
    ]

    with open(os.path.join(TEST_DATA_DIR, "config1.json"), "w") as fh:

        json.dump(config, fh)

    # with pytest.raises(SystemExit):
    #     mock_args.return_value = Namespace(
    #         config=os.path.join(TEST_DATA_DIR, "config1.json"),
    #         dry_run=True,
    #         mode="monitor",
    #     )

    #     s3_upload_main()

    #     raise RuntimeError("foo")

    mock_args.return_value = Namespace(
        config=os.path.join(TEST_DATA_DIR, "config1.json"),
        dry_run=False,
        mode="monitor",
    )

    s3_upload_main()

    raise RuntimeError("foo")
