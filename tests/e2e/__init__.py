"""
All end to end tests require that the tests are able to authenticate
with AWS as files are uploaded, and that a bucket is provided as the
environment variable `E2E_TEST_S3_BUCKET`.
"""

from pathlib import Path
import sys
import os

sys.path.append(os.path.join(Path(__file__).parent.parent.parent, "s3_upload"))


S3_BUCKET = os.environ.get("E2E_TEST_S3_BUCKET")

if not S3_BUCKET:
    raise AttributeError(
        "Required E2E_TEST_S3_BUCKET not set as environment variable"
    )

TEST_DATA_DIR = os.path.join(os.path.dirname(__file__), "test_data")

BASE_CONFIG = {
    "max_cores": 4,
    "max_threads": 8,
    "log_level": "DEBUG",
    "log_dir": "",
    "slack_log_webhook": "https://slack_webhook_log_channel",
    "slack_alert_webhook": "https://slack_webhook_alert_channel",
    "monitor": [],
}
