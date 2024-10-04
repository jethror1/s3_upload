import re
import unittest
from unittest.mock import patch

import boto3
from botocore import exceptions as s3_exceptions
import pytest


from s3_upload.utils import upload
from tests import TEST_DATA_DIR


@patch("s3_upload.utils.upload.boto3.Session.resource")
class TestCheckAwsAccess(unittest.TestCase):
    def test_list_of_buckets_returned_on_aws_being_accessible(self, mock_s3):

        mock_s3.return_value.buckets.all.return_value = [
            "bucket_1",
            "bucket_2",
        ]
        returned_buckets = upload.check_aws_access()

        self.assertEqual(returned_buckets, ["bucket_1", "bucket_2"])

    def test_runtime_error_raised_on_not_being_able_to_connect(self, mock_s3):
        mock_s3.side_effect = s3_exceptions.ClientError(
            {"Error": {"Code": 1, "Message": "foo"}}, "bar"
        )

        expected_error = re.escape(
            "Error in connecting to AWS: An error occurred (1) "
            "when calling the bar operation: foo"
        )

        with pytest.raises(RuntimeError, match=expected_error):
            upload.check_aws_access()


@patch("s3_upload.utils.upload.boto3.client")
class TestCheckBucketExists(unittest.TestCase):
    def test_bucket_metadata_returned_when_bucket_exists(self, mock_client):
        bucket_metadata = {
            "ResponseMetadata": {
                "RequestId": "8WQ3PBQNX",
                "HostId": "1TvQsTG3ZQfoiuJrEFQBXBCMWFIX6DXA=",
                "HTTPStatusCode": 200,
                "HTTPHeaders": {
                    "x-amz-id-2": "Hd4YGwX1TvQsTG3ZQfoiuJrEFQBXBCMWFIX6DXA=",
                    "x-amz-request-id": "8WQ3PBQN3BX0G",
                    "date": "Fri, 04 Oct 2024 13:37:34 GMT",
                    "x-amz-bucket-region": "eu-west-2",
                    "x-amz-access-point-alias": "false",
                    "content-type": "application/xml",
                    "server": "AmazonS3",
                },
                "RetryAttempts": 0,
            },
            "BucketRegion": "eu-west-2",
            "AccessPointAlias": False,
        }

        mock_client.return_value.head_bucket.return_value = bucket_metadata

        bucket_details = upload.check_bucket_exists("jethro-s3-test-v2")

        self.assertEqual(bucket_details, bucket_metadata)
