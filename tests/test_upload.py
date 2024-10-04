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


@patch("s3_upload.utils.upload.boto3.session.Session.client")
class TestUploadSingleFile(unittest.TestCase):
    def test_upload_path_correctly_set_from_input_file_and_parent_path(
        self, mock_client
    ):
        """
        Path to upload in the bucket is set from the specified remote
        path base directory, the path of the local file and the parent
        path to remove. Test that different combinations of the above
        results in the expected upload file path.
        """

        expected_inputs_and_upload_path = [
            {
                "remote_path": "/bucket_dir1/",
                "local_file": "/path/to/monitored_dir/run1/Samplesheet.csv",
                "parent_path": "/path/to/monitored_dir/",
                "expected_upload_path": "bucket_dir1/run1/Samplesheet.csv",
            },
            {
                "remote_path": "/bucket_dir_1/bucket_dir_2",
                "local_file": "/path/to/monitored_dir/run1/Samplesheet.csv",
                "parent_path": "/path/to/monitored_dir/",
                "expected_upload_path": (
                    "bucket_dir_1/bucket_dir_2/run1/Samplesheet.csv"
                ),
            },
            {
                "remote_path": "/",
                "local_file": "/one_level_parent/run1/Samplesheet.csv",
                "parent_path": "/one_level_parent/",
                "expected_upload_path": "run1/Samplesheet.csv",
            },
        ]

        for args in expected_inputs_and_upload_path:
            with self.subTest():
                upload.upload_single_file(
                    s3_client=boto3.client(),
                    bucket="test_bucket",
                    remote_path=args["remote_path"],
                    local_file=args["local_file"],
                    parent_path=args["parent_path"],
                )

                self.assertEqual(
                    mock_client.return_value.upload_file.call_args[1][
                        "object_name"
                    ],
                    args["expected_upload_path"],
                )

    def test_local_file_name_and_object_id_returned(self, mock_client):
        mock_client.return_value.get_object.return_value = {
            "ETag": "1TvQsTG3ZQfoiuJrEFQBXBCMWFIX6DXA"
        }

        local_file, remote_id = upload.upload_single_file(
            s3_client=boto3.client(),
            bucket="test_bucket",
            remote_path="/",
            local_file="/path/to/monitored_dir/run1/Samplesheet.csv",
            parent_path="/path/to/monitored_dir/",
        )

        self.assertEqual(
            (local_file, remote_id),
            (
                "/path/to/monitored_dir/run1/Samplesheet.csv",
                "1TvQsTG3ZQfoiuJrEFQBXBCMWFIX6DXA",
            ),
        )


@patch("s3_upload.utils.upload.upload_single_file")
@patch("s3_upload.utils.upload.boto3.session.Session.client")
@patch("s3_upload.utils.upload.concurrent.futures.as_completed")
class TestMultiThreadUpload(unittest.TestCase):
    def test_upload_function_called_with_correct_args(
        self, mock_thread, mock_client, mock_upload
    ):

        local_files = [
            "/path/to/monitored_dir/run1/Samplesheet.csv",
            "/path/to/monitored_dir/run1/RunInfo.xml",
            "/path/to/monitored_dir/run1/CopyComplete.txt",
        ]

        uploaded_files = upload.multi_thread_upload(
            files=local_files,
            bucket="test_bucket",
            remote_path="/",
            threads=4,
            parent_path="/path/to/monitored_dir/",
        )
