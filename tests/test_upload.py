from concurrent.futures import Future, ProcessPoolExecutor, ThreadPoolExecutor
import re
import unittest
from unittest.mock import ANY, call, patch

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
class TestCheckBucketsExist(unittest.TestCase):
    def test_bucket_metadata_returned_when_bucket_exists(self, mock_client):
        valid_bucket_metadata = {
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

        mock_client.return_value.head_bucket.return_value = (
            valid_bucket_metadata
        )

        bucket_details = upload.check_buckets_exist("jethro-s3-test-v2")

        self.assertEqual(bucket_details, [valid_bucket_metadata])

    def test_runtime_error_raised_if_one_or_more_buckets_not_valid(
        self, mock_client
    ):
        mock_client.side_effect = [
            s3_exceptions.ClientError(
                {"Error": {"Code": 1, "Message": "foo"}}, "bar"
            ),
            s3_exceptions.ClientError(
                {"Error": {"Code": 1, "Message": "baz"}}, "blarg"
            ),
        ]

        expected_error = (
            "2 bucket(s) not accessible / do not exist: invalid_bucket_1,"
            " invalid_bucket_2"
        )

        with pytest.raises(RuntimeError, match=re.escape(expected_error)):
            upload.check_buckets_exist("invalid_bucket_1", "invalid_bucket_2")

    def test_client_error_raised_when_bucket_does_not_exist(self, mock_client):
        mock_client.side_effect = s3_exceptions.ClientError(
            {"Error": {"Code": 1, "Message": "foo"}}, "bar"
        )

        expected_error = "1 bucket(s) not accessible / do not exist: s3-test"

        with pytest.raises(RuntimeError, match=re.escape(expected_error)):
            upload.check_buckets_exist("s3-test")


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
@patch("s3_upload.utils.upload.as_completed")
class TestMultiThreadUpload(unittest.TestCase):
    local_files = [
        "/path/to/monitored_dir/run1/Samplesheet.csv",
        "/path/to/monitored_dir/run1/RunInfo.xml",
        "/path/to/monitored_dir/run1/CopyComplete.txt",
    ]

    def test_correct_number_of_calls_to_upload(
        self, mock_thread, mock_client, mock_upload
    ):
        upload.multi_thread_upload(
            files=self.local_files,
            bucket="test_bucket",
            remote_path="/",
            threads=4,
            parent_path="/path/to/monitored_dir/",
        )

        self.assertEqual(mock_upload.call_count, 3)

    @patch("s3_upload.utils.upload.ThreadPoolExecutor")
    def test_correct_number_of_threads_set(
        self, mock_pool, mock_thread, mock_client, mock_upload
    ):
        for thread in [1, 4]:
            with self.subTest(f"{thread} thread(s) set to use"):
                upload.multi_thread_upload(
                    files=self.local_files,
                    bucket="test_bucket",
                    remote_path="/",
                    threads=thread,
                    parent_path="/path/to/monitored_dir/",
                )
                self.assertEqual(mock_pool.call_args[1]["max_workers"], thread)

    def test_upload_function_called_with_correct_args(
        self, mock_thread, mock_client, mock_upload
    ):
        upload.multi_thread_upload(
            files=self.local_files,
            bucket="test_bucket",
            remote_path="/",
            threads=4,
            parent_path="/path/to/monitored_dir/",
        )

        expected_call_args_for_all_calls = [
            call(
                s3_client=ANY,
                bucket="test_bucket",
                remote_path="/",
                local_file="/path/to/monitored_dir/run1/Samplesheet.csv",
                parent_path="/path/to/monitored_dir/",
            ),
            call(
                s3_client=ANY,
                bucket="test_bucket",
                remote_path="/",
                local_file="/path/to/monitored_dir/run1/RunInfo.xml",
                parent_path="/path/to/monitored_dir/",
            ),
            call(
                s3_client=ANY,
                bucket="test_bucket",
                remote_path="/",
                local_file="/path/to/monitored_dir/run1/CopyComplete.txt",
                parent_path="/path/to/monitored_dir/",
            ),
        ]

        self.assertEqual(
            expected_call_args_for_all_calls, mock_upload.call_args_list
        )

    def test_correct_file_and_id_returned(
        self, mock_thread, mock_client, mock_upload
    ):
        # each upload call per thread will return a dict containing the
        # ETag of the remote object, patch this in to the future object
        # that each thread will return
        mock_thread.return_value = [Future(), Future(), Future()]

        return_values = [
            ("/path/to/monitored_dir/run1/Samplesheet.csv", "abc"),
            ("/path/to/monitored_dir/run1/RunInfo.xml", "def"),
            ("/path/to/monitored_dir/run1/CopyComplete.txt", "ghi"),
        ]

        for i, j in zip(mock_thread.return_value, return_values):
            i.set_result(j)

        returned_uploaded_file_mapping = upload.multi_thread_upload(
            files=self.local_files,
            bucket="test_bucket",
            remote_path="/",
            threads=4,
            parent_path="/path/to/monitored_dir/",
        )

        expected_local_file_to_remote_id_mapping = {
            "/path/to/monitored_dir/run1/Samplesheet.csv": "abc",
            "/path/to/monitored_dir/run1/RunInfo.xml": "def",
            "/path/to/monitored_dir/run1/CopyComplete.txt": "ghi",
        }

        self.assertEqual(
            expected_local_file_to_remote_id_mapping,
            returned_uploaded_file_mapping,
        )


class TestMultiCoreUpload(unittest.TestCase):

    local_files = [
        ["/path/to/monitored_dir/run1/Samplesheet.csv"],
        ["/path/to/monitored_dir/run1/RunInfo.xml"],
        ["/path/to/monitored_dir/run1/CopyComplete.txt"],
    ]

    @patch("s3_upload.utils.upload.as_completed")
    @patch("s3_upload.utils.upload.ProcessPoolExecutor")
    def test_correct_number_of_process_pools_set_from_cores_arg(
        self, mock_pool, mock_completed
    ):
        for core in [1, 4]:
            with self.subTest(f"{core} core(s) set to use"):
                upload.multi_core_upload(
                    files=self.local_files,
                    bucket="test_bucket",
                    remote_path="/",
                    cores=core,
                    threads=1,
                    parent_path="/path/to/monitored_dir/",
                )
                self.assertEqual(mock_pool.call_args[1]["max_workers"], core)

    @patch("s3_upload.utils.upload.as_completed")
    @patch("s3_upload.utils.upload.ProcessPoolExecutor")
    def test_returned_file_mapping_correct(self, mock_pool, mock_completed):
        # each ProcessPool should return a dict mapping from each ThreadPool
        # of local file to remote object ID, these should then be finally
        # merged and returned as a single level dict
        mock_completed.return_value = [Future(), Future(), Future()]

        return_values = [
            {"/path/to/monitored_dir/run1/Samplesheet.csv": "abc"},
            {"/path/to/monitored_dir/run1/RunInfo.xml": "def"},
            {"/path/to/monitored_dir/run1/CopyComplete.txt": "ghi"},
        ]

        for i, j in zip(mock_completed.return_value, return_values):
            i.set_result(j)

        returned_uploaded_file_mapping = upload.multi_core_upload(
            files=self.local_files,
            bucket="test_bucket",
            remote_path="/",
            cores=3,
            threads=1,
            parent_path="/path/to/monitored_dir/",
        )

        expected_local_file_to_remote_id_mapping = {
            "/path/to/monitored_dir/run1/Samplesheet.csv": "abc",
            "/path/to/monitored_dir/run1/RunInfo.xml": "def",
            "/path/to/monitored_dir/run1/CopyComplete.txt": "ghi",
        }

        self.assertEqual(
            expected_local_file_to_remote_id_mapping,
            returned_uploaded_file_mapping,
        )
