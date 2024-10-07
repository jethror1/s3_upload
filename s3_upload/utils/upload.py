"""Functions for handling uploading into S3"""

from concurrent.futures import (
    ProcessPoolExecutor,
    ThreadPoolExecutor,
    as_completed,
)
from os import path
import re
from typing import List

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.config import Config
from botocore import exceptions as s3_exceptions

from utils.log import get_logger


log = get_logger("s3 upload")


def check_aws_access():
    """
    Check authentication with AWS S3 with stored credentials by checking
    access to all buckets

    Returns
    -------
    list
        list of available S3 buckets

    Raises
    ------
    botocore.exceptions.ClientError
        Raised when unable to connect to AWS
    """
    log.info("Checking access to AWS")
    try:
        return list(boto3.Session().resource("s3").buckets.all())
    except s3_exceptions.ClientError as err:
        raise RuntimeError(f"Error in connecting to AWS: {err}") from err


def check_buckets_exist(*buckets) -> List[dict]:
    """
    Check that the provided bucket(s) exist and are accessible

    Parameters
    ----------
    buckets : list
        S3 bucket(s) to check access for

    Returns
    -------
    list
        lists of dicts with bucket metadata

    Raises
    ------
    RuntimeError
        Raised when one or more buckets do not exist / not accessible
    """
    log.info("Checking bucket(s) %s exist", buckets)

    valid = []
    invalid = []

    for bucket in buckets:
        try:
            valid.append(boto3.client("s3").head_bucket(Bucket=bucket))
        except s3_exceptions.ClientError:
            invalid.append(bucket)

    if invalid:
        raise RuntimeError(
            f"{len(invalid) } bucket(s) not accessible / do not exist: "
            f"{', '.join(invalid)}"
        )


def upload_single_file(
    s3_client, bucket, remote_path, local_file, parent_path
):
    """
    Uploads single file into S3 storage bucket

    Parameters
    ----------
    client : bootcore.client.S3
        boto3 S3 client
    bucket : str
        S3 bucket to upload to
    remote_path : str
        parent directory in bucket to upload to
    local_file : str
        file and path to upload
    parent_path : str
        path to parent of sequencing directory, will be removed from
        the file path for uploading to not form part of the remote path

    Returns
    -------
    str
        path and filename of uploaded file
    str
        ETag attribute of the uploaded file
    """
    # remove base directory and join to specified S3 location
    upload_file = re.sub(rf"^{parent_path}", "", local_file)
    upload_file = path.join(remote_path, upload_file).lstrip("/")

    # set threshold for splitting across cores to 1GB and to not use
    # multiple threads for single file upload to allow us to control
    # this better from the config
    config = TransferConfig(multipart_threshold=1024**3, use_threads=False)
    s3_client.upload_file(
        file_name=local_file,
        bucket=bucket,
        object_name=upload_file,
        Config=config,
    )

    # ensure we can access the remote file to log the object ID
    remote_object = s3_client.get_object(Bucket=bucket, Key=upload_file)

    return local_file, remote_object.get("ETag").strip('"')


def multi_thread_upload(
    files, bucket, remote_path, threads, parent_path
) -> list:
    """
    Uploads the given set of `files` to S3 on a single CPU core using
    maximum of n threads

    Parameters
    ----------
    files : list
        list of local files to upload
    bucket : str
        S3 bucket to upload to
    remote_path : str
        parent directory in bucket to upload to
    threads : int
        maximum number of threaded process to open per core
    parent_path : str
        path to parent of sequencing directory, will be removed from
        the file path for uploading to not form part of the remote path
    Returns
    -------
    dict
        mapping of local file to ETag ID of uploaded file
    """
    # defining one S3 client per core due to boto3 clients being thread
    # safe but not safe to share across processes due to expected response
    # ordering: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/clients.html#caveats
    session = boto3.session.Session()
    s3_client = session.client(
        "s3", config=Config(retries={"max_attempts": 10, "mode": "standard"})
    )

    uploaded_files = {}

    with ThreadPoolExecutor(max_workers=threads) as executor:
        concurrent_jobs = {
            executor.submit(
                upload_single_file,
                s3_client=s3_client,
                bucket=bucket,
                remote_path=remote_path,
                local_file=item,
                parent_path=parent_path,
            ): item
            for item in files
        }

        for future in as_completed(concurrent_jobs):
            # access returned output as each is returned in any order
            try:
                local_file, remote_id = future.result()
                uploaded_files[local_file] = remote_id
            except Exception as exc:
                # catch any other errors that might get raised
                print(f"\nError: {concurrent_jobs[future]}: {exc}")
                raise exc

    return uploaded_files


def multi_core_upload(
    files, bucket, remote_path, cores, threads, parent_path
) -> list:
    """
    Call the multi_thread_upload on `files` split across n
    logical CPU cores

    Parameters
    ----------
    files : list
        list of local files to upload
    bucket : str
        S3 bucket to upload to
    remote_path : str
        parent directory in bucket to upload to
    cores : int
        maximum number of logical CPU cores to split uploading across
    threads : int
        maximum number of threaded process to open per core
    parent_path : str
        path to parent of sequencing directory, will be removed from
        the file path for uploading to not form part of the remote path

    Returns
    -------
    dict
        mapping of local file to ETag ID of uploaded file
    """
    uploaded_files = {}

    with ProcessPoolExecutor(max_workers=cores) as exe:
        concurrent_jobs = {
            exe.submit(
                multi_thread_upload,
                files=sub_files,
                bucket=bucket,
                remote_path=remote_path,
                threads=threads,
                parent_path=parent_path,
            ): sub_files
            for sub_files in files
        }

        for future in as_completed(concurrent_jobs):
            # access returned output as each is returned in any order
            try:
                uploaded_files = {**uploaded_files, **future.result()}
            except Exception as exc:
                # catch any other errors that might get raised
                print(f"\nError: {concurrent_jobs[future]}: {exc}")
                raise exc

    return uploaded_files
