"""Functions for handling uploading into S3"""

from concurrent.futures import (
    ProcessPoolExecutor,
    ThreadPoolExecutor,
    wait,
    as_completed,
)
from os import path

import boto3
from botocore.config import Config
from boto3.s3.transfer import TransferConfig


def authenticate():
    """
    Authenticate with AWS S3 with given credentials
    """
    pass


def upload_single_file(s3_client, bucket, remote_path, local_file):
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

    Returns
    -------
    str
        path and filename of uploaded file
    str
        ETag attribute of the uploaded file
    """
    # remove base directory
    upload_file = local_file.lstrip(".").lstrip("/").replace("/genetics", "")
    upload_file = path.join(remote_path, upload_file)

    # set threshold for splitting across cores to 1GB and to not use
    # multiple threads for single file upload to allow us to control
    # this better from the config
    config = TransferConfig(multipart_threshold=1024**3, use_threads=False)
    s3_client.upload_file(local_file, bucket, upload_file, Config=config)

    # ensure we can access the remote file to log the object ID
    remote_object = s3_client.get_object(Bucket=bucket, Key=upload_file)

    return local_file, remote_object.get("ETag")


def single_core_threaded_upload(files, bucket, remote_path, threads) -> list:
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


def call_by_core(files, cores, threads) -> list:
    """
    Call the single_core_threaded_upload on `files` split across n
    logical CPU cores

    Parameters
    ----------
    files : list
        list of local files to upload
    cores : int
        maximum number of logical CPU cores to split uploading across
    threads : int
        maximum number of threaded process to open per core

    Returns
    -------
    dict
        mapping of local file to ETag ID of uploaded file
    """
    uploaded_files = {}

    with ProcessPoolExecutor(max_workers=cores) as exe:
        concurrent_jobs = {
            exe.submit(
                single_core_threaded_upload,
                files=sub_files,
                threads=threads,
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
