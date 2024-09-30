"""Functions for handling uploading into S3"""

from concurrent.futures import (
    ProcessPoolExecutor,
    ThreadPoolExecutor,
    wait,
    as_completed,
)

import boto3


def upload_single_file(local_file):
    """
    Uploads single file into S3 storage bucket

    Parameters
    ----------
    local_file : _type_
        _description_
    """
    pass


def single_core_threaded_upload(files, threads) -> list:
    """
    Uploads the given set of `files` to S3 on a single CPU core using
    maximum of n threads

    Parameters
    ----------
    files : _type_
        _description_
    threads : _type_
        _description_

    Returns
    -------
    list
        _description_
    """
    pass


def call_by_core(files, cores, threads) -> list:
    """
    Call the single_core_threaded_upload on `files` split across n
    logical CPU cores

    Parameters
    ----------
    files : _type_
        _description_
    cores : _type_
        _description_
    threads : _type_
        _description_

    Returns
    -------
    list
        _description_
    """
