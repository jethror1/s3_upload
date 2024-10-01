from concurrent.futures import (
    ProcessPoolExecutor,
    ThreadPoolExecutor,
    wait,
    as_completed,
)
from glob import glob
import os
import pathlib
import sys

import boto3


def single_core_threaded_upload(files, threads):
    """Upload files with single core but multiple threads"""
    with ThreadPoolExecutor(max_workers=threads) as executor:
        concurrent_jobs = {
            executor.submit(upload, item): item for item in files
        }

        for future in as_completed(concurrent_jobs):
            # access returned output as each is returned in any order
            try:
                future.result()
            except Exception as exc:
                # catch any other errors that might get raised during querying
                print(
                    f"\nError getting data for {concurrent_jobs[future]}: {exc}"
                )
                raise exc


def multiple_core_threaded_upload(files, cores, threads):
    """Split uploading of given files across n CPU cores"""

    # split our list of files equally across cores
    # TODO - think about splitting files by size between cores
    # so we have a mix of large and small files split across cores
    files = [files[i : i + cores] for i in range(0, len(files), cores)]

    with ProcessPoolExecutor(max_workers=cores) as exe:
        futures = [
            exe.submit(
                single_core_threaded_upload, threads=threads, files=sub_files
            )
            for sub_files in files
        ]

        wait(futures)


def upload(local_file):
    """Upload single file to bucket"""
    s3_client = boto3.client("s3")

    upload_file = local_file.lstrip(".").lstrip("/").replace("/genetics", "")

    s3_client.upload_file(local_file, "jethro-s3-test-v2", upload_file)


if __name__ == "__main__":
    files = [
        x
        for x in glob(f"{sys.argv[1]}/**/*", recursive=True)
        if pathlib.Path(x).is_file()
    ]

    smol_files = [x for x in files if os.path.getsize(x) < 8388608]
    big_files = [x for x in files if os.path.getsize(x) >= 8388608]

    # single_core_threaded_upload(files)

    multiple_core_threaded_upload(files=files, cores=4, threads=8)
    # single_core_threaded_upload(big_files, threads=8)
