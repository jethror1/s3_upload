import argparse
import os
from os import listdir
from os.path import isfile, join
from pathlib import Path
import sys
from timeit import default_timer as timer
from uuid import uuid4

sys.path.append(os.path.join(Path(__file__).parent.parent, "s3_upload"))

from s3_upload import upload_single_run
from utils.utils import sizeof_fmt


def parse_args() -> argparse.Namespace:
    """
    Parse cmd line arguments

    Returns
    -------
    argparse.Namespace
        Namespace object of parsed cmd line arguments
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--local_path",
        required=True,
        help="path to sequencing run to benchmark with",
    )
    parser.add_argument(
        "--cores",
        nargs="+",
        required=True,
        help="list of numbers of cores to benchmark with",
    )
    parser.add_argument(
        "--threads",
        nargs="+",
        required=True,
        help="list of numbers of threads to benchmark with",
    )
    parser.add_argument("--bucket", type=str, help="S3 bucket to upload to")
    parser.add_argument(
        "--remote_path", type=str, help="path to upload to in bucket"
    )

    return parser.parse_args()


def cleanup_remote_files(bucket, remote_path) -> None:
    """
    Clean up the uploaded test files from the remote path in the test
    S3 bucket

    Parameters
    ----------
    bucket : str
        bucket where files were uploaded to
    remote_path : str
        path where files were uploaded to
    """
    print(f"Deleting files from {bucket}:{remote_path}")
    bucket = boto3.resource("s3").Bucket(bucket)
    objects = bucket.objects.filter(Prefix=remote_path)
    objects = [{"Key": obj.key} for obj in objects]

    if objects:
        bucket.delete_objects(Delete={"Objects": objects})


def main():
    args = parse_args()

    if not args.remote_path:
        args.remote_path = f"/benchmark_upload_{uuid4().hex}"

    print(f"Uploading benchmarking output to {args.bucket}:{args.remote_path}")

    # map pairs of cores and threads combinations to benchmark with
    cores_to_threads = [(x, y) for x in args.cores for y in args.threads]

    benchmarks = []

    import psutil

    process = psutil.Process(os.getpid())
    print(process.memory_info_ex().rss / 1024 / 1024)
    process.memory_info_ex
    exit()

    for core, thread in cores_to_threads:
        print(f"Beginning benchmarking with {core} cores and {thread} threads")

        upload_args = argparse.Namespace(
            cores=core,
            threads=thread,
            local_path=args.local_path,
            bucket=args.bucket,
            remote_path=args.remote_path,
        )

        start = timer()

        upload_single_run(upload_args)

        end = timer()
        elapsed = end - start

        print(f"Uploaded files in {elapsed}s")

        cleanup_remote_files(bucket=args.bucket, remote_path=args.remote_path)


if __name__ == "__main__":
    main()
