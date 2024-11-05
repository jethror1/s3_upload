"""
Wrapper script for calling the uploader with given set of cores / threads
to benchmark performance with each pair iteratively to determine the
optimal to set for uploading
"""

import argparse
from datetime import datetime
import os
from pathlib import Path
import subprocess
import sys
from typing import Tuple

import boto3


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
        type=int,
        required=True,
        help="list of numbers of cores to benchmark with",
    )
    parser.add_argument(
        "--threads",
        nargs="+",
        type=int,
        required=True,
        help="list of numbers of threads to benchmark with",
    )
    parser.add_argument("--bucket", type=str, help="S3 bucket to upload to")
    parser.add_argument(
        "--remote_path", type=str, help="path to upload to in bucket"
    )

    return parser.parse_args()


def parse_time_output(stderr):
    """
    Parse the required fields from the stderr output of /usr/bin/time.

    This contains key metrics such as CPU usage and max resident set
    size (aka peak memory usage)

    Parameters
    ----------
    stderr : list
        stderr output from running the upload

    Returns
    -------
    str
        elapsed time (formatted as h:mm:ss or m:ss)
    int
        maximum resident set size (in kb)
    """
    elapsed_time = [x for x in stderr if x.startswith("Elapsed")][0].split()[
        -1
    ]
    max_resident = [
        x for x in stderr if x.startswith("Maximum resident set size")
    ][0].split()[-1]

    return elapsed_time, max_resident


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

    # delete_objects has a max request size of 1000 keys
    objects = [objects[i : i + 1000] for i in range(0, len(objects), 1000)]

    for sub_objects in objects:
        bucket.delete_objects(Delete={"Objects": sub_objects})


def call_command(command) -> subprocess.CompletedProcess:
    """
    Call the given command with subprocess run

    Parameters
    ----------
    command : str
        command to call

    Returns
    -------
    subprocess.CompletedProcess
        completed process object

    Raises
    ------
    SystemExit
        Raised when provided command does not return a zero exit code
    """
    proc = subprocess.run(command, shell=True, capture_output=True)

    if proc.returncode != 0:
        print(f"Error in calling {command}")
        print(proc.stderr.decode())
        sys.exit(proc.returncode)

    return proc


def check_local_path_size(local_path) -> str:
    """
    Check the size of the provided directory to benchmark with using du

    Parameters
    ----------
    local_path : str
        path to check size of

    Returns
    -------
    str
        size of directory
    """
    proc = call_command(f"du -sh {local_path}")

    return proc.stdout.decode().split()[0]


def check_total_files(local_path) -> int:
    """
    Check the total number of files in the provided directory for uploading

    Parameters
    ----------
    local_path : str
        path to check size of

    Returns
    -------
    int
        total number of files found
    """
    proc = call_command(f"find {local_path} -type f | wc -l")

    return proc.stdout.decode().strip()


def run_benchmark(
    local_path, bucket, remote_path, cores, threads
) -> Tuple[str, int]:
    """
    Call the s3_upload script with the given parameters and capture both
    the elapsed time and maximum resident set size (i.e. peak memory usage).

    We are going to use subprocess.run to call the script instead of
    directly importing, this is so we can use GNU time to measure the
    maximum resident set size since memory profiling in Python where
    child processes are split across cores is a headache.

    Parameters
    ----------
    local_path : str
        path to check size of
    bucket : str
        S3 bucket to upload to
    remote_path : str
        path in bucket to upload to
    cores : int
        list of number of cores to provide to use
    threads : int
        list of number of threads to provide to use

    Returns
    -------
    str
        elapsed time (formatted as h:mm:ss or m:ss)
    int
        maximum resident set size (in mb)
    """
    script_path = os.path.join(
        Path(__file__).absolute().parent.parent, "s3_upload/s3_upload.py"
    )

    command = (
        f"/usr/bin/time -v python3 {script_path} upload --local_path"
        f" {local_path} --bucket {bucket} --remote_path {remote_path} --cores"
        f" {cores} --threads {threads} --skip_check"
    )

    print(f"Calling uploader with:\n\t{command}")

    proc = call_command(command)
    stderr = proc.stderr.decode().replace("\t", "").splitlines()
    elapsed_time, max_resident_set_size = parse_time_output(stderr)
    max_resident_set_size = max_resident_set_size / 1024

    print(f"Uploading completed in {elapsed_time}")

    return elapsed_time, max_resident_set_size


def main():
    args = parse_args()

    assert (
        max(args.cores) <= os.cpu_count()
    ), "maximum specified number of cores exceeds available cores"

    now = datetime.now().strftime("%d-%m-%y_%H:%M")

    if not args.remote_path:
        args.remote_path = f"/benchmark_upload_{now}"

    # map pairs of cores and threads combinations to benchmark with
    cores_to_threads = [(x, y) for x in args.cores for y in args.threads]

    run_size = check_local_path_size(args.local_path)
    run_files = check_total_files(args.local_path)

    print(
        f"\n{run_files} files ({run_size}) to benchmark uploading with from"
        f" {args.local_path}"
    )

    print(f"\nUpload location set to {args.bucket}:{args.remote_path}")

    benchmarks = [
        f"# Benchmarking initiated at {now}",
        (
            f"# Provided arguments - cores: {args.cores} | threads:"
            f" {args.threads} | local_path: {args.local_path} | bucket:"
            f" {args.bucket} | remote_path: {args.remote_path}"
        ),
        f"# Total files to benchmark with: {run_files} ({run_size})",
        "cores\tthreads\telapsed time\tmaximum resident set size",
    ]

    for core, thread in cores_to_threads:
        print(
            f"\nBeginning benchmarking with {core} cores and {thread} threads"
        )

        elapsed_time, max_set_size = run_benchmark(
            local_path=args.local_path,
            bucket=args.bucket,
            remote_path=args.remote_path,
            cores=core,
            threads=thread,
        )

        benchmarks.append(f"{core}\t{thread}\t{elapsed_time}\t{max_set_size}")

        cleanup_remote_files(bucket=args.bucket, remote_path=args.remote_path)

    outfile = f"s3_upload_benchmark_{now}.tsv"

    with open(outfile, mode="w", encoding="utf8") as fh:
        fh.write("\n".join(benchmarks + ["\n"]))

    print(f"\nBenchmarking complete!\n\nOutput written to {outfile}\n")
    print("\n".join(benchmarks))


if __name__ == "__main__":
    main()
