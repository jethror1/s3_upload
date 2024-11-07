"""
Wrapper script for calling the uploader with given set of cores / threads
to benchmark performance with each pair iteratively to determine the
optimal to set for uploading
"""

import argparse
from collections import defaultdict
from datetime import datetime
import os
from pathlib import Path
from statistics import mean
import subprocess
import sys
from time import gmtime, strftime
from timeit import default_timer as timer
from typing import Tuple

import boto3

AWS_DEFAULT_PROFILE = os.environ.get("AWS_DEFAULT_PROFILE")


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
        "--repeats",
        type=int,
        default=1,
        help=(
            "number of times to run the upload for the given core/thread pair,"
            " if greater than one then the mean elapsed time and max resident"
            " set size will be calculated from all uploads"
        ),
    )
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


def get_peak_memory_usage() -> str:
    """
    Read memory usage output from memory-profiler to get peak memory usage.

    File is formatted with one line per sample containing 'MEM 10.00 1.00',
    where 10.00 is memory usage at that time point and 1.00 being the time
    since execution

    Returns
    -------
    float
        peak memory usage of the process
    """
    with open("benchmark.out", mode="r", encoding="utf8") as fh:
        contents = fh.read().splitlines()

    os.remove("benchmark.out")

    try:
        return round(max([float(x.split()[1]) for x in contents[1:]]), 2)
    except Exception as err:
        print(f"Error in parsing output from memory-profiler:\n{err}")
        print("Returning zero and continuing")
        return 0


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
    print(f"Deleting uploaded files from {bucket}:{remote_path}")
    bucket = (
        boto3.Session(profile_name=AWS_DEFAULT_PROFILE)
        .resource("s3")
        .Bucket(bucket)
    )

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
    proc = subprocess.run(
        command, shell=True, check=False, capture_output=True
    )

    stdout = proc.stdout.decode()
    stderr = proc.stderr.decode()

    # running with memory-profile swallows the return code and always exits
    # with zero, therefore check if an error was raised from stderr
    if proc.returncode != 0 or "Error" in stderr:
        print(f"Error in calling {command}")
        print(stdout)
        print(stderr)
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

    return f"{proc.stdout.decode().split()[0]}B"


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
) -> Tuple[int, int]:
    """
    Call the s3_upload script with the given parameters and capture both
    the elapsed time and maximum resident set size (i.e. peak memory usage).

    We are going to use subprocess.run to call the script instead of
    directly importing, this is so we can run memory-profiler to get
    the maximum memory used across all child processes.

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
    int
        elapsed time in seconds
    int
        maximum resident set size (in mb)
    """
    script_path = os.path.join(
        Path(__file__).absolute().parent.parent, "s3_upload/s3_upload.py"
    )

    command = (
        "mprof run --include-children -o benchmark.out python3"
        f" {script_path} upload --local_path {local_path} --bucket"
        f" {bucket} --remote_path {remote_path} --cores {cores} --threads"
        f" {threads} --skip_check"
    )

    print(f"Calling uploader with:\n\t{command}")

    start = timer()
    proc = call_command(command)
    end = timer()

    elapsed_time = round(end - start, 2)
    max_resident_set_size = get_peak_memory_usage()

    print(f"Uploading completed in {elapsed_time}s")

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
            f" {args.bucket} | remote_path: {args.remote_path} | repeats:"
            f" {args.repeats}"
        ),
        f"# Total files to benchmark with: {run_files} ({run_size})",
        "cores\tthreads\telapsed time (h:m:s)\tmaximum resident set size (MB)",
    ]

    total_metrics = defaultdict(lambda: defaultdict(list))
    benchmarks_run = 0

    while args.repeats > benchmarks_run:
        benchmarks_run += 1
        repeat_start = timer()

        print(f"\nRunning benchmarking repeat {benchmarks_run}/{args.repeats}")

        for core, thread in cores_to_threads:
            print(
                f"\nBeginning benchmarking with {core} cores and"
                f" {thread} threads at"
                f" {datetime.now().strftime('%d-%m-%y %H:%M')}"
            )

            elapsed_time, max_set_size = run_benchmark(
                local_path=args.local_path,
                bucket=args.bucket,
                remote_path=args.remote_path,
                cores=core,
                threads=thread,
            )

            total_metrics[(core, thread)]["elapsed_time"].append(elapsed_time)
            total_metrics[(core, thread)]["max_set_size"].append(max_set_size)

            cleanup_remote_files(
                bucket=args.bucket,
                remote_path=args.remote_path,
            )

        repeat_end = timer()
        print(
            f"Completed benchmarking repeat {benchmarks_run}/{args.repeats} in"
            f" {strftime('%H:%M:%S', gmtime(repeat_end - repeat_start))}"
        )

    for compute, metrics in total_metrics.items():
        elapsed_time = strftime(
            "%H:%M:%S", gmtime(round(mean(metrics["elapsed_time"])))
        )
        max_set_size = round(mean(metrics["max_set_size"]), 2)

        benchmarks.append(
            f"{compute[0]}\t{compute[1]}\t{elapsed_time}\t{max_set_size}"
        )

    outfile = f"s3_upload_benchmark_{now.replace(':', '_')}.tsv"

    with open(outfile, mode="w", encoding="utf8") as fh:
        fh.write("\n".join(benchmarks + ["\n"]))

    print(f"\nBenchmarking complete!\n\nOutput written to {outfile}\n")
    print("\n".join(benchmarks))


if __name__ == "__main__":
    main()
