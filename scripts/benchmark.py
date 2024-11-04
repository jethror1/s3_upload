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


def main():
    args = parse_args()

    if not args.remote_path:
        args.remote_path = f"/benchmark_upload_{uuid4().hex}"

    # dir_size = sizeof_fmt(os.path.getsize(Path(args.local_path).absolute()))

    # total_files = len(
    #     [
    #         f
    #         for f in listdir(args.local_path)
    #         if isfile(join(args.local_path, f))
    #     ]
    # )

    # print(total_files)
    # print(dir_size)
    # exit()

    print(f"Uploading benchmarking output to {args.bucket}:{args.remote_path}")

    # map pairs of cores and threads combinations to benchmark with
    cores_to_threads = [(x, y) for x in args.cores for y in args.threads]

    for core, thread in cores_to_threads:
        print(f"Beginning benchmarking with {core} cores and {thread} threads")

        upload_args = argparse.Namespace(
            cores=core,
            threads=thread,
            local_path=args.local_path,
            bucket=args.bucket,
            remote_path=args.remote_path,
        )
        print(upload_args)


if __name__ == "__main__":
    main()
