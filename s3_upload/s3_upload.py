import argparse
from os import cpu_count, path
from pathlib import Path


from utils.upload import (
    check_aws_access,
    check_bucket_exists,
    multi_core_upload,
)
from utils.utils import get_sequencing_file_list


def parse_args() -> argparse.Namespace:
    """
    Parse cmd line arguments

    Returns
    -------
    argparse.Namespace
        parsed arguments
    """
    parser = argparse.ArgumentParser()

    subparsers = parser.add_subparsers(
        help="upload mode to run", dest="mode", required=True
    )

    monitor_parser = subparsers.add_parser(
        "monitor",
        help=(
            "mode to be run on a schedule to monitor directories for newly"
            " completed sequencing runs"
        ),
    )

    monitor_parser.add_argument(
        "--upload_config",
        help="path config file for monitoring directories to upload",
    )

    upload_parser = subparsers.add_parser(
        "upload",
        help="mode to upload a single directory to given location in S3",
    )

    upload_parser.add_argument(
        "--local_path", help="path to directory to upload"
    )
    upload_parser.add_argument(
        "--bucket",
        type=str,
        help="S3 bucket to upload to",
    )
    upload_parser.add_argument(
        "--remote_path",
        default="/",
        help="remote path in bucket to upload sequencing dir to",
    )
    upload_parser.add_argument(
        "--cores",
        nargs=1,
        required=False,
        default=cpu_count(),
        help=(
            "number of CPU cores to split total files to upload across, will "
            "default to using all available"
        ),
    )
    upload_parser.add_argument(
        "--threads",
        nargs=1,
        type=int,
        default=32,
        help="number of threads to open per core to split uploading across",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    check_aws_access()
    check_bucket_exists(args.bucket)

    if args.mode == "upload":
        files = get_sequencing_file_list(args.local_path)

        # pass through the parent of the specified directory to upload
        # to ensure we upload into the actual run directory
        parent_path = Path(args.local_path).parent

        multi_core_upload(
            files=files,
            bucket=args.bucket,
            remote_path=args.remote_path,
            cores=args.cores,
            threads=args.threads,
            parent_path=parent_path,
        )

    # check connectivity to AWS
    # check given S3 bucket exists

    # if running in monitor:
    #   - read from upload log
    #   - find directories to upload
    #   -

    # per dir to upload
    #   -


if __name__ == "__main__":
    main()
