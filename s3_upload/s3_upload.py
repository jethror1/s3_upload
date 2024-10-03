import argparse
from os import cpu_count


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
        nargs=1,
        help="path config file for monitoring directories to upload",
    )

    upload_parser = subparsers.add_parser(
        "upload",
        help="mode to upload a single directory to given location in S3",
    )

    upload_parser.add_argument(
        "--local_path", type=str, nargs=1, help="path to directory to upload"
    )
    upload_parser.add_argument(
        "--upload path",
        type=str,
        nargs=1,
        help=(
            "S3 bucket and path to upload to, should be in the format: "
            "bucket://path/to/upload"
        ),
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
