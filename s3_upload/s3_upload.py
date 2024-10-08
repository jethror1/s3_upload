import argparse
from os import cpu_count
from pathlib import Path

from utils.upload import (
    check_aws_access,
    check_buckets_exist,
    multi_core_upload,
)
from utils.utils import (
    check_is_sequencing_run_dir,
    check_termination_file_exists,
    get_runs_to_upload,
    get_sequencing_file_list,
    read_config,
    split_file_list_by_cores,
    verify_args,
    verify_config,
)
from utils.log import get_logger


log = get_logger("s3 upload")


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
        "--config",
        required=True,
        help="config file for monitoring directories to upload",
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
        required=False,
        default=cpu_count(),
        help=(
            "number of CPU cores to split total files to upload across, will "
            "default to using all available"
        ),
    )
    upload_parser.add_argument(
        "--threads",
        type=int,
        default=8,
        help=(
            "number of threads to open per core to split uploading across "
            "(default: 8)"
        ),
    )

    return parser.parse_args()


def upload_single_run(args):
    """
    Upload provided single run directory into AWS S3

    Parameters
    ----------
    args : argparse.NameSpace
        parsed command line arguments
    """
    check_aws_access()
    check_buckets_exist(args.bucket)

    if not check_is_sequencing_run_dir(
        args.local_path
    ) or not check_termination_file_exists(args.local_path):
        log.error(
            f"Provided directory: {args.local_path} does not appear to be"
            " a complete sequencing run. Please check the provided path"
            " and try again."
        )
        exit()

    files = get_sequencing_file_list(args.local_path)
    files = split_file_list_by_cores(files=files, n=args.cores)

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


def monitor_directories_for_upload(config):
    """
    Monitor specified directories for complete sequencing runs to upload

    Parameters
    ----------
    config : dict
        contents of config file
    """
    log.info("Beginning monitoring directories for runs to upload")

    check_aws_access()
    check_buckets_exist(set([x["bucket"] for x in config["monitor"]]))

    cores = config.get("max_cores", cpu_count)
    threads = config.get("max_threads", 4)

    to_upload = []

    # find all the runs to upload in the specified monitored directories
    for monitor_dir_config in config["monitor"]:
        completed_runs = get_runs_to_upload(monitor_dir_config)

        to_upload.extend(
            [
                {
                    "run_dir": run_dir,
                    "parent_path": Path(run_dir).parent,
                    "bucket": monitor_dir_config["bucket"],
                    "remote_path": monitor_dir_config["remote_path"],
                }
                for run_dir in completed_runs
            ]
        )

    log.info(
        f"Found {len(to_upload)} sequencing runs to upload:"
        f" {', '.join([Path(x['run_dir']).name for x in to_upload])}"
    )

    for run_config in to_upload:
        # begin uploading of each sequencing run
        files = get_sequencing_file_list(run_config["run_dir"])
        files = split_file_list_by_cores(files=files, n=cores)

        multi_core_upload(
            files=files,
            bucket=run_config["bucket"],
            remote_path=run_config["remote_path"],
            cores=cores,
            threads=threads,
            parent_path=run_config["parent_path"],
        )


def main() -> None:
    args = parse_args()

    # TODO -  add function to check log dir readable

    if args.mode == "upload":
        upload_single_run(args)
    else:
        config = read_config(config=args.config)
        verify_config(config=config)

        monitor_directories_for_upload(config)


if __name__ == "__main__":
    main()
