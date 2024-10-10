import argparse
from os import cpu_count, path
from pathlib import Path
import sys

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
    filter_uploaded_files,
    read_config,
    write_upload_state_to_log,
    split_file_list_by_cores,
    verify_args,
    verify_config,
)
from utils.log import get_logger, set_file_handler


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
            "Provided directory: %s does not appear to be a complete "
            "sequencing run. Please check the provided path and try again.",
            args.local_path,
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
    log_dir = config.get("log_dir", "/var/log/s3_upload")

    to_upload = []
    partially_uploaded = []

    # find all the runs to upload in the specified monitored directories
    # and build a dict per run with config of where to upload
    for monitor_dir_config in config["monitor"]:
        completed_runs, partially_uploaded = get_runs_to_upload(
            monitor_dir_config["monitored_directories"], log_dir=log_dir
        )

        for run_dir in completed_runs:
            to_upload.append(
                {
                    "run_dir": run_dir,
                    "run_id": Path(run_dir).name,
                    "parent_path": Path(run_dir).parent,
                    "bucket": monitor_dir_config["bucket"],
                    "remote_path": monitor_dir_config["remote_path"],
                }
            )

        for partial_run, uploaded_files in partially_uploaded.items():
            partially_uploaded.append(
                {
                    "run_dir": partial_run,
                    "run_id": Path(partial_run).name,
                    "parent_path": Path(partial_run).parent,
                    "bucket": monitor_dir_config["bucket"],
                    "remote_path": monitor_dir_config["remote_path"],
                    "uploaded_files": uploaded_files,
                }
            )

    if not to_upload and not partially_uploaded:
        log.info("No sequencing runs requiring upload found. Exiting now.")
        sys.exit()

    log.info(
        "Found %s new sequencing runs to upload: %s",
        len(to_upload),
        ", ".join([x["run_id"] for x in to_upload]),
    )
    if partially_uploaded:
        log.info(
            "Found %s partially uploaded runs to continue uploading: %s",
            len(partially_uploaded),
            ", ".join([x["run_id"] for x in partially_uploaded]),
        )

        # preferentially upload partial runs first
        to_upload = partially_uploaded + to_upload

    for run_config in to_upload:
        # begin uploading of each sequencing run
        all_run_files = get_sequencing_file_list(run_config["run_dir"])
        files_to_upload = all_run_files.copy()

        if run_config.get("uploaded_files"):
            # files we stored from a previous partial upload to not reupload
            files_to_upload = filter_uploaded_files(
                local_files=files_to_upload,
                uploaded_files=run_config.get("uploaded_files"),
            )

        files_to_upload = split_file_list_by_cores(
            files=files_to_upload, n=cores
        )

        # call the actual upload, any errors being raised that result in
        # files not being uploaded should be returned and will result in
        # upload state log storing the run as not complete, allowing for
        # retries on uploading
        uploaded_files, failed_upload = multi_core_upload(
            files=files_to_upload,
            bucket=run_config["bucket"],
            remote_path=run_config["remote_path"],
            cores=cores,
            threads=threads,
            parent_path=run_config["parent_path"],
        )

        # set output logs to go into subdirectory with stdout/stderr log
        run_log_file = path.join(
            log_dir, f"/uploads/{run_config['run_id']}.upload.log.json"
        )

        write_upload_state_to_log(
            run_id=run_config["run_id"],
            run_path=run_config["run_dir"],
            log_file=run_log_file,
            local_files=all_run_files,
            uploaded_files=uploaded_files,
            failed_files=failed_upload,
        )


def main() -> None:
    args = parse_args()

    if args.mode == "upload":
        upload_single_run(args)
    else:
        config = read_config(config=args.config)
        verify_config(config=config)

        set_file_handler(log, config.get("log_dir", "/var/log/s3_upload"))

        monitor_directories_for_upload(config)


if __name__ == "__main__":
    main()
