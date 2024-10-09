"""General utility functions"""

from glob import glob
from itertools import zip_longest
import json
from os import path, scandir, stat
from pathlib import Path
import re
from typing import List

from .log import get_logger


log = get_logger("s3 upload")


def check_termination_file_exists(run_dir) -> bool:
    """
    Check if the run has completed sequencing from the presence of
    CopyComplete.txt (for NovaSeqs), or RTAComplete(.txt/.xml) for other
    types of Illumina sequencers.

    Adapted from: https://github.com/eastgenomics/dx-streaming-upload/blob/476b28af980ad62c5f2750cc0b6920b82a287b11/files/incremental_upload.py#L393

    Parameters
    ----------
    run_dir : str
        path to run directory to check

    Returns
    -------
    bool
        True if run is complete else False
    """
    log.debug("Checking for termination file in %s", run_dir)

    if path.exists(path.join(run_dir, "CopyComplete.txt")):
        # NovaSeq run that is complete
        return True
    elif path.exists(path.join(run_dir, "RTAComplete.txt")) or path.exists(
        path.join(run_dir, "RTAComplete.xml")
    ):
        # other type of Illumina sequencer (e.g. MiSeq, NextSeq, HiSeq)
        return True
    else:
        return False


def check_is_sequencing_run_dir(run_dir) -> bool:
    """
    Check if a given directory is a sequencing run from presence of
    RunInfo.xml file

    Parameters
    ----------
    run_dir : str
        path to directory to check

    Returns
    -------
    bool
        True if directory is a sequencing run else False
    """
    log.debug("Checking if directory is a sequencing run: %s", run_dir)
    return path.exists(path.join(run_dir, "RunInfo.xml"))


def get_runs_to_upload(monitor_dirs) -> list:
    """
    Get completed sequencing runs to upload from specified directories
    to monitor

    Parameters
    ----------
    monitor_dirs : list
        list of directories to check for completed sequencing runs

    Returns
    -------
    list
        list of directories that are completed runs
    """
    to_upload = []

    for monitored_dir in monitor_dirs:
        # check each sub directory if it looks like a completed
        # sequencing run
        # TODO - this needs to also check against local log files
        log.info("Checking %s for completed sequencing runs", monitored_dir)

        sub_directories = [
            f.path for f in scandir(monitored_dir) if f.is_dir()
        ]

        log.debug(
            "directories found in %s: %s", monitored_dir, sub_directories
        )

        for sub_dir in sub_directories:
            if check_is_sequencing_run_dir(
                sub_dir
            ) and check_termination_file_exists(sub_dir):
                log.debug("%s is a completed sequencing run", sub_dir)
                to_upload.append(sub_dir)

    return to_upload


def get_sequencing_file_list(seq_dir, exclude_patterns=None) -> list:
    """
    Recursively get list of files and their paths from the given
    directory.

    Files are returned in order of their file size without the given
    root `seq_dir`.

    Parameters
    ----------
    seq_dir : str
        path to search for files
    exclude_patterns : list
        list of regex patterns against which to exclude files, matching
        against the full file path and file name

    Returns
    -------
    list
        sorted list of files by their file size (descending)
    """
    log.info("Getting list of files to upload in %s", seq_dir)
    files = sorted(
        [
            (x, stat(x).st_size)
            for x in glob(f"{seq_dir}/**/*", recursive=True)
            if Path(x).is_file()
        ],
        key=lambda x: x[1],
        reverse=True,
    )

    if exclude_patterns:
        files = [
            x
            for x in files
            if not re.search(r"|".join(exclude_patterns), x[0])
        ]

    total_size = sizeof_fmt(sum(x[1] for x in files))

    log.info(f"{len(files)} files found to upload totalling %s", total_size)

    return [x[0] for x in files]


def split_file_list_by_cores(files, n) -> List[List[str]]:
    """
    Split given list of files sorted by file size into n approximately
    equal total size and length.

    This is a reasonably naive approach to give us n lists of files with
    an equal length and approximately equal split of small to large files,
    allowing us to more evenly split the total amount of data to upload
    between each ProcessPool.

    Parameters
    ----------
    files : list
        sorted list of files
    n : int
        number of sub lists to split file list to

    Returns
    ------
    list
        list of lists of filenames
    """
    files = [files[i : i + n] for i in range(0, len(files), n)]
    files = [[x for x in y if x] for y in zip_longest(*files)]

    return files


def check_upload_state(dir) -> str:
    """
    Checking upload state of run (i.e. complete, partial, not started)

    Parameters
    ----------
    dir : _type_
        _description_

    Returns
    -------
    str
        _description_
    """
    pass


def read_config(config) -> dict:
    """
    Read in the JSON config file

    Parameters
    ----------
    config : str
        filename of config file

    Returns
    -------
    dict
        contents of config file
    """
    log.info("Loading config from %s", config)
    with open(config, "r") as fh:
        return json.load(fh)


def read_upload_state_log(log_file) -> bool:
    """
    Read upload state log to check if run has completed uploading

    Parameters
    ----------
    log_file : str
        path to upload state log for run

    Returns
    -------
    bool
        True if run has completed uploading else False
    """
    log.info("Reading upload state from log file: %s", log_file)

    with open(log_file) as fh:
        log_data = json.loads(fh)

    uploaded = (
        "finished upload" if log_data["completed"] else "incomplete upload"
    )

    log.info("state of run %s: %s", log_data["run_id"], uploaded)

    if not log_data["completed"]:
        log.info(
            "total local files: %s | total uploaded files: %s | total failed"
            " upload: %s | total files to upload %s",
            log_data["total_local_files"],
            log_data["total_uploaded_files"],
            log_data["total_failed_upload"],
            log_data["total_local_files"] - log_data["total_uploaded_files"],
        )

    return log_data["completed"]


def write_upload_state_to_log(
    run_id, run_path, log_file, local_files, uploaded_files, failed_files
) -> dict:
    """
    Write the log file for the run to log the state of what has been uploaded.

    If the uploaded files matches the local files with no failed uploads,
    the run is marked as complete uploaded. This is then used for future
    monitoring to know not to attempt re-upload.

    Parameters
    ----------
    run_id : str
        ID of sequencing run
    run_path : str
        path to run directory being uploaded
    log_file : str
        file to write log to
    local_files : list
        list of local files provided to upload
    uploaded_files : dict
        mapping of uploaded local file path to remote object ID
    failed_files : list
        list of files that failed to upload

    Returns
    -------
    dict
        all log data for the run
    """
    total_local_files = len(local_files)
    total_uploaded_files = len(uploaded_files.keys())
    total_failed_upload = len(failed_files)

    log.info("logging upload state of %s", run_id)
    log.info(
        "total local files: %s | total uploaded files: %s | total failed"
        " upload: %s",
        total_local_files,
        total_uploaded_files,
        total_failed_upload,
    )

    if path.exists(log_file):
        # log file already exists => continuing previous failed upload
        log.debug("log file already exists to update at %s", log_file)

        with open(log_file, "r") as fh:
            log_data = json.load(fh)
    else:
        log_data = {
            "run_id": run_id,
            "run path": run_path,
            "completed": False,
            "total_local_files": total_local_files,
            "total_uploaded_files": 0,
            "total_failed_upload": 0,
            "failed_upload_files": [],
            "uploaded_files": {},
        }

    log_data["total_uploaded_files"] += total_uploaded_files
    log_data["total_failed_upload"] = total_failed_upload
    log_data["failed_upload_files"] = failed_files
    log_data["uploaded_files"] = {
        **log_data["uploaded_files"],
        **uploaded_files,
    }

    if (
        total_failed_upload == 0
        and total_local_files == log_data["total_uploaded_files"]
    ):
        log.info(
            "All local files uploaded and no files failed uploading, run"
            " completed uploading"
        )
        log_data["completed"] = True

    with open(log_file, "w") as fh:
        json.dump(log_data, fh, indent=4)

    return log_data


def verify_args(args) -> None:
    """
    Verify that the provided args are valid

    Parameters
    ----------
    args : argparse.NameSpace
        parsed command line arguments
    """
    # TODO - complete this once I decide on all args to have
    pass


def verify_config(config) -> None:
    """
    Verify that config structure and parameters are valid

    Parameters
    ----------
    config : dict
        contents of config file to check
    """
    log.debug(
        "Verifying contents of config are valid, contents parsed: %s", config
    )
    errors = []

    if not isinstance(config.get("max_cores", 0), int):
        errors.append("max_cores must be an integer")

    if not isinstance(config.get("max_threads", 0), int):
        errors.append("max_threads must be an integer")

    if not config.get("log_dir"):
        errors.append("required parameter log_dir not defined")

    if not config.get("monitor"):
        errors.append("required parameter monitor not defined")

    for idx, monitor in enumerate(config.get("monitor", "")):
        for key, expected_type in {
            "monitored_directories": list,
            "bucket": str,
            "remote_path": str,
        }.items():
            if not monitor.get(key):
                errors.append(
                    f"required parameter {key} missing from monitor section"
                    f" {idx}"
                )
            else:
                if not isinstance(monitor.get(key), expected_type):
                    errors.append(
                        f"{key} not of expected type from monitor section "
                        f"{idx}. Expected: {expected_type} | Found "
                        f"{type(monitor.get(key))}"
                    )

    if errors:
        error_message = (
            f"{len(errors)} errors found in config:{chr(10)}{chr(9)}"
            f"{f'{chr(10)}{chr(9)}'.join(errors)}"
        )
        log.error(error_message)
        raise RuntimeError(error_message)
    else:
        log.debug("Config valid")


def sizeof_fmt(num) -> str:
    """
    Function to turn bytes to human readable file size format.

    Taken from https://stackoverflow.com/questions/1094841/get-human-readable-version-of-file-size

    Parameters
    ----------
    num : int
        total size in bytes

    Returns
    -------
    str
        file size in human-readable format
    """
    for unit in ["", "K", "M", "G", "T", "P", "E", "Z"]:
        if abs(num) < 1024.0:
            return f"{num:3.2f}{unit}B"
        num /= 1024.0
    return f"{num:.2f}YiB"
