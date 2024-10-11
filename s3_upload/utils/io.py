import json
from os import path

from .log import get_logger

log = get_logger("s3 upload")


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


def read_upload_state_log(log_file) -> dict:
    """
    Read upload state log to check if run has completed uploading

    Parameters
    ----------
    log_file : str
        path to upload state log for run

    Returns
    -------
    dict
        contents of log file
    """
    log.debug("Reading upload state from log file: %s", log_file)

    with open(log_file) as fh:
        log_data = json.loads(fh)

    uploaded = (
        "finished upload" if log_data["completed"] else "incomplete upload"
    )

    log.debug("state of run %s: %s", log_data["run_id"], uploaded)

    if not log_data["completed"]:
        log.debug(
            "total local files: %s | total uploaded files: %s | total failed"
            " upload: %s | total files to upload %s",
            log_data["total_local_files"],
            log_data["total_uploaded_files"],
            log_data["total_failed_upload"],
            log_data["total_local_files"] - log_data["total_uploaded_files"],
        )

    return log_data


def write_upload_state_to_log(
    run_id, run_path, log_file, local_files, uploaded_files, failed_files
) -> dict:
    """
    Write the log file for the run to log the state of what has been uploaded.

    If the uploaded files matches the local files with no failed uploads,
    the run is marked as complete uploaded. This is then used for future
    monitoring to know not to attempt re-upload.

    Log file will have the following structure:

    {
        "run_id": run_id,           -> ID of sequencing run
        "run path": run_path,       -> full local path to the run dir
        "completed": False,         -> if all files have uploaded
        "total_local_files": ,      -> total count of local files to upload
        "total_uploaded_files": 0,  -> total files already uploaded
        "total_failed_upload": 0,   -> total files failed to upload
        "failed_upload_files": [],  -> list of files previously failed upload
        "uploaded_files": {},       -> mapping of uploaded files to object ID
    }

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
