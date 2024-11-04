import json
from os import makedirs, path
from pathlib import Path

from e2e import TEST_DATA_DIR


def create_files(run_dir, *files):
    """
    Create the given files and intermediate paths provided from the
    given test run directory

    Parameters
    ----------
    run_dir : str
        path to test run directory

    files : list
        files and relative paths to create
    """
    for file_path in files:
        full_path = path.join(run_dir, file_path)
        parent_dir = Path(full_path).parent

        makedirs(parent_dir, exist_ok=True)
        open(full_path, encoding="utf-8", mode="a").close()


def read_upload_log() -> dict:
    """
    Read in the run upload log file

    Returns
    -------
    dict
        contents of run upload log file
    """
    with open(
        path.join(TEST_DATA_DIR, "logs/uploads/run_1.upload.log.json"),
        encoding="utf8",
        mode="r",
    ) as fh:
        return json.load(fh)


def read_stdout_stderr_log() -> list:
    """
    Read the stdout / stderr log file s3_upload.log

    Returns
    -------
    list
        contents of log file
    """
    with open(
        path.join(TEST_DATA_DIR, "logs/s3_upload.log"),
        encoding="utf8",
        mode="r",
    ) as fh:
        return fh.read().splitlines()
