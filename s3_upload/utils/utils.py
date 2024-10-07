"""General utility functions"""

from glob import glob
from itertools import zip_longest
import json
from os import path, stat
from pathlib import Path
import re
from typing import List

from utils.log import get_logger


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
    return path.exists(path.join(run_dir, "RunInfo.xml"))


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


def parse_config(config) -> dict:
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
    with open(config, "r") as fh:
        return json.load(fh)
