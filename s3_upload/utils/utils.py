"""General utility functions"""

from os import path


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


def get_sequencing_file_list(dir, exclude_patterns) -> list:
    """
    Recursively get list of files and their paths from the given directory

    Parameters
    ----------
    dir : _type_
        _description_
    exclude_patterns : _type_
        _description_

    Returns
    -------
    list
        _description_
    """
    pass


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
