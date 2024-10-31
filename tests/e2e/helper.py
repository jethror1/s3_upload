from os import makedirs, path
from pathlib import Path


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
