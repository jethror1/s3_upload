import os
from pathlib import Path
from shutil import rmtree
import unittest

import pytest

from tests import TEST_DATA_DIR
from s3_upload.utils import log


class TestCheckWritePermissionToLogDir(unittest.TestCase):
    def test_valid_existing_path_with_permission_does_not_raise_error(self):
        log.check_write_permission_to_log_dir(TEST_DATA_DIR)

    def test_missing_dir_with_valid_parent_dir_does_not_raise_error(self):
        test_log_dir = os.path.join(
            TEST_DATA_DIR, "sub_directory_that_does_not_exist_yet"
        )

        Path(test_log_dir).mkdir(parents=True, exist_ok=True)

        with self.subTest():
            log.check_write_permission_to_log_dir(test_log_dir)

        rmtree(test_log_dir)

    def test_dir_with_no_write_permission_raises_permission_error(self):
        test_log_dir = "/"

        expected_error = (
            f"Path to provided log directory {test_log_dir} does not appear to"
            " have write permission for current user"
        )

        with pytest.raises(PermissionError, match=expected_error):
            log.check_write_permission_to_log_dir(test_log_dir)

    def test_missing_dir_with_parent_dir_no_write_permission_raises_error(
        self,
    ):
        test_log_dir = "/sub_directory_that_does_not_exist_yet"

        expected_error = (
            "Path to provided log directory / does not appear to"
            " have write permission for current user"
        )

        with pytest.raises(PermissionError, match=expected_error):
            log.check_write_permission_to_log_dir(test_log_dir)
