import json
import os
from pathlib import Path
import re
from shutil import rmtree
import unittest
from unittest.mock import patch

import pytest

from tests import TEST_DATA_DIR
from s3_upload.utils import io


@patch("s3_upload.utils.io.listdir")
class TestReadSamplesheet(unittest.TestCase):
    def test_no_samplesheet_returns_none(self, mock_dir):
        contents = io.read_samplesheet_from_run_directory(TEST_DATA_DIR)

        self.assertEqual(contents, None)

    @patch("s3_upload.utils.io.Path")
    def test_samplesheet_regex_finds_expected_files(self, mock_path, mock_dir):
        samplesheets = [
            "SAMPLESHEET.CSV",
            "SampleSheet.csv",
            "Samplesheet.csv",
            "samplesheet.csv",
            "experiment_1_samplesheet.csv",
            "experiment_2_SampleSheet.csv",
            "experiment_3-samplesheet_attempt_1.csv",
        ]

        for samplesheet in samplesheets:
            # mock finding single samplesheet for each name in given dir
            mock_dir.return_value = [samplesheet]
            mock_path.return_value.read_text.return_value = "foo\nbar"

            with self.subTest(f"checking regex with {samplesheet}"):
                contents = io.read_samplesheet_from_run_directory(
                    TEST_DATA_DIR
                )

                self.assertEqual(contents, ["foo", "bar"])

    def test_not_samplesheets_do_not_get_selected_by_regex(self, mock_dir):
        not_samplesheets = [
            "my_file.csv",
            "SampleSheet.txt",
            "samplesheet.tsv",
            "Samplesheet.xlsx",
            "samplesheet",
            "sample_1.csv",
        ]

        for not_a_samplesheet in not_samplesheets:
            # mock finding single samplesheet for each name in given dir
            mock_dir.return_value = [not_a_samplesheet]

            with self.subTest(f"checking regex with {not_a_samplesheet}"):
                contents = io.read_samplesheet_from_run_directory(
                    TEST_DATA_DIR
                )

                self.assertEqual(contents, None)

    @patch("s3_upload.utils.io.Path")
    def test_two_samplesheets_with_same_contents_returns_contents(
        self, mock_path, mock_dir
    ):
        mock_dir.return_value = ["samplesheet1.csv", "samplesheet2.csv"]

        mock_path.return_value.read_text.side_effect = [
            "foo\nbar",
            "foo\nbar",
        ]
        contents = io.read_samplesheet_from_run_directory(TEST_DATA_DIR)

        self.assertEqual(contents, ["foo", "bar"])

    @patch("s3_upload.utils.io.Path")
    def test_two_samplesheets_with_different_contents_returns_none(
        self, mock_path, mock_dir
    ):
        mock_dir.return_value = ["samplesheet1.csv", "samplesheet2.csv"]

        mock_path.return_value.read_text.side_effect = [
            "foo\nbar",
            "baz\nblarg",
        ]
        contents = io.read_samplesheet_from_run_directory(TEST_DATA_DIR)

        self.assertEqual(contents, None)


@patch("s3_upload.utils.io.path.exists")
class TestWriteUploadStateToLog(unittest.TestCase):
    def tearDown(self):
        os.remove(os.path.join(TEST_DATA_DIR, "test_run.upload.log.json"))

    def test_when_no_file_exists_for_fully_uploaded_run(self, mock_exists):
        mock_exists.return_value = False
        log_file = os.path.join(TEST_DATA_DIR, "test_run.upload.log.json")

        io.write_upload_state_to_log(
            run_id="test_run",
            run_path="/some/path/seq1/test_run",
            log_file=log_file,
            local_files=["file1.txt", "file2.txt", "file3.txt"],
            uploaded_files={
                "file1.txt": "abc123",
                "file2.txt": "def456",
                "file3.txt": "ghi789",
            },
            failed_files=[],
        )

        with open(log_file, "r") as fh:
            written_log_contents = json.load(fh)

        expected_log_contents = {
            "run_id": "test_run",
            "run path": "/some/path/seq1/test_run",
            "completed": True,
            "total_local_files": 3,
            "total_uploaded_files": 3,
            "total_failed_upload": 0,
            "failed_upload_files": [],
            "uploaded_files": {
                "file1.txt": "abc123",
                "file2.txt": "def456",
                "file3.txt": "ghi789",
            },
        }

        self.assertDictEqual(written_log_contents, expected_log_contents)

    def test_when_no_file_exists_for_partially_uploaded_run(self, mock_exists):
        mock_exists.return_value = False
        log_file = os.path.join(TEST_DATA_DIR, "test_run.upload.log.json")

        io.write_upload_state_to_log(
            run_id="test_run",
            run_path="/some/path/seq1/test_run",
            log_file=log_file,
            local_files=["file1.txt", "file2.txt", "file3.txt"],
            uploaded_files={
                "file1.txt": "abc123",
                "file2.txt": "def456",
            },
            failed_files=["file3.txt"],
        )

        with open(log_file, "r") as fh:
            written_log_contents = json.load(fh)

        expected_log_contents = {
            "run_id": "test_run",
            "run path": "/some/path/seq1/test_run",
            "completed": False,
            "total_local_files": 3,
            "total_uploaded_files": 2,
            "total_failed_upload": 1,
            "failed_upload_files": ["file3.txt"],
            "uploaded_files": {"file1.txt": "abc123", "file2.txt": "def456"},
        }

        self.assertDictEqual(written_log_contents, expected_log_contents)

    def test_when_log_file_exists_that_complete_run_data_is_merged_in(
        self, mock_exists
    ):
        mock_exists.return_value = True
        log_file = os.path.join(TEST_DATA_DIR, "test_run.upload.log.json")

        # set up a log file from a previous partial upload with some
        # failed file uploads
        partial_run_contents = {
            "run_id": "test_run",
            "run path": "/some/path/seq1/test_run",
            "completed": False,
            "total_local_files": 3,
            "total_uploaded_files": 2,
            "total_failed_upload": 1,
            "failed_upload_files": ["file3.txt"],
            "uploaded_files": {"file1.txt": "abc123", "file2.txt": "def456"},
        }

        with open(log_file, "w") as fh:
            json.dump(partial_run_contents, fh)

        io.write_upload_state_to_log(
            run_id="test_run",
            run_path="/some/path/seq1/test_run",
            log_file=log_file,
            local_files=["file1.txt", "file2.txt", "file3.txt"],
            uploaded_files={
                "file3.txt": "ghi789",
            },
            failed_files=[],
        )

        expected_contents = {
            "run_id": "test_run",
            "run path": "/some/path/seq1/test_run",
            "completed": True,
            "total_local_files": 3,
            "total_uploaded_files": 3,
            "total_failed_upload": 0,
            "failed_upload_files": [],
            "uploaded_files": {
                "file1.txt": "abc123",
                "file2.txt": "def456",
                "file3.txt": "ghi789",
            },
        }

        with open(log_file, "r") as fh:
            written_log_contents = json.load(fh)

        self.assertDictEqual(written_log_contents, expected_contents)

    def test_when_log_file_exists_that_partially_complete_run_data_is_merged_in(
        self, mock_exists
    ):
        """
        Test when a previous partially uploaded run has written to the log,
        and another upload attempt still does not upload all files that
        the log is correctly updated but leaves the `completed` key as False
        """
        mock_exists.return_value = True
        log_file = os.path.join(TEST_DATA_DIR, "test_run.upload.log.json")

        # set up a log file from a previous partial upload with some
        # failed file uploads
        partial_run_contents = {
            "run_id": "test_run",
            "run path": "/some/path/seq1/test_run",
            "completed": False,
            "total_local_files": 3,
            "total_uploaded_files": 1,
            "total_failed_upload": 1,
            "failed_upload_files": ["file2.txt", "file3.txt"],
            "uploaded_files": {"file1.txt": "abc123"},
        }

        with open(log_file, "w") as fh:
            json.dump(partial_run_contents, fh)

        io.write_upload_state_to_log(
            run_id="test_run",
            run_path="/some/path/seq1/test_run",
            log_file=log_file,
            local_files=["file1.txt", "file2.txt", "file3.txt"],
            uploaded_files={
                "file2.txt": "def456",
            },
            failed_files=["file3.txt"],
        )

        expected_contents = {
            "run_id": "test_run",
            "run path": "/some/path/seq1/test_run",
            "completed": False,
            "total_local_files": 3,
            "total_uploaded_files": 2,
            "total_failed_upload": 1,
            "failed_upload_files": ["file3.txt"],
            "uploaded_files": {
                "file1.txt": "abc123",
                "file2.txt": "def456",
            },
        }

        with open(log_file, "r") as fh:
            written_log_contents = json.load(fh)

        self.assertDictEqual(written_log_contents, expected_contents)
