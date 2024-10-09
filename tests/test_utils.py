import json
import os
from pathlib import Path
import re
from shutil import rmtree
import unittest
from unittest.mock import patch

import pytest

from tests import TEST_DATA_DIR
from s3_upload.utils import utils


class TestCheckTerminationFileExists(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.test_run_dir = os.path.join(TEST_DATA_DIR, "test_run")
        os.makedirs(
            cls.test_run_dir,
            exist_ok=True,
        )

    @classmethod
    def tearDownClass(cls):
        rmtree(cls.test_run_dir)

    def test_complete_novaseq_run_returns_true(self):
        """
        Check complete NovaSeq runs correctly identified from
        CopyComplete.txt file in the run directory
        """
        termination_file = os.path.join(self.test_run_dir, "CopyComplete.txt")
        open(termination_file, "w").close()

        with self.subTest("Complete NovaSeq run identified"):
            self.assertTrue(
                utils.check_termination_file_exists(self.test_run_dir)
            )

        os.remove(termination_file)

    def test_complete_non_novaseq_run_returns_true(self):
        """
        Check other completed non-NovaSeq runs correctly identified from
        RTAComplete.txt or RTAComplete.xml files
        """
        for suffix in ["txt", "xml"]:
            termination_file = os.path.join(
                self.test_run_dir, f"RTAComplete.{suffix}"
            )

            open(termination_file, "w").close()

            with self.subTest(f"Checking RTAComplete.{suffix}"):
                self.assertTrue(
                    utils.check_termination_file_exists(self.test_run_dir)
                )

            os.remove(termination_file)

    def test_incomplete_sequencing_run_returns_false(self):
        """
        Check incomplete runs correctly identified
        """
        self.assertFalse(
            utils.check_termination_file_exists(self.test_run_dir)
        )


class TestCheckIsSequencingRunDir(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.test_run_dir = os.path.join(TEST_DATA_DIR, "test_run")
        os.makedirs(
            cls.test_run_dir,
            exist_ok=True,
        )

    @classmethod
    def tearDownClass(cls):
        rmtree(cls.test_run_dir)

    def test_non_sequencing_run_dir_returns_false(self):
        # no RunInfo.xml file present in test_data dir => not a run
        utils.check_is_sequencing_run_dir(self.test_run_dir)

    def test_check_sequencing_run_dir_returns_true(self):
        run_info_xml = os.path.join(self.test_run_dir, "RunInfo.xml")
        open(run_info_xml, "w").close()

        with self.subTest("RunInfo.xml exists"):
            utils.check_is_sequencing_run_dir(self.test_run_dir)

        os.remove(run_info_xml)


@patch("s3_upload.utils.utils.path.exists")
@patch("s3_upload.utils.utils.read_upload_state_log")
class TestReadUploadStateLog(unittest.TestCase):
    def test_new_returned_when_upload_log_does_not_exist(
        self, mock_log, mock_exists
    ):
        mock_exists.return_value = False

        upload_state = utils.check_upload_state(
            sub_dir="/some/path/to/test_run"
        )

        with self.subTest("correct path provided to check exists"):
            expected_log_path = "/var/log/s3_upload/uploads/test_run"

            self.assertEqual(expected_log_path, mock_exists.call_args[0][0])

        with self.subTest("correct string returned"):
            self.assertEqual(upload_state, "new")

    def test_uploaded_returned_for_run_that_has_uploaded(
        self, mock_log, mock_exists
    ):
        mock_exists.return_value = True

        mock_log.return_value = {
            "run_id": "test_run",
            "run_path": "/some/path/to/test_run",
            "uploaded": True,
        }

        upload_state = utils.check_upload_state(
            sub_dir="/some/path/to/test_run"
        )

        self.assertEqual(upload_state, "uploaded")

    def test_partial_returned_for_run_that_has_not_fully_uploaded(
        self, mock_log, mock_exists
    ):
        mock_exists.return_value = True

        mock_log.return_value = {
            "run_id": "test_run",
            "run_path": "/some/path/to/test_run",
            "uploaded": False,
        }

        upload_state = utils.check_upload_state(
            sub_dir="/some/path/to/test_run"
        )

        self.assertEqual(upload_state, "partial")


class TestGetRunsToUpload(unittest.TestCase):
    def test_uploadable_directories_correctly_returned(self):
        test_run_dir_structure = [
            # valid incomplete NovaSeq run -> not to upload
            "seq1/run1/RunInfo.xml",
            # valid complete NovaSeq run -> upload
            "seq1/run2/RunInfo.xml",
            "seq1/run2/CopyComplete.txt",
            # valid complete other sequencer run -> upload
            "seq2/run3/RunInfo.xml",
            "seq2/run3/RTAComplete.txt",
            # other non run directory -> not to upload
            "seq2/some_dir/file1.txt",
        ]

        test_run_dir_structure = [
            os.path.join(TEST_DATA_DIR, x) for x in test_run_dir_structure
        ]

        # create the test files and dir structure
        for test_file in test_run_dir_structure:
            os.makedirs(
                Path(test_file).parent,
                exist_ok=True,
            )
            open(test_file, "w").close()

        returned_upload_dirs = utils.get_runs_to_upload(
            monitor_dirs=[
                os.path.join(TEST_DATA_DIR, "seq1"),
                os.path.join(TEST_DATA_DIR, "seq2"),
            ]
        )

        valid_upload_dirs = [
            os.path.join(TEST_DATA_DIR, "seq1/run2"),
            os.path.join(TEST_DATA_DIR, "seq2/run3"),
        ]

        with self.subTest():
            self.assertEqual(valid_upload_dirs, returned_upload_dirs)

        # clear out the created test directories
        rmtree(os.path.join(TEST_DATA_DIR, "seq1"))
        rmtree(os.path.join(TEST_DATA_DIR, "seq2"))


class TestGetSequencingFileList(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """
        Set up a reasonable approximation of a sequencing dir structure with
        files of differing sizes
        """
        cls.sequencing_dir_paths = [
            "Data/Intensities/BaseCalls/L001/C1.1",
            "Data/Intensities/BaseCalls/L002/C1.1",
            "Thumbnail_Images/L001/C1.1",
            "Thumbnail_Images/L002/C1.1",
            "InterOp/C1.1",
            "Logs",
        ]

        cls.sequencing_files = [
            ("Data/Intensities/BaseCalls/L001/C1.1/L001_2.cbcl", 232012345),
            ("Data/Intensities/BaseCalls/L002/C1.1/L002_2.cbcl", 232016170),
            ("Thumbnail_Images/L001/C1.1/s_1_2103_green.png", 69551),
            ("Thumbnail_Images/L002/C1.1/s_1_2103_red.png", 54132),
            ("InterOp/C1.1/BasecallingMetricsOut.bin", 13731),
            ("Logs/240927_A01295_0425_AHJWGFDRX5_Cycle0_Log.00.log", 5243517),
        ]

        cls.test_run_dir = os.path.join(TEST_DATA_DIR, "test_run")

        for sub_dir in cls.sequencing_dir_paths:
            os.makedirs(
                os.path.join(cls.test_run_dir, sub_dir),
                exist_ok=True,
            )

        for seq_file, size in cls.sequencing_files:
            with open(os.path.join(cls.test_run_dir, seq_file), "wb") as f:
                # create test file of given size without actually
                # writing any data to disk
                f.truncate(size)

    @classmethod
    def tearDownClass(cls):
        rmtree(cls.test_run_dir)

    def test_files_returned_in_sorted_order_by_file_size(self):
        expected_file_list = [
            "Data/Intensities/BaseCalls/L002/C1.1/L002_2.cbcl",
            "Data/Intensities/BaseCalls/L001/C1.1/L001_2.cbcl",
            "Logs/240927_A01295_0425_AHJWGFDRX5_Cycle0_Log.00.log",
            "Thumbnail_Images/L001/C1.1/s_1_2103_green.png",
            "Thumbnail_Images/L002/C1.1/s_1_2103_red.png",
            "InterOp/C1.1/BasecallingMetricsOut.bin",
        ]

        expected_file_list = [
            os.path.join(self.test_run_dir, x) for x in expected_file_list
        ]

        returned_file_list = utils.get_sequencing_file_list(
            seq_dir=self.test_run_dir
        )

        self.assertEqual(returned_file_list, expected_file_list)

    def test_empty_directories_ignored_and_only_files_returned(self):
        empty_dir = os.path.join(self.test_run_dir, "empty_dir")
        os.makedirs(
            empty_dir,
            exist_ok=True,
        )

        with self.subTest("empty directory ignored"):
            returned_file_list = utils.get_sequencing_file_list(
                seq_dir=self.test_run_dir
            )
            # just test we get back the same files and ignore their ordering
            self.assertEqual(
                sorted(returned_file_list),
                sorted(
                    os.path.join(self.test_run_dir, x[0])
                    for x in self.sequencing_files
                ),
            )

        rmtree(empty_dir)

    def test_exclude_patterns_removes_matching_files(self):
        """
        Test that both patterns of filenames and / or directory names
        correctly excludes expected files from the returned file list
        """
        exclude_patterns_matched_files = [
            (
                [".*png$"],
                [
                    "Data/Intensities/BaseCalls/L002/C1.1/L002_2.cbcl",
                    "Data/Intensities/BaseCalls/L001/C1.1/L001_2.cbcl",
                    "Logs/240927_A01295_0425_AHJWGFDRX5_Cycle0_Log.00.log",
                    "InterOp/C1.1/BasecallingMetricsOut.bin",
                ],
            ),
            (
                [".*log$"],
                [
                    "Data/Intensities/BaseCalls/L002/C1.1/L002_2.cbcl",
                    "Data/Intensities/BaseCalls/L001/C1.1/L001_2.cbcl",
                    "Thumbnail_Images/L001/C1.1/s_1_2103_green.png",
                    "Thumbnail_Images/L002/C1.1/s_1_2103_red.png",
                    "InterOp/C1.1/BasecallingMetricsOut.bin",
                ],
            ),
            (
                [".*png$", ".*log$"],
                [
                    "Data/Intensities/BaseCalls/L002/C1.1/L002_2.cbcl",
                    "Data/Intensities/BaseCalls/L001/C1.1/L001_2.cbcl",
                    "InterOp/C1.1/BasecallingMetricsOut.bin",
                ],
            ),
            (
                ["Logs/"],
                [
                    "Data/Intensities/BaseCalls/L002/C1.1/L002_2.cbcl",
                    "Data/Intensities/BaseCalls/L001/C1.1/L001_2.cbcl",
                    "Thumbnail_Images/L001/C1.1/s_1_2103_green.png",
                    "Thumbnail_Images/L002/C1.1/s_1_2103_red.png",
                    "InterOp/C1.1/BasecallingMetricsOut.bin",
                ],
            ),
            (
                ["Thumbnail_Images/"],
                [
                    "Data/Intensities/BaseCalls/L002/C1.1/L002_2.cbcl",
                    "Data/Intensities/BaseCalls/L001/C1.1/L001_2.cbcl",
                    "Logs/240927_A01295_0425_AHJWGFDRX5_Cycle0_Log.00.log",
                    "InterOp/C1.1/BasecallingMetricsOut.bin",
                ],
            ),
        ]

        for patterns, expected_files in exclude_patterns_matched_files:
            with self.subTest("files correctly excluded by pattern(s)"):
                returned_file_list = utils.get_sequencing_file_list(
                    seq_dir=self.test_run_dir, exclude_patterns=patterns
                )

                expected_files = [
                    os.path.join(self.test_run_dir, x) for x in expected_files
                ]

                self.assertEqual(
                    sorted(returned_file_list), sorted(expected_files)
                )


class TestSplitFileListByCores(unittest.TestCase):
    items = [1, 2, 3, 4, 5, 6, 7, 8, 100, 110, 120, 130, 140, 150, 160, 170]

    def test_list_split_as_expected(self):

        returned_split_list = utils.split_file_list_by_cores(
            files=self.items, n=4
        )

        expected_list = [
            [1, 5, 100, 140],
            [2, 6, 110, 150],
            [3, 7, 120, 160],
            [4, 8, 130, 170],
        ]

        self.assertEqual(returned_split_list, expected_list)

    def test_correct_return_when_file_length_not_exact_multiple_of_n(self):
        returned_split_list = utils.split_file_list_by_cores(
            files=self.items, n=3
        )

        expected_list = [
            [1, 4, 7, 110, 140, 170],
            [2, 5, 8, 120, 150],
            [3, 6, 100, 130, 160],
        ]

        self.assertEqual(returned_split_list, expected_list)

    def test_error_not_raised_when_n_greater_than_total_files(self):
        returned_split_list = utils.split_file_list_by_cores(files=[1, 2], n=3)

        self.assertEqual([[1], [2]], returned_split_list)

    def test_empty_file_list_returns_empty_list(self):
        returned_split_list = utils.split_file_list_by_cores(files=[], n=2)

        self.assertEqual([], returned_split_list)


class TestParseConfig(unittest.TestCase):
    def test_contents_of_config_returned_as_dict(self):
        config_contents = utils.read_config(
            os.path.join(TEST_DATA_DIR, "test_config.json")
        )

        expected_contents = {
            "max_cores": 4,
            "max_threads": 8,
            "log_level": "INFO",
            "log_dir": "/var/log/s3_upload",
            "monitor": [
                {
                    "monitored_directories": ["/absolute/path/to/sequencer_1"],
                    "bucket": "bucket_A",
                    "remote_path": "/",
                },
            ],
        }

        self.assertEqual(expected_contents, config_contents)


@patch("s3_upload.utils.utils.path.exists")
class TestWriteUploadStateToLog(unittest.TestCase):
    def tearDown(self):
        os.remove(os.path.join(TEST_DATA_DIR, "test_run.upload.log.json"))

    def test_when_no_file_exists_for_fully_uploaded_run(self, mock_exists):
        mock_exists.return_value = False
        log_file = os.path.join(TEST_DATA_DIR, "test_run.upload.log.json")

        utils.write_upload_state_to_log(
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

        utils.write_upload_state_to_log(
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

        utils.write_upload_state_to_log(
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

        utils.write_upload_state_to_log(
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


class TestVerifyArgs(unittest.TestCase):
    # TODO - add tests
    pass


class TestVerifyConfig(unittest.TestCase):
    def test_valid_config_passes(self):
        valid_config = {
            "max_cores": 4,
            "max_threads": 8,
            "log_level": "INFO",
            "log_dir": "/var/log/s3_upload",
            "monitor": [
                {
                    "monitored_directories": [
                        "/absolute/path/to/sequencer_1",
                        "/absolute/path/to/sequencer_2",
                    ],
                    "bucket": "bucket_A",
                    "remote_path": "/",
                },
                {
                    "monitored_directories": [
                        "/absolute/path/to/sequencer_3",
                    ],
                    "bucket": "bucket_B",
                    "remote_path": "/sequencer_3_runs",
                },
            ],
        }

        utils.verify_config(valid_config)

    def test_invalid_config_raises_runtime_error_with_expected_errors(self):
        invalid_config = {
            "max_cores": "4",
            "max_threads": "8",
            "log_level": "INFO",
            "monitor": [
                {
                    "bucket": "bucket_A",
                },
                {
                    "monitored_directories": [
                        "/absolute/path/to/sequencer_3",
                    ],
                    "bucket": 1,
                    "remote_path": "/sequencer_3_runs",
                },
            ],
        }

        expected_errors = (
            "6 errors found in config:\n\tmax_cores must be an"
            " integer\n\tmax_threads must be an integer\n\trequired parameter"
            " log_dir not defined\n\trequired parameter monitored_directories"
            " missing from monitor section 0\n\trequired parameter remote_path"
            " missing from monitor section 0\n\tbucket not of expected type"
            " from monitor section 1. Expected: <class 'str'> | Found <class"
            " 'int'>"
        )

        with pytest.raises(RuntimeError, match=re.escape(expected_errors)):
            utils.verify_config(invalid_config)

    def test_missing_monitor_section_raises_runtime_error(self):
        invalid_config = {
            "max_cores": 4,
            "max_threads": 8,
            "log_dir": "/var/log/s3_upload",
        }

        expected_error = (
            "1 errors found in config:\n\trequired parameter monitor not"
            " defined"
        )

        with pytest.raises(RuntimeError, match=re.escape(expected_error)):
            utils.verify_config(invalid_config)


class TestSizeofFmt(unittest.TestCase):
    def test_expected_value_and_suffix_returned(self):
        bytes_to_formatted = [
            (0, "0.00B"),
            (1, "1.00B"),
            (1050, "1.03KB"),
            (1234567, "1.18MB"),
            (123456789, "117.74MB"),
            (1234567890, "1.15GB"),
            (112233445566, "104.53GB"),
            (11223344556677, "10.21TB"),
            (112233445566778899, "99.68PB"),
            (11223344556677889900, "9.73EB"),
            (111222333444555666777888999, "92.00YiB"),
        ]

        for byte in bytes_to_formatted:
            with self.subTest():
                self.assertEqual(utils.sizeof_fmt(byte[0]), byte[1])
