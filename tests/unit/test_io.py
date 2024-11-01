import fcntl
import json
import os
import unittest
from unittest.mock import patch

from unit import TEST_DATA_DIR
from s3_upload.utils import io


class TestAcquireLock(unittest.TestCase):
    test_lock = os.path.join(TEST_DATA_DIR, "test.lock")

    def setUp(self):
        if os.path.exists(self.test_lock):
            os.remove(self.test_lock)

    def tearDown(self):
        if os.path.exists(self.test_lock):
            os.remove(self.test_lock)

    @patch("s3_upload.utils.io.os.open", wraps=os.open)
    def test_when_lock_file_does_not_exist_the_correct_flag_set(
        self, mock_open
    ):
        """
        When the lock file does not already exist we should open the lock
        file with O_RDWR (2), O_CREAT (64) and O_TRUNC (512). Therefore
        the flag set for open should be 578
        """
        io.acquire_lock(self.test_lock)

        self.assertEqual(mock_open.call_args[1]["flags"], 578)

    @patch("s3_upload.utils.io.os.open", wraps=os.open)
    def test_when_lock_file_exists_the_correct_flag_set(self, mock_open):
        """
        When the lock file already exists we should open the lock file
        with only O_RDWR (2) and therefore the flag should be 2, which
        will prevent the file from being truncated and losing contents.
        """
        open(self.test_lock, "w").close()

        io.acquire_lock(self.test_lock)

        self.assertEqual(mock_open.call_args[1]["flags"], 2)

    def test_file_lock_correctly_acquired_when_not_already_set(self):
        """
        If we can successfully acquire a file lock we should get an integer
        file descriptor returned and the expected message written to the file
        """
        lock_fd = io.acquire_lock(self.test_lock)

        with open(self.test_lock) as fh:
            lock_contents = fh.read()

        with self.subTest("file descriptor returned"):
            self.assertEqual(type(lock_fd), int)

        with self.subTest("lock contents correct"):
            expected_contents = (
                r"file lock acquired from running upload at"
                r" [\d]{2}:[\d]{2}:[\d]{2} from process"
                r" [\d]+"
            )
            self.assertRegex(lock_contents, expected_contents)

    def test_correctly_exit_when_lock_already_present(self):
        io.acquire_lock(self.test_lock)

        with patch("s3_upload.utils.io.sys.exit") as mock_exit:
            io.acquire_lock(self.test_lock)

            self.assertTrue(mock_exit.called)


class TestReleaseLock(unittest.TestCase):
    test_lock = os.path.join(TEST_DATA_DIR, "test.lock")

    def setUp(self):
        if os.path.exists(self.test_lock):
            os.remove(self.test_lock)

    def tearDown(self):
        if os.path.exists(self.test_lock):
            os.remove(self.test_lock)

    def test_file_lock_correctly_released(self):
        lock_fd = io.acquire_lock(self.test_lock)

        with patch(
            "s3_upload.utils.io.flock", wraps=fcntl.flock
        ) as mock_flock:
            io.release_lock(lock_fd)

            # should pass LOCK_UN (8) to flock call operation parameter
            self.assertEqual(mock_flock.call_args[0][1], 8)

    def test_no_error_raised_if_called_on_invalid_descriptor(self):
        """
        Test that if we try release a lock on a file descriptor that no
        longer exists (i.e the file got somehow removed) that the function
        does not raise an error
        """
        # find a file descriptor that does not already exist
        fd = 0

        while True:
            try:
                os.readlink(f"/proc/self/fd/{fd}")
                fd += 1
            except FileNotFoundError:
                break

        io.release_lock(fd)


@patch("s3_upload.utils.io.os.listdir")
class TestReadSamplesheetFromRunDirectory(unittest.TestCase):
    def test_no_samplesheet_returns_none(self, mock_dir):
        contents = io.read_samplesheet_from_run_directory(TEST_DATA_DIR)

        self.assertEqual(contents, None)

    @patch("s3_upload.utils.io.Path")
    def test_samplesheet_regex_finds_expected_files(self, mock_path, mock_dir):
        """Test when single valid samplesheet found we return the contents"""
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
        """
        Test if no samplesheet is found against the regex that we return None
        """
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

    @patch("s3_upload.utils.io.Path")
    def test_trailing_new_lines_removed_from_returned_contents(
        self, mock_path, mock_dir
    ):
        """
        Trailing new lines in the file will result in empty strings in
        the returned contents list, ensure they are correctly removed when
        reading in
        """
        mock_dir.return_value = ["samplesheet1.csv"]
        mock_path.return_value.read_text.return_value = (
            "Sample_ID\nsample_a\nsample_b\nsample_n\n\n"
        )

        contents = io.read_samplesheet_from_run_directory(TEST_DATA_DIR)

        expected_contents = ["Sample_ID", "sample_a", "sample_b", "sample_n"]

        self.assertEqual(contents, expected_contents)


class TestReadUploadStateLog(unittest.TestCase):
    def test_file_contents_returned_correctly(self):
        upload_log = os.path.join(
            TEST_DATA_DIR, "complete_run_upload.log.json"
        )

        read_contents = io.read_upload_state_log(upload_log)

        expected_contents = {
            "run_id": "181024_A01295_001_ABC123",
            "run_path": "/genetics/181024_A01295_001_ABC123",
            "completed": True,
            "total_local_files": 2,
            "total_uploaded_files": 2,
            "total_failed_upload": 0,
            "failed_upload_files": [],
            "uploaded_files": {"file1.txt": "abc123", "file2.txt": "def456"},
        }

        self.assertDictEqual(read_contents, expected_contents)

    def test_incomplete_upload_displays_correct_debug_log(self):
        upload_log = os.path.join(
            TEST_DATA_DIR, "incomplete_run_upload.log.json"
        )

        with self.assertLogs("s3_upload", level="DEBUG") as log:
            io.read_upload_state_log(upload_log)

            expected_log_message = (
                "total local files: 2 | total uploaded files: 1 | total"
                " failed upload: 0 | total files to upload 1"
            )

            self.assertIn(expected_log_message, "".join(log.output))


@patch("s3_upload.utils.io.os.path.exists")
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
            "run_path": "/some/path/seq1/test_run",
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
            "run_path": "/some/path/seq1/test_run",
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
            "run_path": "/some/path/seq1/test_run",
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
            "run_path": "/some/path/seq1/test_run",
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
            "run_path": "/some/path/seq1/test_run",
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
            "run_path": "/some/path/seq1/test_run",
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
