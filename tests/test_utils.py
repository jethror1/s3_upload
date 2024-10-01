import os
from shutil import rmtree
import unittest

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

            with self.subTest("Checking RTAComplete.txt"):
                self.assertTrue(
                    utils.check_termination_file_exists(self.test_run_dir)
                )

            os.remove(termination_file)

    def incomplete_sequencing_run_returns_false(self):
        """
        Check incomoplete runs correctly identified
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
