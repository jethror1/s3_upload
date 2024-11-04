"""
End to end test for an upload being interrupted and failing to upload
all files, then resuming on the next run.
"""
    @staticmethod
    def upload_side_effect(**kwargs):
        """
        Helper function to pass to the side_effect param when mocking
        the upload_single_file function.

        This allows us to simulate failing the upload of a single file
        whilst still uploading the other files, resulting in a partially
        uploaded run.

        For all files except the RunInfo.xml we will simply pass through
        the call to upload_single_file with the provided arguments.
        """

        @classmethod
        def setUpClass(cls):
            # create test sequencing run in set monitored directory
            cls.run_1 = os.path.join(TEST_DATA_DIR, "sequencer_a", "run_1")

            # create as a complete run with some example files
            create_files(
                cls.run_1,
                "RunInfo.xml",
                "CopyComplete.txt",
                "Config/Options.cfg",
                "InterOp/EventMetricsOut.bin",
            )

            shutil.copy(
                os.path.join(TEST_DATA_DIR, "example_samplesheet.csv"),
                os.path.join(cls.run_1, "samplesheet.csv"),
            )

            # define full unique path to upload test runs to
            now = datetime.now().strftime("%y%m%d_%H%M%S")
            cls.remote_path = f"s3_upload_e2e_test/{now}/sequencer_a"

            # add in the sequencer to monitor with test run
            config_file = os.path.join(TEST_DATA_DIR, "test_config.json")
            config = deepcopy(BASE_CONFIG)
            config["log_dir"] = os.path.join(TEST_DATA_DIR, "logs")
            config["monitor"].append(
                {
                    "monitored_directories": [
                        os.path.join(TEST_DATA_DIR, "sequencer_a")
                    ],
                    "bucket": S3_BUCKET,
                    "remote_path": cls.remote_path,
                }
            )

            with open(config_file, "w") as fh:
                json.dump(config, fh)

            # mock command line args that would be set pointing to the config
            cls.mock_args = patch("s3_upload.s3_upload.parse_args").start()
            cls.mock_args.return_value = Namespace(
                config=config_file,
                dry_run=False,
                mode="monitor",
            )

            # mock the file lock that stops concurrent uploads as this breaks
            # when running unittest
            cls.mock_flock = patch("s3_upload.s3_upload.acquire_lock").start()

            cls.mock_slack = patch(
                "s3_upload.s3_upload.slack.post_message"
            ).start()

            # call the main entry point to run the upload, with a side effect
            # of failing to upload the RunInfo.xml file
            with patch(
                "s3_upload.utils.upload.upload_single_file", side_effect=effect
            ):
            s3_upload_main()

            # capture the stdout/stderr logs written to log file for testing
            with open(
                os.path.join(TEST_DATA_DIR, "logs/s3_upload.log"), "r"
            ) as fh:
                cls.upload_log = fh.read().splitlines()

        @classmethod
        def tearDownClass(cls):
            shutil.rmtree(cls.run_1)

            os.remove(
                os.path.join(TEST_DATA_DIR, "logs/uploads/run_1.upload.log.json")
            )

            os.remove(os.path.join(TEST_DATA_DIR, "test_config.json"))

            # delete the logger log files
            for log_file in glob(os.path.join(TEST_DATA_DIR, "logs", "*log*")):
                os.remove(log_file)

            # clean up the remote files we just uploaded
            bucket = boto3.resource("s3").Bucket(S3_BUCKET)
            objects = bucket.objects.filter(Prefix=cls.remote_path)
            objects = [{"Key": obj.key} for obj in objects]

            if objects:
                bucket.delete_objects(Delete={"Objects": objects})

            cls.mock_args.stop()
            cls.mock_flock.stop()
            cls.mock_slack.stop()



            if kwargs["local_file"].endswith("RunInfo.xml"):
                raise RuntimeError("Upload failed for RunInfo.xml")

            return upload_single_file(**kwargs)

