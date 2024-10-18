# s3 upload

![pytest](https://github.com/eastgenomics/s3_upload/actions/workflows/pytest.yml/badge.svg)

Uploads Illumina sequencing runs into AWS S3 storage.

There are 2 modes implemented, one to interactively upload a single sequencing run, and another to monitor on a schedule (i.e. via cron) one or more directories for newly completed sequencing runs and automatically upload into a given S3 bucket location.


## Usage

Uploading a single run:
```
python3 s3_upload/s3_upload.py upload \
    --local_path /path/to/run/to/upload \
    --bucket myBucket
```

Adding to a crontab:
```
python3 s3_upload/s3_upload.py monitor --config /path/to/config.json
```


## Inputs

Available inputs for `upload`:
* `--local_path` (required): path to sequencing run to upload
* `--bucket` (required): existing S3 bucket with write permission for authenticated user
* `--remote_path` (optional | default: `/`): path in bucket in which to upload the run
* `cores` (optional | default: maximum available): total CPU cores to split uploading of files across
* `--threads` (optional | default: 4): total threads to use per CPU core for uploading


Available inputs for `monitor`:
* `--config`: path to JSON config file for monitoring (see below)
* `--dry_run` (optional): calls everything except the actual upload to check what runs would be uploaded

## Config

The behaviour for monitoring of directories for sequencing runs to upload is controlled through the use of a JSON config file. An example may be found [here](https://github.com/eastgenomics/s3_upload/blob/main/example/example_config.json).

The top level keys that may be defined include:
* `max_cores` (int): maximum number of CPU cores to split uploading across (default is the maximum available)
* `max_threads` (int): the maximum number of threads to use per CPU core
* `log_level` (str): the level of logging to set, available options are defined [here](https://docs.python.org/3/library/logging.html#logging-levels)
* `log_dir` (str): path to where to store logs (default is `/var/log/s3_upload`)

Monitoring of specified directories for sequencing runs to upload are defined in a list of dictionaries under the `monitor` key. The available keys per monitor dictionary include:
* `monitored_directories` (list): list of absolute paths to directories to monitor for new sequencing runs (i.e the location the sequencer outputs to)
* `bucket` (str): name of S3 bucket to upload to
* `remote_path` (str): parent path in which to upload sequencing run directories in the specified bucket
* `sample_regex` (str | optional): regex pattern to match against all samples parsed from the samplesheet, all samples must match this pattern to upload the run. This is to be used for controlling upload of specific runs where samplenames inform the assay / test.

Each dictionary inside of the list to monitor allows for setting separate upload locations for each of the monitored directories. For example, in the below example the output of both `sequencer_1` and `sequencer_2` would be uploaded to the root of `bucket_A`, and the output of `sequencer_3` would be uploaded into `sequencer_3_runs` in `bucket_B`. Any number of these dictionaries may be defined in the monitor list.

```
    "monitor": [
        {
            "monitored_directories": [
                "/absolute/path/to/sequencer_1",
                "/absolute/path/to/sequencer_2"
            ],
            "bucket": "bucket_A",
            "remote_path": "/",
            "sample_regex": "_assay_1_code_|_assay_code_2_"
        },
        {
            "monitored_directories": [
                "/absolute/path/to/sequencer_3"
            ],
            "bucket": "bucket_B",
            "remote_path": "/sequencer_3_runs"
        }
    ]
```
*Example `monitor` config section defining two sets of monitored directories and upload locations*

## Logging

All logs by default are written to `/var/log/s3_upload`. Logs from stdout and stderr are written to the file `s3_upload.log`, and are on a rotating time handle at midnight and backups stored in the same directory for 5 days.

> [!IMPORTANT]
> Write permission is required to the default or specified log directory, if not a `PermissionError` will be raised on checking the log directory permissions.

A JSON log file is written per sequencing run to upload that is stored in a `uploads/` subdirectory of the main log directory. Each of these log files is used to store the state of the upload (i.e if it has completed or only partially uploaded), and what files have been uploaded along with the S3 "ETag" ID. This log file is used when searching for sequencing runs to upload. Any runs with a log file containing `"completed": true` will be skipped and not reuploaded, and those with a log file containing `"completed": false` indicates a run that previously did not complete uploading and will be added to the upload list.

The expected fields in this log file are:

* `run_id` (str) - the ID of the run (i.e. the name of the run directory)
* `run_path` (str) - the absolute path to the run directory being uploaded
* `completed` (bool) - the state of the run upload
* `total_local_files` (int) - the total number of files in the run expected to upload
* `total_uploaded_files` (int) - the total number of files that have been successfully uploaded
* `total_failed_upload` (int) - the total number of files that failed to upload in the most recent upload attempt
* `failed_upload_files` (list) - list of filepaths of files that failed to upload
* `uploaded_files` (dict) - mapping of filename to ETag ID of successfully uploaded files


## Docker

TODO

## Notes
TODO
