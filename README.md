# s3_upload
Uploads Illumina sequencing runs into AWS S3 storage.

Can interactively upload a single sequencing run or be scheduled (i.e. via cron) to monitor one or more directories for newly completed sequencing runs and automatically upload into a given S3 bucket location.


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
* `--local_path`: path to sequencing run to upload
* `--bucket`: existing S3 bucket with write permission for authenticated user
* `--remote_path` (optional | default: `/`): path in bucket in which to upload the run
* `cores` (optional | default: maximum available): total CPU cores to split uploading of files across
* `--threads` (optional | default: 4): total threads to use per CPU core for uploading


Available inputs for `monitor`:
* `--config`: path to JSON config file for monitoring (see below)


## Logging

Logs from stdout and stderr by default are written to `/var/log/s3_upload`. These are on a rotating time handle at midnight and backups stored in the same directory for 5 days.

## Docker

TODO