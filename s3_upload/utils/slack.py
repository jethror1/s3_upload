import json
import requests

from .log import get_logger


log = get_logger("s3_upload")


def format_complete_message(completed=None, failed=None) -> str:
    """
    Format Slack message to send to on completing upload

    Parameters
    ----------
    completed : list
        list of run IDs of successfully uploaded runs
    failed : list
        list of run IDs of runs that failed uploading

    Returns
    -------
    str
        formatted message for posting to Slack
    """
    message = (
        "Completed run uploads."
        f" {len(completed) if completed else 0} successfully uploaded."
        f" {len(failed) if failed else 0} failed uploading."
    )

    if completed:
        message += "\n\nSuccessfully uploaded runs\n\t:black_square: "
        message += "\n\t:black_square: ".join(completed)

    if failed:
        message += "\n\nFailed uploading runs\n\t:black_square: "
        message += "\n\t:black_square: ".join(failed)

    return message


def post_message(url, message) -> None:
    """
    Post message to provided webhook URL, used for posting messages to
    specific Slack channel

    Parameters
    ----------
    url : str
        endpoint to post message to
    message : str
        message to post to Slack
    """
    log.info("Posting message to Slack")
    try:
        response = requests.post(
            url=url,
            data=json.dumps({"text": f":arrow_up: S3 Upload\n\n{message}"}),
            headers={"content-type": "application/json"},
            timeout=30,
        )

        if not response.status_code == 200:
            log.error(
                "Error in post request to Slack (%s): %s",
                response.status_code,
                response.text,
            )
    except requests.exceptions.RequestException as error:
        log.error("Error in post request to Slack: %s", error)
