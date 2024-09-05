#!/usr/bin/python
import requests
from ratelimit import limits, sleep_and_retry
from pathlib import Path
import gzip

import time
from concurrent.futures import ProcessPoolExecutor
import multiprocessing
import logging
from logging.handlers import QueueHandler, QueueListener

from typing import Sequence


from main_parameters import (
    SEC_HTTP_HEADERS,
    HTTP_TIMEOUT,
    SEC_RATE_LIMIT,
    # MAX_CHUNKSIZE,
)

_logger_name = "sec_edgar_dl"


def is_bad_response(response, row, logger):
    if "Request Rate Threshold Exceeded" in response.text:
        logger.warning(f"Rate limit exceeded. Waiting at {row=}")
        time.sleep(615)
        return True

    if response.status_code == 200:
        return False

    logger.warning(f"Expected 200 but got {response.status_code=} for {row=}")
    time.sleep(1.1)
    return True


# Define the download function
@sleep_and_retry
@limits(calls=1, period=1)
def download_file(row):
    logger = logging.getLogger(_logger_name)
    try:
        logger.info(f"Working on item: {row=}")
        url = row["url"]
        response = requests.get(url=url, headers=SEC_HTTP_HEADERS, timeout=HTTP_TIMEOUT)
        retries = 0
        while is_bad_response(response, row, logger):
            retries += 1
            if retries > 3:
                logger.warning(f"Too many retries for {row=}")
                return
            response = requests.get(
                url=url, headers=SEC_HTTP_HEADERS, timeout=HTTP_TIMEOUT
            )
        file_path = row["file_path"]
        file_path.parent.mkdir(parents=True, exist_ok=True)
        if file_path.suffix == ".gz":
            with gzip.open(file_path, "wb") as fd:
                for chunk in response.iter_content(chunk_size=4096):
                    fd.write(chunk)
        else:
            with open(file_path, "wb") as fd:
                for chunk in response.iter_content(chunk_size=4096):
                    fd.write(chunk)
    except Exception as e:
        logger.warning(f"{row=} failed to download: {e}")


def init_worker(log_queue: multiprocessing.Queue, log_level: int) -> None:
    """Initialize a worker process."""
    logger = logging.getLogger(_logger_name)
    logger.setLevel(log_level)

    handler = QueueHandler(log_queue)
    logger.addHandler(handler)


def file_exists(file_path: Path) -> bool:
    if file_path.exists():
        return True
    if file_path.suffix == ".gz":
        return (file_path.parent / file_path.stem).exists()
    return False


def download_files(rows: Sequence, overwrite: bool = False) -> None:
    # gwerbin/multiprocessing_logging.py
    # https://gist.github.com/gwerbin/e9ab7a88fef03771ab0bf3a11cf921bc
    log_level = logging.WARNING
    log_format = "%(name)s:%(levelname)s:%(processName)s:%(asctime)s: %(message)s"
    formatter = logging.Formatter(log_format)
    stdout_handler = logging.StreamHandler()
    stdout_handler.setFormatter(formatter)

    # Set up the log listener.
    # The log_listener will loop forever (in its own thread), handling log
    # messages as they arrive on the log_queue. See the top-level docstring for
    # more detail on this.
    # Note that we do not need to actually get a Logger object to run this!

    # https://docs.python.org/3/howto/logging-cookbook.html#using-concurrent-futures-processpoolexecutor
    # queue = multiprocessing.Manager().Queue(-1)

    log_queue = multiprocessing.Queue()
    log_listener = QueueListener(log_queue, stdout_handler)

    # -- Run the application -- #

    # Start a background thread that listens for and handles log messages.
    log_listener.start()

    if overwrite:
        print("Overwriting existing files (if any).")
    else:
        print(f"Checking {len(rows)=} for existing files.")
        rows = [row for row in rows if not file_exists(row["file_path"])]

    print(f"starting download of {len(rows)=} files.")

    with ProcessPoolExecutor(
        max_workers=SEC_RATE_LIMIT,
        initializer=init_worker,
        initargs=(log_queue, log_level),
    ) as executor:
        executor.map(
            download_file,
            rows,
            # chunksize=min(len(rows) // SEC_RATE_LIMIT, MAX_CHUNKSIZE),
        )

    # # Put a "shutdown" sentinel on the end of the logging queue.
    # # Note that this calls `.put_nowait()` on the queue, so you might need to do some
    # # error handling here, or subclass QueueListener to behave differently.
    # log_listener.enqueue_sentinel()
    log_listener.stop()

    print("Download complete.")
