#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import time
import threading
from pathlib import Path
from typing import List, Callable
import tarfile
from datetime import datetime
import logging
import os
import asyncio
import aiohttp
import queue
import logging
import multiprocessing
import string
from concurrent.futures import ProcessPoolExecutor
from logging.handlers import QueueHandler, QueueListener
from random import Random
from time import sleep
from typing import Sequence

import requests

from concurrent.futures import ProcessPoolExecutor
from ratelimit import limits, sleep_and_retry

from tqdm import tqdm

from main_parameters import SEC_USER_AGENT, SEC_MASTER_URLS, SEC_RATE_LIMIT


_logger_name = "sec_edgar_dl"



class ProgressTracker:
    """
    A class for tracking and displaying the progress of file downloads and processing.

    This class uses tqdm to create a progress bar that updates in real-time,
    showing the number of files downloaded and processed out of the total.

    Attributes:
        total_files (int): The total number of files to be downloaded and processed.
        downloaded_files (int): The number of files that have been downloaded.
        processed_files (int): The number of files that have been processed.
        pbar (tqdm): The progress bar object.

    Methods:
        update(download_queue, process_queue): Asynchronously updates the progress bar
                                               based on the state of the download and process queues.
    """

    def __init__(self, total_files):
        self.total_files = total_files
        self.downloaded_files = 0
        self.processed_files = 0
        self.pbar = tqdm(total=total_files, desc="Downloading", unit="file")

    async def update(self, download_queue, process_queue):
        while self.downloaded_files < self.total_files or self.processed_files < self.total_files:
            self.downloaded_files = self.total_files - download_queue.qsize()
            self.processed_files = self.total_files - process_queue.qsize()
            self.pbar.n = self.processed_files
            self.pbar.set_postfix(downloaded=self.downloaded_files, processed=self.processed_files)
            self.pbar.refresh()
            await asyncio.sleep(0.1)
        self.pbar.close()


# gwerbin/multiprocessing_logging.py
# https://gist.github.com/gwerbin/e9ab7a88fef03771ab0bf3a11cf921bc


def init_worker(log_queue: multiprocessing.Queue, log_level: int) -> None:
    """Initialize a worker process."""
    logger = logging.getLogger(_logger_name)
    logger.setLevel(log_level)

    handler = QueueHandler(log_queue)
    logger.addHandler(handler)


# Define the download function
@sleep_and_retry
@limits(calls=1, period=1)
def download_file(row):
    cik, date, url, file_path = row
    logger = logging.getLogger(_logger_name)
    logger.info(f"Working on {row=}")
    try:
        print(f"Downloading {cik}, {date}")
        txt = requests.get(
            url=SEC_MASTER_URLS + url, headers=SEC_USER_AGENT, timeout=60
        ).text
        while "Request Rate Threshold Exceeded" in txt:
            print("Rate limit exceeded. Waiting for 610 seconds.")
            time.sleep(610)
            txt = requests.get(
                f"https://www.sec.gov/Archives/{url}", headers=SEC_USER_AGENT, timeout=60
            ).text
        with open(file_path, "w", errors="ignore") as f:
            f.write(txt)
    except Exception as e:
        print(f"{cik}, {date} failed to download: {e}")


class EfficientDownloader:
    """A class for efficiently downloading and processing files."""

    def __init__(
        self,
        urls: List[str],
        download_dir: Path,
        archive_prefix: str,
        process_func: Callable = lambda x: x,  # default process_func is Identity
        user_agent: dict = None,
        rate_limit: int = 10,
    ):
        """
        Initialize the EfficientDownloader class.

        :param urls: List of URLs to download.
        :param download_dir: Directory to store downloaded files.
        :param archive_prefix: Filepath prefix of the tar.gz archive to create.
        :param processor_fun: User-supplied function to process downloaded files.
        :param user_agent: Optional user agent dictionary.
        :param rate_limit: Rate limit in requests per second (default: 10).
        :param disable_async: Disables asynchronous downloads; (greater overhead and thread control).
        """
        self.urls = urls
        self.process_func = process_func
        self.download_dir = download_dir
        self.archive_prefix = archive_prefix
        self.common_prefix = os.path.commonprefix(self.urls)
        if not self.common_prefix.endswith("/"):
            self.common_prefix = self.common_prefix.rsplit("/", 1)[0] + "/"
        self.user_agent = user_agent or {"User-Agent": "EfficientDownloader/1.0"}

        self.failed_downloads = []
        self.successful_downloads = []
        self.stats = {
            "total_files": len(urls),
            "successful_downloads": 0,
            "failed_downloads": 0,
            "total_download_size": 0,
            "total_processed_size": 0,
            "total_download_time": 0,
            "total_processing_time": 0,
            "elapsed_wall_time": 0,
        }

        # Create download directory if it doesn't exist
        self.download_dir.mkdir(parents=True, exist_ok=True)

        # Set up logging
        self.logger = logging.getLogger(__name__)
        logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

        self.rate_limiter = RateLimiter(rate_limit)

    async def download_all(self):
        """Download and process all files in the URL list asynchronously."""
        download_queue = asyncio.Queue()
        process_queue = queue.Queue()
        batch_size = 10  # Example batch size

        overall_start_time = time.time()  # Start timing the overall process

        progress_tracker = ProgressTracker(len(self.urls))
        tracker_task = asyncio.create_task(progress_tracker.update(download_queue, process_queue))

        # Start processing in a separate thread pool
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as process_executor:
            process_future = process_executor.submit(self._process_files, process_queue)

            # Asynchronous downloading
            async with aiohttp.ClientSession() as session:
                for i in range(0, len(self.urls), batch_size):
                    batch = self.urls[i:i + batch_size]
                    download_tasks = [self._download_file(session, url, download_queue) for url in batch]
                    await asyncio.gather(*download_tasks)

            # Signal end of downloads
            await download_queue.put(None)

            # Transfer items from download_queue to process_queue
            while True:
                item = await download_queue.get()
                if item is None:
                    process_queue.put(None)  # Signal end of processing
                    break
                process_queue.put(item)

        # Wait for processing to complete
        process_future.result()

        # Wait for the progress tracker to finish
        await tracker_task

        # Calculate total elapsed time
        self.stats["elapsed_wall_time"] = time.time() - overall_start_time


    async def _download_file(self, session, url: str, download_queue: asyncio.Queue):
        """Download a file asynchronously and add it to the download queue."""
        file_name = self._get_filename(url)
        retries = 0
        max_retries = 3
        while retries < max_retries:
            try:
                self.rate_limiter.wait()
                start_time = time.time()
                async with session.get(url, headers=self.user_agent) as response:
                    content = await response.read()
                download_time = time.time() - start_time
                await download_queue.put((file_name, content, download_time))
                self.logger.info(f"Successfully downloaded: {url}")
                break
            except Exception as e:
                retries += 1
                if retries == max_retries:
                    self.failed_downloads.append((url, str(e), retries))
                    self.logger.error(f"Failed to download {url} after {retries} attempts: {e}")
                    with threading.Lock():
                        self.stats["failed_downloads"] += 1
                else:
                    self.logger.warning(f"Retry {retries} for {url}. Retrying...")
                    await asyncio.sleep(2**retries)

    def _process_files(self, process_queue: queue.Queue):
        """Process files from the queue and save the results."""
        while True:
            item = process_queue.get()
            if item is None:
                break
            file_name, content, download_time = item
            start_time = time.time()
            processed_content = self.process_func(content)
            processing_time = time.time() - start_time
            file_path = self.download_dir / file_name
            with file_path.open("wb") as f:
                f.write(processed_content)
            with threading.Lock():
                self.stats["successful_downloads"] += 1
                self.stats["total_download_size"] += len(content)
                self.stats["total_processed_size"] += len(processed_content)
                self.stats["total_download_time"] += download_time
                self.stats["total_processing_time"] += processing_time
                self.successful_downloads.append(file_path)
            self.logger.info(f"Successfully processed: {file_name}")

    def _get_filename(self, url: str) -> str:
        """
        Generate a filename based on the URL, stripping the common prefix
        and replacing '/' with '_' to ensure a valid filename.

        :param url: URL of the file.
        :return: Generated filename.
        """
        filename = url[len(self.common_prefix) :] or url.rsplit("/", 1)[-1]
        return filename.replace("/", "_")

    def log_failures(self):
        """Write failed downloads to a log file."""
        if not self.failed_downloads:
            return
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        log_file = self.download_dir / f"failed_downloads_{timestamp}.log"
        with log_file.open("w") as f:
            for url, error, attempts in self.failed_downloads:
                f.write(f"URL: {url}\nError: {error}\nAttempts: {attempts}\n\n")
        self.logger.info(f"Failed downloads logged to: {log_file}")

    def create_archive(self):
        """Compress successfully downloaded files into a tar.gz archive."""
        archive_file = (self.download_dir / self.archive_prefix).with_suffix(".tar.gz")
        with tarfile.open(archive_file, "w:gz") as tar:
            for file_path in self.successful_downloads:
                tar.add(file_path, arcname=file_path.name)
                file_path.unlink()  # Remove the original file after adding to archive
        self.logger.info(f"Created archive: {archive_file}")

    def _print_summary_stats(self):
        """Print summary statistics of the download process."""
        self.logger.info("\nDownload Summary:")

        for key, value in self.stats.items():
            # Format the keys that represent sizes in bytes to MB with 2 decimal places
            if "size" in key:
                formatted_value = f"{value / 1024 / 1024:.2f} MB"
            elif "time" in key:
                formatted_value = f"{value:.2f} seconds"
            else:
                formatted_value = str(value)

            # Log the formatted value with the key
            self.logger.info(f"{key.replace('_', ' ').capitalize()}: {formatted_value}")

        # Add a new log entry for average download speed
        if self.stats["total_download_size"] > 0 and self.stats["elapsed_wall_time"] > 0:
            avg_speed = (self.stats["total_download_size"] / 1024 / 1024) / self.stats["elapsed_wall_time"]
            self.logger.info(f"Average download speed: {avg_speed:.2f} MB/s")

    def download_and_process(self):
        """Download, process all URLs, create an archive, and log failures."""
        self.logger.info("Starting download process...")
        asyncio.run(self.download_all())
        self.logger.info("Download process completed.")

        print("Creating archive...")
        self.create_archive()
        print("Logging failed downloads...")
        self.log_failures()
        self._print_summary_stats()
