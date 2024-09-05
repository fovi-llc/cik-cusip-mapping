#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
dl_idx.py

This module is responsible for downloading master index data; weblinked references archived SEC filings.
It handles the communication with the SEC's EDGAR system, retrieves the reference data, and stores
it locally for further processing. The filtered output is a subset (e.g. only 13D and 13G filings) of
the full (master) index file.

"""
import csv          # For reading from and writing to CSV files
import re           # For regular expressions, useful in pattern matching and text processing

from downloader import download_files

import tarfile

from main_parameters import(
    SEC_ARCHIVES_URL,
    SEC_MASTER_URLS,
    FILING_TYPES, 
    MASTER_INDEX_PREFIX, 
    FILTERED_INDEX_FILE,
    DATA_RAW_FOLDER,
    ARCHIVES_FOLDER,
)


def download_sec_index_of_filings():
    rows = [{"url": SEC_ARCHIVES_URL + url, "file_path": ARCHIVES_FOLDER / url} for url in SEC_MASTER_URLS]
    download_files(rows)


def apply_pattern_to_lines(pattern, lines):
    """
    Filters lines based on a regular expression pattern.
    Args:
        pattern (re.Pattern): Compiled regular expression pattern to match against.
        lines (iterator): An iterator yielding lines from a file.
    Yields:
        list: Fields from each line that matches the pattern.
    """
    for line in lines:
        if ".txt" in line:
            fields = line.strip().split("|")
            if len(fields) > 2 and pattern.search(fields[2]):
                yield fields

def process_tarfile(tar_path, line_processor):
    """
    Processes files within a tar.gz archive and applies a line processor function.
    Args:
        tar_path (Path): Path to the tar.gz archive.
        line_processor (function): Function to apply to each line in the extracted files.
    Yields:
        generator: Yields processed lines from the tar.gz archive.
    """
    with tarfile.open(tar_path, "r:gz") as tar:
        for member in tar.getmembers():
            if member.isfile() and member.name.endswith(".idx"):
                print(f"Processing {member.name}")
                with tar.extractfile(member) as file_obj:
                    lines = (line.decode('latin1') for line in file_obj)
                    yield from line_processor(lines)

def write_csv(output_file, headers, data_generator):
    """
    Writes filtered data to a CSV file.
    Args:
        output_file (Path): Path to the output CSV file.
        headers (list): List of column headers for the CSV file.
        data_generator (iterator): An iterator yielding rows of data to write to the CSV file.
    """
    with output_file.open(mode="w", errors="ignore") as csvfile:
        wr = csv.writer(csvfile)
        wr.writerow(headers)
        for row in data_generator:
            wr.writerow(row)

def filter_sec_index_of_filings_to_csv():
    """
    Filters the master index of filings by applying a pattern to select specific filing types.

    This function extracts files from a tar.gz archive, decodes the lines, filters them based on 
    specified filing types, and writes the filtered data to a CSV file.
    """
    master_index_archive = (DATA_RAW_FOLDER / MASTER_INDEX_PREFIX).with_suffix(".tar.gz")
    pattern = re.compile("|".join(FILING_TYPES), re.IGNORECASE)
    headers = ["cik", "comnam", "form", "date", "filename"]

    filtered_data = process_tarfile(master_index_archive, lambda lines: apply_pattern_to_lines(pattern, lines))

    # make sure the directory exists
    FILTERED_INDEX_FILE.parent.mkdir(parents=True, exist_ok=True)
    write_csv(FILTERED_INDEX_FILE, headers, filtered_data)


if __name__ == "__main__":
    download_sec_index_of_filings()
    filter_sec_index_of_filings_to_csv()
