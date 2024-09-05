import csv

from downloader import download_files

from main_parameters import SEC_ARCHIVES_URL, ARCHIVES_FOLDER, FILTERED_INDEX_FILE

if __name__ == "__main__":
    rows = []
    with open(FILTERED_INDEX_FILE, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            row["file_path"] = ARCHIVES_FOLDER / row["filename"]
            row["url"] = SEC_ARCHIVES_URL + row["filename"]
            rows.append(row)

    download_files(rows)
