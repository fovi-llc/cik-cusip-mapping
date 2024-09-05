import argparse
import csv

from downloader import download_files

from main_parameters import SEC_ARCHIVES_URL, ARCHIVES_FOLDER

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("filing", type=str)
    parser.add_argument("folder", type=str)

    args = parser.parse_args()
    filing = args.filing
    folder = args.folder

    rows = []
    with open("full_index.csv", "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if filing in row["form"]:
                row["file_path"] = ARCHIVES_FOLDER / row["url"]
                row["url"] = SEC_ARCHIVES_URL + row['url']
                rows.append(row)

    download_files(rows)
