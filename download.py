#!/usr/bin/env python3
"""
download.py - Script to download EDR datasets from data.gov.ua
"""
import os
import sys
import urllib.request
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

DATA_DIR = os.getenv("DATA_DIR", "./data")

DATA_SOURCES = {
    "uo.zip": os.getenv("UO_DATA_URL"),
    "fop.zip": os.getenv("FOP_DATA_URL"),
    "fsu.zip": os.getenv("FSU_DATA_URL"),
}

class DownloadProgressBar(tqdm):
    def update_to(self, b=1, bsize=1, tsize=None):
        if tsize is not None:
            self.total = tsize
        self.update(b * bsize - self.n)

def download_file(url, output_path):
    print(f"Downloading {url} -> {output_path}")
    with DownloadProgressBar(unit='B', unit_scale=True, miniters=1, desc=os.path.basename(output_path)) as t:
        urllib.request.urlretrieve(url, filename=output_path, reporthook=t.update_to)

def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    for filename, url in DATA_SOURCES.items():
        if not url:
            print(f"Warning: No URL provided for {filename}")
            continue
        filepath = os.path.join(DATA_DIR, filename)
        if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
            print(f"File {filename} already exists ({os.path.getsize(filepath) / (1024*1024):.1f} MB). Skipping download.")
            continue
        download_file(url, filepath)
    print("All downloads completed.")

if __name__ == "__main__":
    main()
