import os
import csv
import logging
import argparse
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("BibTeX Fetcher")

# Parse command-line arguments
parser = argparse.ArgumentParser(description="Fetch BibTeX data for journal papers.")
parser.add_argument(
    "--inputfile", default="journal.csv", metavar="*.csv",
    help="Input CSV file with a 'bibtex_url' column. Default: journal.csv"
)
parser.add_argument(
    "--outputfile", default="journal_with_bibtex.csv", metavar="*.csv",
    help="Output CSV file with fetched BibTeX entries. Default: journal_with_bibtex.csv"
)
args = parser.parse_args()

def fetch_bibtex(bibtex_url):
    """Fetch BibTeX entry from the given URL."""
    try:
        response = requests.get(bibtex_url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        bibtex_section = soup.find("div", id="bibtex-section")
        if bibtex_section:
            return bibtex_section.find("pre").text.strip()
        logger.warning(f"No BibTeX section found for URL: {bibtex_url}")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch BibTeX from {bibtex_url}: {e}")
        return None

def load_existing_data(outputfile, key_field="title"):
    """
    Load existing BibTeX data from the output file to avoid re-fetching.
    Uses a specified key field (default is 'title') for deduplication.
    """
    existing = {}
    if os.path.exists(outputfile):
        with open(outputfile, mode="r", encoding="utf-8", newline="") as infile:
            reader = csv.DictReader(infile)
            for row in reader:
                key = row.get(key_field, "").strip()
                if key:
                    existing[key] = row
    return existing

def print_statistics(inputfile, outputfile):
    """Print basic statistics about the input and successfully processed entries."""
    total = 0
    if os.path.exists(inputfile):
        with open(inputfile, mode="r", encoding="utf-8", newline="") as infile:
            total = sum(1 for _ in csv.DictReader(infile))
    else:
        logger.warning(f"Input file '{inputfile}' does not exist.")

    success = 0
    if os.path.exists(outputfile):
        with open(outputfile, mode="r", encoding="utf-8", newline="") as outfile:
            for row in csv.DictReader(outfile):
                bibtex = row.get("bibtex_data", "").strip()
                if bibtex and bibtex not in ("Not Available", "No URL"):
                    success += 1

    logger.error(f"Total entries in {inputfile}: {total}")
    logger.error(f"Successfully fetched BibTeX entries in {outputfile}: {success}")

def process_csv(inputfile, outputfile):
    """
    Process input CSV to fetch BibTeX data and write to output CSV.
    Existing entries will be reused if available.
    """
    existing_data = load_existing_data(outputfile, key_field="title")

    with open(inputfile, mode="r", encoding="utf-8", newline="") as infile:
        reader = csv.DictReader(infile)
        input_rows = list(reader)
        fieldnames = reader.fieldnames.copy() if reader.fieldnames else []
        if "bibtex_data" not in fieldnames:
            fieldnames.append("bibtex_data")

    with open(outputfile, mode="w", encoding="utf-8", newline="") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        for row in tqdm(input_rows, desc="Processing papers", leave=False):
            title = row.get("title", "").strip()
            url = row.get("bibtex_url", "").strip()

            if title in existing_data:
                cached = existing_data[title].get("bibtex_data", "").strip()
                if cached and cached not in ("Not Available", "No URL"):
                    row["bibtex_data"] = cached
                    writer.writerow(row)
                    outfile.flush()
                    continue

            if url:
                bibtex = fetch_bibtex(url)
                row["bibtex_data"] = bibtex if bibtex else "Not Available"
            else:
                row["bibtex_data"] = "No URL"

            writer.writerow(row)
            outfile.flush()

if __name__ == "__main__":
    print_statistics(args.inputfile, args.outputfile)
    process_csv(args.inputfile, args.outputfile)