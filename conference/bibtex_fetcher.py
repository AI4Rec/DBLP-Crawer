import os
import csv
import logging
import argparse
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

# Configure logging to show only ERROR level messages
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger("BibTeX Fetcher")

# Set up command-line arguments
parser = argparse.ArgumentParser(description="Fetch BibTeX data for academic papers.")
parser.add_argument(
    "--inputfile", default="conference.csv", metavar="*.csv",
    help="Input CSV file with a 'bibtex_url' column. Default: conference.csv"
)
parser.add_argument(
    "--outputfile", default="conference_with_bibtex.csv", metavar="*.csv",
    help="Output CSV file to save BibTeX results. Default: conference_with_bibtex.csv"
)
args = parser.parse_args()

def fetch_bibtex(bibtex_url):
    """Fetch BibTeX entry from the provided URL."""
    try:
        response = requests.get(bibtex_url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        bibtex_section = soup.find("div", id="bibtex-section")
        if bibtex_section:
            return bibtex_section.find("pre").text.strip()
        else:
            logger.warning(f"No BibTeX section found for URL: {bibtex_url}")
            return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch BibTeX from {bibtex_url}: {e}")
        return None

def load_existing_data(outputfile, key_field="title"):
    """
    Load already processed records to avoid duplicate fetching.
    Uses title as the unique key.
    """
    existing_data = {}
    if os.path.exists(outputfile):
        with open(outputfile, mode="r", encoding="utf-8", newline="") as infile:
            reader = csv.DictReader(infile)
            for row in reader:
                key = row.get(key_field, "").strip()
                if key:
                    existing_data[key] = row
    return existing_data

def print_statistics(inputfile, outputfile):
    """
    Print the number of total entries and successfully fetched BibTeX records.
    """
    total_entries = 0
    if os.path.exists(inputfile):
        with open(inputfile, mode="r", encoding="utf-8", newline="") as infile:
            reader = csv.DictReader(infile)
            total_entries = sum(1 for _ in reader)
    else:
        logger.warning(f"Input file '{inputfile}' does not exist.")

    success_entries = 0
    if os.path.exists(outputfile):
        with open(outputfile, mode="r", encoding="utf-8", newline="") as outfile:
            reader = csv.DictReader(outfile)
            for row in reader:
                bibtex = row.get("bibtex_data", "").strip()
                if bibtex and bibtex not in ("Not Available", "No URL"):
                    success_entries += 1

    logger.error(f"Total entries in {inputfile}: {total_entries}")
    logger.error(f"Successfully fetched BibTeX entries in {outputfile}: {success_entries}")

def process_csv(inputfile, outputfile):
    """
    Process the input CSV file and write BibTeX entries to the output file.
    Progress is saved incrementally to prevent data loss.
    """
    processed_data = load_existing_data(outputfile, key_field="title")

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
            bibtex_url = row.get("bibtex_url", "").strip()

            # Use existing BibTeX data if available
            if title in processed_data:
                existing_bibtex = processed_data[title].get("bibtex_data", "").strip()
                if existing_bibtex and existing_bibtex not in ("Not Available", "No URL"):
                    row["bibtex_data"] = existing_bibtex
                    writer.writerow(row)
                    outfile.flush()
                    continue

            # Fetch new BibTeX data if URL is provided
            if bibtex_url:
                bibtex_data = fetch_bibtex(bibtex_url)
                row["bibtex_data"] = bibtex_data if bibtex_data else "Not Available"
            else:
                row["bibtex_data"] = "No URL"

            writer.writerow(row)
            outfile.flush()

if __name__ == "__main__":
    print_statistics(args.inputfile, args.outputfile)
    process_csv(args.inputfile, args.outputfile)