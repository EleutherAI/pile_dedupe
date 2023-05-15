"""
This module provides helper functions for working with the Pile duplicates. 

It's runnable for displaying duplicate statistics and inspecting the duplicates. When inspecting
we build an index mapping all duplicates and their respective matches (in the first duplicates file) 
to their pile document. Then we can step through and inspect the duplicate and their matches as a sanity
test of the lsh.

Before using this script you will need to have created the duplicates using dedupe.py. If you are inspecting
the duplicates you will also need the downloaded pile in the "pile" directory or have a symlink - though this
is a requried condition for all the substeps anyway.

Relevant Functions
------------------
yield_duplicates(duplicates_directory, first_file_only=False)
    Returns a generator for pile duplicates and their matching documents. first_file_only is really only used
    when inspecting to avoid processing all the duplicates.

get_duplicate_stats(duplicates_directory)
    Returns a dictionary of the following, will depend on the lsh_threshold setting used.
    {"Data": "Pile duplicate statistics", "Total Duplicates": duplicate_count, "lsh_threshold": lsh_threshold }

Arguments
---------
--duplicates_directory (-dir)
    Location of the duplicates files from the dedupe step. Default: "pile_duplicates"
--inspect_duplicates
    Flag that will allow an inspection of the duplicates against their matches in the Pile
"""

import os
import argparse
import json
import glob
import tqdm
import pickle

from working_with_pile import get_pile_statistics, yield_pile

import logging
from logger import setup_logger_tqdm
logger = logging.getLogger(__name__)

def yield_duplicates(duplicates_directory, first_file_only=False):
    query = os.path.join(duplicates_directory, "*duplicates_*.pkl")
    duplicates_files = [x for x in glob.glob(query) if "smol" not in x]

    for file in duplicates_files:
        logger.info(f"Reading from duplicate file: {file}")        
        data = pickle.load(open(file, "rb"))
        for offset, found_offsets in data:
            yield (offset, found_offsets)
        if first_file_only:
            break


def inspect_duplicates(duplicates_directory):
    pile_statistics = get_pile_statistics()

    indexed_documents_file = os.path.join(duplicates_directory, "verify_pile_documents.pkl")
    if os.path.exists(indexed_documents_file):
        logger.info("Pile duplicates document index already exists, loading.")
        indexed_documents = pickle.load(open(indexed_documents_file, "rb"))
    else:        
        # Prepare space + index relevant documents
        logger.info("Building pile duplicates document index...")
        indexed_documents = {}
        count = 0        
        logger.info("Preparing index with duplicates.")
        for offset, found_offsets in yield_duplicates(duplicates_directory, first_file_only=True):
            indexed_documents[offset] = None
            for found_offset in found_offsets:
                indexed_documents[found_offset] = None

            count += 1

        logger.info("Pile full scan.")
        with tqdm.tqdm(total=pile_statistics["Document Count"], dynamic_ncols=True, unit="docs", miniters=100000) as progress:
            for offset, document in yield_pile():
                if offset in indexed_documents:
                    indexed_documents[offset] = document
                progress.update()

        pickle.dump(indexed_documents, open(indexed_documents_file, "wb"))

    for offset, found_offsets in yield_duplicates(duplicates_directory, True):
        logger.info("===========================")        
        logger.info(f"DOCUMENT - offset {offset}")
        logger.info("---------------------------")   
        logger.info(indexed_documents[offset])
        logger.info("---------------------------")           
        for found_offset in found_offsets:
            input("Press enter for next matching document")
            logger.info(f"MATCHING DOCUMENT - offset {found_offset}")
            logger.info("---------------------------")   
            logger.info(indexed_documents[found_offset])
            logger.info("---------------------------")
        input("No more matching documents. Press enter for next duplicate.")
        logger.info("===========================")

duplicates_statistics_file = "duplicate_statistics.json"

def save_duplicate_stats(duplicates_directory, duplicate_count, lsh_threshold):
    stats_file_path = os.path.join(duplicates_directory, duplicates_statistics_file)
    stats = {"Data": "Pile duplicate statistics",
             "Total Duplicates": duplicate_count,
             "lsh_threshold": lsh_threshold
             }
    json.dump(stats, open(stats_file_path, "w"))


def get_duplicate_stats(duplicates_directory):
    stats_file_path = os.path.join(duplicates_directory, duplicates_statistics_file)

    if os.path.exists(stats_file_path):
        stats = json.load(open(stats_file_path, "r"))
        return stats
    else:
        logger.error(f"'{stats_file_path}' not found.")

parser = argparse.ArgumentParser(description='In memory Pile dedupe using minhash lsh.')
parser.add_argument("-dir", "--duplicates_directory", default="pile_duplicates")
parser.add_argument("--inspect_duplicates", action="store_true")

if __name__ == "__main__":
    args = parser.parse_args()

    setup_logger_tqdm()
    logger.info("Duplicate Statistics".center(40, "="))

    stats = get_duplicate_stats(args.duplicates_directory)

    total_duplicates = stats['Total Duplicates']
    logger.info(f"Total Duplicates: {total_duplicates}")

    pile_statistics = get_pile_statistics()
    document_count = pile_statistics["Document Count"]

    percent_remaining = ((document_count - total_duplicates) / document_count) * 100
    logger.info(f"Total Original Documents: {document_count}")
    logger.info(f"Remaining: {percent_remaining:0.2f}%")

    if args.inspect_duplicates:
        inspect_duplicates(args.duplicates_directory)
