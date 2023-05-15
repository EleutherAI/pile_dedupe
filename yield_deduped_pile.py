"""
This module provides a single helper function to get the deduped pile.

It's runnable to simulate yielding the deduped pile.

Relevant Functions
------------------
yield_deduped_pile(minhashes_directory)
    Returns a generator for the Pile with duplicates removed.

Arguments
---------
--duplicates_directory (-dupe_dir)
    Location of the duplicates files from the dedupe step. Default: "pile_duplicates"
"""
import pickle
import tqdm
import argparse

from working_with_pile import yield_pile
from working_with_duplicates import get_duplicate_stats, yield_duplicates

import logging
from logger import setup_logger_tqdm
logger = logging.getLogger(__name__)

def yield_deduped_pile(pile_directory, duplicates_directory):
    duplicate_stats = get_duplicate_stats()
    total_duplicates = duplicate_stats['Total Duplicates']
    logger.info(f"LSH Dedupe Threshold: {duplicate_stats['lsh_threshold']}")
    logger.info(f"Total Duplicates: {total_duplicates}")

    pile_statistics = pickle.load(open("pile_statistics.pkl", "rb"))
    document_count = pile_statistics["Document Count"]

    percent_remaining = ((document_count - total_duplicates) / document_count) * 100
    logger.info(f"Total Original Documents: {document_count}")
    logger.info(f"Remaining: {percent_remaining:0.2f}%")

    progress = tqdm.tqdm(total=document_count, unit="documents", unit_scale=1, 
                         dynamic_ncols=True, mininterval=1)
    duplicates = yield_duplicates(duplicates_directory)
    current_duplicate = next(duplicates)
    for global_offset, document in yield_pile():
        progress.update()
        if current_duplicate == global_offset:
            try:
                current_duplicate = next(duplicates)
            except StopIteration:
                current_duplicate = None
        else:
            yield (global_offset, document)

    progress.close()

parser = argparse.ArgumentParser(description='Yield deduped pile.')
parser.add_argument("-dupe_dir", "--duplicates_directory", default="pile_duplicates")
parser.add_argument("-pile_dir", "--pile_directory", default="pile")

if __name__ == '__main__':
    setup_logger_tqdm()
    args = parser.parse_args()

    yielded_documents = 0
    for global_offset, document in yield_deduped_pile(args.pile_directory, args.duplicates_directory):
        yielded_documents += 1

    logger.info(f"Total Yielded Documents: {yielded_documents}")