"""
This module provides helper functions for working with the Pile minhashes. 

It's runnable for verifying the minhashes.

Before using this script be sure to place the downloaded pile in the "pile" directory or use a symlink.

Relevant Functions
------------------
yield_minhashes(minhashes_directory)
    Returns a generator for minhashes.

Arguments
---------
--generated_minhashes (-dir)
    Location of the minhash files to verify. Default: "generated_minhashes"
"""

import os
import pickle
import re
import argparse
import glob
import random

import tqdm

from working_with_pile import yield_pile
from working_with_pile import get_pile_statistics
from generate_minhashes import generate_minhash

import logging
from logger import setup_logger_tqdm
logger = logging.getLogger(__name__)

def yield_minhashes(minhashes_directory):
    query = os.path.join(minhashes_directory, "*minhashes_*.pkl")
    minhash_files = list(glob.glob(query)) 
    minhash_files.sort(key=lambda x: int(re.search(r'minhashes_(\d+)\.pkl', x).group(1)))

    for file in minhash_files:
        data = pickle.load(open(file, "rb"))
        for offset, minhash in data:
            yield (offset, minhash)


def verify_minhashes(minhashes_directory):
    logger.info("Verifying minhashes offsets and a small sample of recalculations.")

    logger.info("Counting minhashes...")
    minhashes_count = sum(1 for _ in yield_minhashes(minhashes_directory))
    logger.info(f"Minhashes found: {minhashes_count}")

    if minhashes_count < 10000:
        logger.info("Less than 10,000 minhashes found, exiting.")
        return

    pile_stats = get_pile_statistics()
    if minhashes_count != pile_stats["Document Count"]:
        logger.warning(f"Warning: There are less minhashes than Pile documents ({pile_stats['Document Count']})")
        logger.warning("This is ok if you are only verifying a partial set of minhashes.")

    previous_offset = -1
    sample_count = 10000
    random_samples = iter(sorted(random.sample(range(0, minhashes_count), sample_count)))
    next_sample = next(random_samples)
    pile_gen = yield_pile()
    with tqdm.tqdm(total=minhashes_count, dynamic_ncols=True, unit="docs", miniters=100000) as progress:    
        for offset, minhash in yield_minhashes(minhashes_directory):
            assert(offset == previous_offset + 1)
            previous_offset = offset

            ground_truth_offset, document = next(pile_gen)
            assert(offset == ground_truth_offset)

            if offset == next_sample:
                logger.info(f"Verifying offset {offset}")
                ground_truth_minhash = generate_minhash(document, None, None)
                assert(minhash == ground_truth_minhash)
                try:
                    next_sample = next(random_samples)                    
                except StopIteration:
                    logger.info("Sampling complete.")

            progress.update()

    logger.info("Minhashes verified")

parser = argparse.ArgumentParser(description='Generate minhashes for The Pile.')
parser.add_argument("-dir", "--minhashes_directory", default="generated_minhashes")

if __name__ == '__main__':
    setup_logger_tqdm()
    logger.info("Running version 1.00")

    args = parser.parse_args()
    verify_minhashes(args.minhashes_directory)
