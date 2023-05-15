"""
Dedupes the Pile using an LSH built with the provided minhashes. Duplicates are output as pickle dumps of lists in
the format [(offset, duplicate_offsets),...]. There's also a corresponding duplicates_smol output where the duplicate_offsets
are not included, only the document offset - for when more speed/less storage is desired later.

Before using this script be sure to generate the minhashes using generate_minhashes.py. If you use the default
directory in that script, it will automatically work in this script, otherwise specify the minhashes directory manually.

Arguments
---------
--minhashes_directory (-in_dir)
    Location of the minhash files to use for deduping. Default: "generated_minhashes"
--duplicates_directory (-out_dir)
    Location of the output duplicates pickle files to use for deduping. Default: "pile_duplicates"
--lsh_threshold
    Threshold when finding matches in the LSH, higher means LESS deduplication. Default: 0.5
"""
import argparse
import os
import pickle
import glob
import gc
from functools import partial
import random
import shutil
import json

from datasketch import MinHashLSH
import tqdm

import logging
from logger import setup_logger_tqdm
logger = logging.getLogger(__name__)

from pathlib import Path

from working_with_pile import get_pile_statistics
from working_with_minhashes import yield_minhashes
from working_with_duplicates import save_duplicate_stats

def ensure_disk_space():
    usage = shutil.disk_usage(".")
    assert((usage.free / usage.total) > 0.05)

def load_or_create_lsh(lsh_path, lsh_threshold, document_count, minhashes_directory):
    if os.path.exists(lsh_path):
        logger.info("Loading lsh from pickle...")
        lsh = pickle.load(open(lsh_path, "rb"))
        logger.info("LSH load complete.")            
        return lsh

    logger.info(f"Building LSH, threshold: {lsh_threshold}")
    lsh = MinHashLSH(lsh_threshold, num_perm=10)

    with tqdm.tqdm(total=document_count, dynamic_ncols=True, unit="docs", miniters=100000) as progress:
        for offset, minhash in yield_minhashes(minhashes_directory):
            lsh.insert(offset, minhash)
            progress.update()

    logger.info("Dumping LSH")
    pickle.dump(lsh, open(lsh_path, "wb"))
    return lsh

def save_duplicate_batch(duplicates_directory, duplicates, batch_number):
    logger.info(f"Dumping duplicates for batch {batch_number}. Duplicates found: {len(duplicates)}")
    duplicates_file = os.path.join(duplicates_directory, f"duplicates_{batch_number:04d}.pkl")
    pickle.dump(duplicates, open(duplicates_file, "wb"))

    duplicates_smol = list(map(lambda x: x[0], duplicates))
    duplicates_smol_file = os.path.join(duplicates_directory, f"duplicates_smol_{batch_number:04d}.pkl")
    pickle.dump(duplicates_smol, open(duplicates_smol_file, "wb"))
    duplicates_smol = []


def main(minhashes_directory, duplicates_directory, lsh_threshold):

    os.makedirs(duplicates_directory, exist_ok=True)

    done_file = os.path.join(duplicates_directory, "dedupe.done")
    if os.path.exists(done_file):
        logger.info("Dedupe already completed.")
        return

    pile_statistics = get_pile_statistics()

    lsh_path = os.path.join(duplicates_directory, "lsh.pkl")
    lsh = load_or_create_lsh(lsh_path, lsh_threshold, pile_statistics["Document Count"], minhashes_directory)

    save_frequency = 1000000

    duplicates = []
    batch_number = 0
    duplicate_count = 0
    with tqdm.tqdm(total=pile_statistics["Document Count"], dynamic_ncols=True, unit="docs", miniters=100000) as progress:    
        for offset, minhash in yield_minhashes(minhashes_directory):
            progress.update()

            results = lsh.query(minhash)
            for found_offset in results:
                if found_offset != offset:
                    duplicates.append((offset, results))
                    duplicate_count += 1
                    lsh.remove(offset) # Only remove self
                    break 

            if len(duplicates) == save_frequency:   
                save_duplicate_batch(duplicates_directory, duplicates, batch_number)
                batch_number += 1;
                duplicates = []

                ensure_disk_space()

        # Dump final batch
        if len(duplicates) != 0:
            save_duplicate_batch(duplicates_directory, duplicates, batch_number)

    save_duplicate_stats(duplicates_directory, duplicate_count, lsh_threshold)

    Path(done_file).touch()    

parser = argparse.ArgumentParser(description='In memory Pile dedupe using minhash lsh.')
parser.add_argument("-in_dir", "--minhashes_directory", default="generated_minhashes")
parser.add_argument("-out_dir", "--duplicates_directory", default="pile_duplicates")
parser.add_argument("--lsh_threshold", type=float, default=0.5)

if __name__ == '__main__':
    logfile_path = "dedupe.log"
    setup_logger_tqdm(logfile_path)

    logger.info("Running version 1.11")

    args = parser.parse_args()
    assert(args.lsh_threshold > 0 and args.lsh_threshold < 1)

    main(args.minhashes_directory, args.duplicates_directory, args.lsh_threshold)

