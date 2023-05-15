"""
This script generates a LeanMinHash for each pile document, which is saved along with the 
document offset into a series of files. Multiprocessing is supported through tqdm-multiprocess. 
For the purposes of output we do batches of 100,000 documents. Each document in the batch is processed as its own
task (different pool process).

Before using this script be sure to place the downloaded pile in the "pile" directory or use a symlink.

Arguments
---------
--working_directory (-dir)
    Where to save the files containing LeanMinHash and document offsets. Also used for checkpointing.
    Default: "generated_minhashes"
--process_count (-procs)
    Number of processes to use when processing a batch.
    Default: 4
--backup_dir (-dir) OPTIONAL
    If specified the working directory will be rsync'd to this location after each batch. Useful with flaky compute/
    non-persistent containers.
"""

import argparse
import os
import glob
import pickle
import signal
import sys
import math
from pathlib import Path
from signal import SIGINT, SIG_IGN

import nltk
nltk.download('punkt')
from nltk.util import ngrams
from datasketch import MinHash, LeanMinHash
import tqdm
from tqdm_multiprocess import TqdmMultiProcessPool

from working_with_pile import yield_pile, get_pile_statistics

import logging
from logger import setup_logger_tqdm
logger = logging.getLogger(__name__)

megabyte = 1024 * 1024

def extract_ngrams(data, num):
    n_grams = ngrams(nltk.word_tokenize(data), num)
    return [ ' '.join(grams) for grams in n_grams]

def slice_document(document):
    if len(document) > megabyte:
        slice_size = megabyte
        num_slices = math.ceil(len(document) / slice_size)
        slices = []
        for i in range(num_slices):
            start_index = slice_size * i
            end_index = min(start_index + slice_size, len(document))
            slices.append(document[start_index:end_index])
        return slices
    else:
        return [document]

def generate_minhash(document, tqdm_func, global_tqdm):
    try:
        slices = slice_document(document)
        five_gram_set = set()
        for a_slice in slices:
            n_grams = extract_ngrams(a_slice, 5)
            five_gram_set.update(n_grams)
        minhash = MinHash(num_perm=10)
        for five_gram in five_gram_set:
            minhash.update(five_gram.encode('utf8'))

        return LeanMinHash(minhash)
    except Exception as ex:
        logger.info(f"Generate minhash failure: {ex}. Doc length: {len(document)}")
        return None

def process_batch(pool, progress, batch, working_directory, backup_dir):
    checkpoint_file = os.path.join(working_directory, "checkpoint.pkl")
    checkpoint_temp_file = os.path.join(working_directory, "checkpoint_temp.pkl")
    checkpoint_old_file = os.path.join(working_directory, "checkpoint_old.pkl")    
    transaction_lock = os.path.join(working_directory, ".transaction_lock")

    # Generate minhash tasks
    tasks = []
    for offset, document in batch:
        task = (generate_minhash, (document,))
        tasks.append(task)

    def on_error(result):
        logger.info("error :(")
        sys.exit("Brokened")

    def on_done(result):
        pass

    minhashes = pool.map(progress, tasks, on_error, on_done)

    # Commence Transaction
    previous_signal_int = signal.signal(SIGINT, SIG_IGN)
    Path(transaction_lock).touch()
    start_offset = batch[0][0]
    last_offset = batch[-1][0]

    # Dump Minhashes
    minhashes_and_meta = []
    for i, minhash in enumerate(minhashes):
        offset, document = batch[i]
        minhashes_and_meta.append((offset, minhash))

    minhashes_file = os.path.join(working_directory, f"minhashes_{start_offset}.pkl")
    pickle.dump(minhashes_and_meta, open(minhashes_file, "wb"))

    # Dump Checkpoint                
    pickle.dump(last_offset, open(checkpoint_temp_file, "wb"))

    # Move stuff around safely in case of failure
    if os.path.exists(checkpoint_file):
        os.rename(checkpoint_file, checkpoint_old_file)
    os.rename(checkpoint_temp_file, checkpoint_file)

    # Transaction Finished
    os.remove(transaction_lock)
    signal.signal(SIGINT, previous_signal_int)

    # Extra Backup
    if backup_dir:
        logger.info("rsyncing to backup drive")
        os.system(f"rsync -avzl --info=progress2 {working_directory} {backup_dir}")
        logger.info("rsyncing complete")


def main(working_directory, process_count, backup_dir):
    if not os.path.exists("pile"):
        logger.error("'pile' directory not found, please either use a symlink or place the downloaded pile in the 'pile' directory.")

    pile_statistics = get_pile_statistics()
    document_count = pile_statistics["Document Count"]

    logger.info(f"Total documents in dataset: {document_count:,}")

    checkpoint_file = os.path.join(working_directory, "checkpoint.pkl")
    checkpoint_temp_file = os.path.join(working_directory, "checkpoint_temp.pkl")
    checkpoint_old_file = os.path.join(working_directory, "checkpoint_old.pkl")    
    transaction_lock = os.path.join(working_directory, ".transaction_lock")

    if os.path.exists(transaction_lock):
        logger.info("Program crashed during transaction, fixing files...")
        # Just re-do from last safe checkpoint (overwrite minhashes)
        if os.path.exists(checkpoint_temp_file):
            if os.path.exists(checkpoint_old_file):
                os.rename(checkpoint_old_file, checkpoint_file)
            os.remove(checkpoint_temp_file)
        else:
            pass

        os.remove(transaction_lock)
    
    if os.path.exists(checkpoint_file):
        checkpoint_offset = pickle.load(open(checkpoint_file, "rb")) + 1
        logger.info(f"Checkpoint found, starting from offset {checkpoint_offset:,}")            
    else:
        checkpoint_offset = 0
        logger.info(f"No checkpoint found, starting from beginning.")        

    batch_size = 100000
    batch = []
    pool = TqdmMultiProcessPool(process_count)

    iterate = False
    if checkpoint_offset != 0:
        iterate = True

    with tqdm.tqdm(total=checkpoint_offset, dynamic_ncols=True, unit="docs", miniters=10000) as progress:
        for offset, document in yield_pile(checkpoint_offset):
            if iterate:
                # yield_pile can skip earlier processed files
                logger.info(f"Iterating to offset {checkpoint_offset} from {offset}")                
                progress.update(offset)
                iterate = False

            if offset < checkpoint_offset:
                progress.update()
                continue

            if offset == checkpoint_offset:
                progress.reset(total=document_count)
                progress.update(checkpoint_offset)

            batch.append((offset, document))

            if len(batch) == batch_size:
                process_batch(pool, progress, batch, working_directory, backup_dir)
                batch = []
                progress.update(batch_size)

        if len(batch) != 0:
            process_batch(pool, progress, batch, working_directory, backup_dir)
            progress.update(len(batch))

parser = argparse.ArgumentParser(description='Generate minhashes for The Pile.')
parser.add_argument("-dir", "--working_directory", default="generated_minhashes")
parser.add_argument("-procs", "--process_count", type=int, default=4)
parser.add_argument("--backup_dir", default="")

if __name__ == '__main__':
    logfile_path = "generate_minhashes.log"
    setup_logger_tqdm(logfile_path)

    logger.info("Running version 1.02")

    args = parser.parse_args()
    os.makedirs(args.working_directory, exist_ok=True)    
    main(args.working_directory, args.process_count, args.backup_dir)