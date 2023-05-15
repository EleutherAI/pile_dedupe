"""
This module provides helper functions for working with the pile.

It's also runnable in case you ever need to regenerate the pile statistics or view them on the command line.

Relevant Functions
------------------
yield_pile(checkpoint_offset=None)
    Returns a generator for pile documents starting from checkpoint_offset if supplied or the beginning otherwise.

get_pile_statistics(process_count=4):
    Returns a dictionary of the following:
    {"Data": "Pile statistics", "Document Count": 210607728, "Total Pile Characters": 421215456, "File Start Offsets": [0, 7021438, 14042822, 21066113, 28086515, 35106072, 42123306, 49145091, 56165817, 63185587, 70211208, 77234322, 84249267, 91267634, 98285983, 105305110, 112322489, 119342491, 126367373, 133389153, 140412039, 147432373, 154452516, 161470190, 168492733, 175512521, 182526939, 189547478, 196565318, 203583306]}
"""

import os
import json
from functools import reduce
import glob
import argparse

import tqdm
from tqdm_multiprocess import TqdmMultiProcessPool
from archiver import Reader

import logging
from logger import setup_logger_tqdm
logger = logging.getLogger(__name__)

def get_pile_files():
    directory = "pile"
    files = list(sorted(glob.glob(os.path.join(directory, "*.jsonl.zst*"))))
    return files

# Multiprocessed
def get_file_stats(file_path, tqdm_func, global_tqdm):
    reader = Reader()
    total_documents = 0
    total_size = 0
    update_frequency = 10000
    current_file_position = 0

    with tqdm_func(total=os.path.getsize(file_path), dynamic_ncols=True, unit="byte", unit_scale=1) as progress:
        for document in reader.read(file_path, get_meta=True):
            total_size += len(document)
            total_documents += 1

            if total_documents % update_frequency == 0:
                new_file_pos = reader.fh.tell() 
                bytes_read = new_file_pos - current_file_position
                current_file_position = new_file_pos
                progress.update(bytes_read)
                global_tqdm.update(bytes_read)

    return (total_documents, total_size)

def get_stats(process_count):
    files = get_pile_files()
    total_size_bytes = sum(map(lambda x: os.path.getsize(x), files))
    
    pool = TqdmMultiProcessPool(process_count)
    global_tqdm = tqdm.tqdm(total=total_size_bytes, dynamic_ncols=True, unit="byte", unit_scale=1)

    # Generate minhashes with pool
    tasks = [(get_file_stats, (file,)) for file in files]

    on_done = lambda _ : None
    on_error = lambda _ : None
    results = pool.map(global_tqdm, tasks, on_error, on_done)

    total_documents, total_size = reduce(lambda x, y: (x[0]+y[0],x[1]+y[1]), results)

    start_offsets = []
    current_offset = 0
    for file_document_count, _ in results:
        start_offsets.append(current_offset)
        current_offset += file_document_count

    return (total_documents, total_size, start_offsets)

def yield_pile(checkpoint_offset=None):
    stats = get_pile_statistics()
    files = get_pile_files()

    pile_global_offset = 0
    start_file = 0
    if checkpoint_offset:
        for i, start_offset in enumerate(stats["File Start Offsets"]):
            if start_offset > checkpoint_offset:
                break

            start_file = i
            pile_global_offset = start_offset

    
    for i, file in enumerate(files):
        if i < start_file:
            logger.info(f"Skipping file {file}")            
            continue
        logger.info(f"Reading from pile file: {file}")
        reader = Reader()
        for document in reader.read(file):
            yield (pile_global_offset, document)
            pile_global_offset += 1


pile_statistics_file = "pile_statistics.json"

def get_pile_statistics(process_count=4):
    if os.path.exists(pile_statistics_file):
        stats = json.load(open(pile_statistics_file, "r"))
    else:
        logger.info(f"'{pile_statistics_file}' missing, calculating statistics")

        document_count, total_document_size_chars, start_offsets = get_stats(process_count)
        stats = {"Data": "Pile statistics",
                 "Document Count": document_count,
                 "Total Pile Characters": total_document_size_chars,
                 "File Start Offsets": start_offsets
                 }
        json.dump(stats, open(pile_statistics_file, "w"))

    return stats        

parser = argparse.ArgumentParser(description='Calculate and display Pile stats.')
parser.add_argument("-procs", "--process_count", type=int, default=4)

if __name__ == '__main__':
    setup_logger_tqdm()    
    logger.info("Running version 1.02")

    args = parser.parse_args()

    stats = get_pile_statistics(args.process_count)

    logger.info(f"document_count: {stats['Document Count']}")
    logger.info(f"total_chars: {stats['Total Pile Characters']}")
    logger.info(f"start_offsets: {stats['File Start Offsets']}")
