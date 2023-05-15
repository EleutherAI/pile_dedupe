# Pile Dedupe

## Prerequisites

Download the [Pile distribution](https://the-eye.eu/public/AI/pile/). Relevant files are in train.

## Install

```
git clone https://github.com/EleutherAI/pile_dedupe.git
pip install -r requirements.txt
ln -s PILE_LOCATION pile
```

## Usage

| Step | Overview | Details |
| ---- | ------- | ---------|
| 1 | Prerequisites | Download the pile. |
| 2 | Install | Clone the repo, install requirements, symlink to the location of the downloaded train directory |
| 3 | Generate Minhashes | `python generate_minhashes.py --process_count PROCESS_COUNT` Recommend one process per logical core. |
| 4 | Verify Minhashes (Optional) | `python working_with_minhashes.py` |
| 5 | Dedupe Pile | `python dedupe.py --lsh_threshold LSH_THRESHOLD` It's fairly safe to leave lsh_threshold default (0.5) if you don't mind a bit of extra dedupe. |
| 6 | Inspect duplicates |  `python working_with_duplicates.py --inspect_duplicates` |

## I'm Done - Give Me A Generator

```python
from yield_deduped_pile import yield_deduped_pile

pile_directory = "pile"
duplicates_directory = "pile_duplicates"
yield_deduped_pile(pile_directory, duplicates_directory)
```

## Further Documentation
Each file is described at the top.
