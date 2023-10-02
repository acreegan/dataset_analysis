import argparse
import tkinter as tk
from tkinter.filedialog import askopenfilename
from pathlib import Path
import matplotlib.pyplot as plt
from tqdm import tqdm
import fnmatch
import pandas as pd
from multiprocessing import pool
import functools
import numpy as np

"""
Script to analyse copdgene subject dataset zip index
"""


def run_cli():
    output_directory = Path("output")

    search_phase = "COPD1"
    search_inex = "EXP"

    parser = argparse.ArgumentParser()
    parser.add_argument("--data_file", help="Subject data file for COPDGene dataset", type=str)
    args = parser.parse_args()

    if args.data_file is not None:
        data_file = args.data_file
    else:
        tk.Tk().withdraw()
        if not (data_file := askopenfilename(title="Select dataset zip index file")):
            exit()

    data = pd.read_csv(data_file, sep="\t")

    mpool = pool.Pool()

    # filter = list(tqdm(mpool.imap(any_zips_or_lobes, data["files"]), desc="Filtering data", total=len(data["files"])))
    # data = data[filter]
    # data.to_csv(str(output_directory / f"{Path(data_file).stem}_filtered.csv"), sep="\t")

    # Get all subjects
    # subjects = list(
    #     tqdm(mpool.imap(find_subjects, data["files"]), desc="Finding all subjects", total=len(data["files"])))
    subjects = list(
        tqdm([find_subjects(files) for files in data["files"]], desc="Finding all subjects", total=len(data["files"])))
    unique_subjects = list(set(flatten(subjects)))
    subject_dict = {s: 0 for s in unique_subjects}

    # subjects_segmentations = list(
    #     tqdm(mpool.imap(find_lobe_segmentations, data["files"]), desc="Finding subjects with lobe segmentations",
    #          total=len(data["files"])))
    subjects_segmentations = list(
        tqdm([find_lobe_segmentations((files, dirpath, search_phase, search_inex)) for files, dirpath in
              zip(data["files"], data["dirpath"])], desc="Finding subjects with lobe segmentations",
             total=len(data["files"])))
    unique, counts = np.unique(flatten(subjects_segmentations), return_counts=True)

    for unique, counts in zip(unique, counts):
        subject_dict[unique] = counts

    subject_dict_df = pd.DataFrame.from_dict(subject_dict, orient="index")
    print(subject_dict_df.value_counts())

    subject_dict_df.to_csv(str(output_directory / "subject_dict_df.csv"))


def flatten(l):
    return [item for sublist in l for item in sublist]


def args_unpacker(func):
    """Decorator for multiprocessing methods."""

    def unpack(args):
        return func(*args)

    functools.update_wrapper(unpack, func)
    return unpack


def any_zips_or_lobes(files):
    match = False
    files = files.split("'")
    if len(fnmatch.filter(files, "*.zip")) > 0 or len(fnmatch.filter(files, "*Lobes.mhd")) > 0:
        match = True

    return match


def find_subjects(files):
    subjects = []
    files = files.split("'")
    if len(zip_files := fnmatch.filter(files, "*.zip")) > 0:
        for file in zip_files:
            subject = file.strip(".zip")
            subjects.append(subject)

    return subjects


@args_unpacker
def find_lobe_segmentations(files, dirpath, search_phase, search_inex):
    subjects = []
    files = files.split("'")
    if len(lobe_files := fnmatch.filter(files, "*Lobes.mhd")) > 0:
        for file in lobe_files:
            subject = file.split("_")[0]
            phase = Path(dirpath).parents[0].name
            inex = file.split("_")[1]
            if search_phase is not None and phase != search_phase:
                continue
            if search_inex is not None and inex != search_inex:
                continue
            subjects.append(subject)
    return subjects


if __name__ == "__main__":
    run_cli()
