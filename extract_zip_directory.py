import argparse
import tkinter as tk
from tkinter.filedialog import askdirectory, askopenfilename
import os
from pathlib import Path, PurePosixPath
from tqdm import tqdm
import zipfile
from multiprocessing import pool
import fnmatch
import functools
import numpy as np

"""
Script to extract all zip files in a directory
"""


def run_cli():
    """
    Run command line interface
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--zip_root", help="Directory containing zip files", type=str)
    parser.add_argument("--output_root", help="Directory in which to place extracted files", type=str)
    parser.add_argument("--select_files", help="csv containing list of files to select", type=str)
    parser.add_argument("--skip_files", help="csv containing list of files to skip", type=str)
    args = parser.parse_args()

    if args.zip_root is not None:
        zip_root = Path(args.zip_root)
    else:
        tk.Tk().withdraw()
        if not (zip_root := askdirectory(title="Select zip_root directory")):
            exit()
        zip_root = Path(zip_root)

    if args.output_root is not None:
        output_root = Path(args.output_root)
    else:
        tk.Tk().withdraw()
        if not (output_root := askdirectory(title="Select output_root directory")):
            exit()
        output_root = Path(output_root)

    if args.select_files is not None:
        select_files = np.loadtxt(Path(args.select_files),dtype=str, delimiter=",")
    else:
        tk.Tk().withdraw()
        if not (select_files := askopenfilename(title="Select select files list (or cancel to continue)")):
            select_files = None
        else:
            select_files = np.loadtxt(Path(select_files),dtype=str, delimiter=",")

    if args.skip_files is not None:
        skip_files = np.loadtxt(Path(args.skip_files),dtype=str, delimiter=",")
    else:
        tk.Tk().withdraw()
        if not (skip_files := askopenfilename(title="Select skip files list (or cancel to continue)")):
            skip_files = None
        else:
            skip_files = np.loadtxt(Path(skip_files),dtype=str, delimiter=",")

    print(f"Extracting zip files in: {zip_root}\nto {output_root}")
    extract_zip_directory(zip_root, output_root, pool.Pool(), skip_files=skip_files, select_files=select_files)
    # extract_zip_directory(zip_root, output_root, mpool=None)


def extract_zip_directory(zip_root: Path, output_root: Path, mpool: pool.Pool, skip_files: list = None, select_files: list = None):
    """
    Workflow for extract zip directory. Index directory to find files first, then run extraction task

    Parameters
    ----------
    zip_root
    output_root
    mpool
    """
    # Make all the directories first so we don't crash into each other in the pool
    # Search backwards so we make deeper dirs first and can skip shallower ones
    directory_index = index_directory(zip_root)
    for dirpath, _, _ in directory_index[::-1]:
        if not os.path.exists(output_root / dirpath):
            os.makedirs(output_root / dirpath)

    results = extract_zip_files(zip_root, output_root, directory_index, mpool, skip_files=skip_files, select_files=select_files)
    failed_files = [(file, exception) for exception, file in results if exception is not None]
    succeeded_files = [str(PurePosixPath(file)) for exception, file in results if exception is None]

    if failed_files:
        print(f"Failed to extract: {failed_files}")
        fname = str(Path(__file__).parent / "test_data" / "succeeded_files.csv")
        np.savetxt(fname, np.array(succeeded_files), fmt="%s", delimiter=",")
    else:
        print("All files extracted successfully")


def extract_zip_files(zip_root: Path, output_root: Path, directory_index, mpool: pool.Pool, skip_files: list = None, select_files: list=None):
    """
    Extraction task for workflow. Flatten file list then distribute them to the worker function

    Parameters
    ----------
    zip_root
    output_root
    directory_index
    mpool
    skip
        list of zip files to skip

    Returns
    -------

    """
    if skip_files is None:
        skip_files = []
    # Get a flat list of all the zip files
    files_list = []  # List of tuples with zip path and output path
    for dirpath, _, files in directory_index:
        if len(zip_files := fnmatch.filter(files, "*.zip")) > 0:
            for zip_file in zip_files:
                if np.any([len(fnmatch.filter([zip_file], f"*{pat}*")) > 0 for pat in select_files]):
                    if zip_file not in [PurePosixPath(s).parts[-1] for s in skip_files]:
                        files_list.append((zip_root / dirpath / zip_file, output_root / dirpath))

    if mpool is None:
        results = []
        for zip_file, output_directory in tqdm(files_list, desc="Extracting files"):
            r = extract_zip_file((zip_file, output_directory))
            results.append(r)
    else:
        results_it = tqdm(mpool.imap(extract_zip_file, files_list, chunksize=2), desc="Extracting files",
                          total=len(files_list))
        results = list(results_it)

    return results


def args_unpacker(func):
    """Decorator for multiprocessing methods."""

    def unpack(args):
        return func(*args)

    functools.update_wrapper(unpack, func)
    return unpack


@args_unpacker
def extract_zip_file(zip_file_path, output_directory):
    """
    Worker function for the
    Parameters
    ----------
    zip_file_path
    output_directory

    Returns
    -------

    """
    exception = None
    try:
        with zipfile.ZipFile(zip_file_path, 'r') as zip_file:
            zip_file.extractall(str(output_directory))
    except Exception as e:
        exception = e

    return exception, zip_file_path


def index_directory(directory, show_progress=True):
    directory_walk = []
    for i, (dirpath, dirnames, files) in tqdm(enumerate(os.walk(directory)), desc="Indexing directory",
                                              disable=not show_progress):
        directory_walk.append([PurePosixPath(Path(dirpath).relative_to(directory)), dirnames, files])

    return directory_walk


if __name__ == "__main__":
    run_cli()
