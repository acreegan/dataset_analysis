import argparse
import tkinter as tk
from tkinter.filedialog import askdirectory
import os
from pathlib import Path, PurePosixPath
from tqdm import tqdm
import zipfile
from multiprocessing import pool
import fnmatch
import functools

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

    print(f"Extracting zip files in: {zip_root}\nto {output_root}")
    extract_zip_directory(zip_root, output_root, pool.Pool())
    # extract_zip_directory(zip_root, output_root, mpool=None)


def extract_zip_directory(zip_root: Path, output_root: Path, mpool: pool.Pool):
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

    succeeded_files = extract_zip_files(zip_root, output_root, directory_index, mpool)
    failed_files = [file for suceeded, file in succeeded_files if not suceeded]

    if failed_files:
        print(f"Failed to extract: {failed_files}")
    else:
        print("All files extracted successfully")


def extract_zip_files(zip_root: Path, output_root: Path, directory_index, mpool: pool.Pool):
    """
    Extraction task for workflow. Flatten file list then distribute them to the worker function

    Parameters
    ----------
    zip_root
    output_root
    directory_index
    mpool

    Returns
    -------

    """
    # Get a flat list of all the zip files
    files_list = []  # List of tuples with zip path and output path
    for dirpath, _, files in directory_index:
        if len(zip_files := fnmatch.filter(files, "*.zip")) > 0:
            for zip_file in zip_files:
                files_list.append((zip_root / dirpath / zip_file, output_root / dirpath))

    if mpool is None:
        succeeded = []
        for zip_file, output_directory in tqdm(files_list, desc="Extracting files"):
            s = extract_zip_file((zip_file, output_directory))
            succeeded.append(s)
    else:
        succeeded = tqdm(mpool.imap(extract_zip_file, files_list), desc="Extracting files", total=len(files_list))

    return zip(succeeded, files_list)


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
    succeeded = False,
    with zipfile.ZipFile(zip_file_path, 'r') as zip_file:
        try:
            zip_file.extractall(str(output_directory))
            succeeded = True
        except Exception as e:
            pass

    return succeeded


def index_directory(directory, show_progress=True):
    directory_walk = []
    for i, (dirpath, dirnames, files) in tqdm(enumerate(os.walk(directory)), desc="Indexing directory", disable=not show_progress):
        directory_walk.append([PurePosixPath(Path(dirpath).relative_to(directory)), dirnames, files])

    return directory_walk


if __name__ == "__main__":
    run_cli()
