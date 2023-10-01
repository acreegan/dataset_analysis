import argparse
import tkinter as tk
from tkinter.filedialog import askdirectory
import os
from pathlib import Path, PurePosixPath
from tqdm import tqdm
import pandas as pd
import zipfile
"""
Script to create an index of a folder with multiple zip files
"""


def run_cli():
    output_directory = Path("output")
    index_filename = "directory_index.csv"

    parser = argparse.ArgumentParser()
    parser.add_argument("--root", help="Root", type=str)
    args = parser.parse_args()

    if args.root is not None:
        root = Path(args.root)
    else:
        tk.Tk().withdraw()
        if not (root := askdirectory(title="Select root directory")):
            exit()
        root = Path(root)

    print(f"Indexing directory: {str(root)}")
    directory_walk = index_directory(root)

    index_path = output_directory / index_filename

    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    pd.DataFrame(directory_walk).to_csv(index_path, header=["dirpath", "dirnames", "files"], sep="\t", index=False)


def index_directory(directory, show_progress=True):
    directory_walk = []
    for i, (dirpath, dirnames, files) in tqdm(enumerate(os.walk(directory)), disable=not show_progress):
        directory_walk.append([PurePosixPath(Path(dirpath).relative_to(directory)), dirnames, files])
        for file in files:
            if Path(file).suffix == ".zip":
                zipwalk_list = list(zipwalk(Path(dirpath) / file))
                for item in zipwalk_list:
                    directory_walk.append([item[0], item[1], item[2]])

    return directory_walk


# Mimic os.walk() function for zipfiles
def zipwalk(file: Path):

    zfile = zipfile.ZipFile(str(file))
    # Initialize database
    dlistdb = {}
    # Walk through zip file information list
    for info in zfile.infolist():
        if info.is_dir():
            zpath = Path(info.filename).parent
            zfile = Path(info.filename).name
            if zpath in dlistdb:
                dlistdb[zpath][0].append(zfile)
            else:
                dlistdb[zpath] = [[zfile], []]
        else:
            zpath = Path(info.filename).parent
            zfile = Path(info.filename).name
            if zpath in dlistdb:
                dlistdb[zpath][1].append(zfile)
            else:
                dlistdb[zpath] = [[], [zfile]]

    # Convert to os.walk() output format
    dlist = []
    for key in dlistdb.keys():
        dlist.append((key, dlistdb[key][0], dlistdb[key][1]))

    return iter(dlist)

if __name__ == "__main__":
    run_cli()
