import argparse
import tkinter as tk
from tkinter.filedialog import askopenfilename
from pathlib import Path
import matplotlib.pyplot as plt

import pandas as pd

"""
Script to analyse copdgene subject data
"""


def run_cli():
    output_directory = Path("output")

    parser = argparse.ArgumentParser()
    parser.add_argument("--data_file", help="Subject data file for COPDGene dataset", type=str)
    parser.add_argument("--data_dict", help="Subject data dict", type=str)
    parser.add_argument("--use_subject_list", help="Use sub list of subjects", default=False,
                        action=argparse.BooleanOptionalAction)
    parser.add_argument("--subject_list", help="List of subjects to analyse", type=str)
    args = parser.parse_args()

    use_subject_list = False
    if args.subject_list is not None and args.use_subject_list:
        use_subject_list = True

    if args.data_file is not None:
        data_file = args.data_file
    else:
        tk.Tk().withdraw()
        if not (data_file := askopenfilename(title="Select subject data file")):
            exit()

    if args.data_dict is not None:
        data_dict_file = args.data_dict
    else:
        tk.Tk().withdraw()
        if not (data_dict_file := askopenfilename(title="Select subject data file")):
            exit()

    if use_subject_list and args.subject_list is not None:
        subject_list_file = args.subject_list
        subject_list = pd.read_csv(subject_list_file)
    elif use_subject_list:
        tk.Tk().withdraw()
        if not (subject_list_file := askopenfilename(title="Select subject list file")):
            exit()
        subject_list = pd.read_csv(subject_list_file)

    data = pd.read_csv(data_file, sep="\t")

    fig_title = "COPD Gene Study Participant Statistics\n"
    if use_subject_list:
        data = data.loc[data["sid"].isin(subject_list["sid"])]
        fig_title = f"COPD Gene Study Participant Statistics, Subjects: {str(Path(subject_list_file).stem)}\n"

    data_dict = pd.read_excel(data_dict_file)
    data_dict = data_dict.set_index("VariableName")

    # Race
    race_counts = data["race"].value_counts()
    race_vals = data_dict["CodedValues"]["race"]
    race_mapping = parse_discrete(race_vals)

    # Gold Stage
    gold_counts = data["finalgold_baseline"].value_counts()
    gold_vals = data_dict["CodedValues"]["finalgold_baseline"]
    gold_mapping = parse_discrete(gold_vals)

    # Gender
    gender_counts = data["gender"].value_counts()
    gender_vals = data_dict["CodedValues"]["gender"]
    gender_mapping = parse_discrete(gender_vals)

    fig, axs = plt.subplots(2, 3, layout="constrained")
    axs[0, 0].bar([gold_mapping[i].split("(")[0] for i in gold_counts.index], gold_counts)
    axs[0, 0].set_ylabel("Number of Participants")
    axs[0, 0].set_title("Participants by COPD Gold Stage")
    axs[0, 0].set_xticks(range(len(axs[0, 0].get_xticks())), axs[0, 0].get_xticklabels(), rotation=45)

    axs[0, 1].bar([race_mapping[i] for i in race_counts.index], race_counts)
    axs[0, 1].set_ylabel("Number of Participants")
    axs[0, 1].set_title("Participants by Race")

    axs[0, 2].bar([gender_mapping[i] for i in gender_counts.index], gender_counts)
    axs[0, 2].set_ylabel("Number of Participants")
    axs[0, 2].set_title("Participants by Gender")

    axs[1, 0].hist(data["age_baseline"], bins=int((max(data["age_baseline"]) - min(data["age_baseline"])) / 2.5))
    axs[1, 0].set_ylabel("Number of Participants")
    axs[1, 0].set_xlabel(f"Age ({data_dict['Units']['age_baseline']})")
    axs[1, 0].set_title(f"Age at Baseline ({data_dict['Units']['age_baseline']})")

    axs[1, 1].hist(data["Height_CM"], bins=int((max(data["Height_CM"]) - min(data["Height_CM"])) / 2.5))
    axs[1, 1].set_ylabel("Number of Participants")
    axs[1, 1].set_xlabel(f"Height ({data_dict['Units']['Height_CM']})")
    axs[1, 1].set_title(f"Height ({data_dict['Units']['Height_CM']})")

    axs[1, 2].hist(data["Weight_KG"], bins=int((max(data["Weight_KG"]) - min(data["Weight_KG"])) / 2.5))
    axs[1, 2].set_ylabel("Number of Participants")
    axs[1, 2].set_xlabel(f"Weight ({data_dict['Units']['Weight_KG']})")
    axs[1, 2].set_title(f"Weight ({data_dict['Units']['Weight_KG']})")

    fig.set_size_inches(15, 6)
    fig.suptitle(fig_title)

    plt.show()


def parse_discrete(coded_values: str):
    pairs = coded_values.split(" | ")
    mapping = {int(s[0]): s[1] for s in [pair.split("=") for pair in pairs]}
    return mapping


if __name__ == "__main__":
    run_cli()
