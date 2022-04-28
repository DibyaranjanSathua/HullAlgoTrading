"""
File:           database_import.py
Author:         Dibyaranjan Sathua
Created on:     26/04/22, 10:10 pm
"""
import argparse
from pathlib import Path

from src.utils.csv2db import CSV2DB


def database_import():
    """ Import data from csv to postgres """
    parser = argparse.ArgumentParser()
    parser.add_argument("--top-dir", type=str, required=False, help="Top dir path")
    parser.add_argument("--csv-file", type=str, required=False, help="csv file path")
    args = parser.parse_args()
    if args.top_dir:
        CSV2DB().process_top_level_directory(Path(args.top_dir))
    elif args.csv_file:
        CSV2DB().process_csv_file(Path(args.csv_file))


if __name__ == "__main__":
    database_import()
