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
    parser.add_argument("--top-dir", type=str, required=True, help="Top dir path")
    args = parser.parse_args()
    CSV2DB().process_top_level_directory(Path(args.top_dir))


if __name__ == "__main__":
    database_import()
