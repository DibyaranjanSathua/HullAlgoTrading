"""
File:           main.py
Author:         Dibyaranjan Sathua
Created on:     16/04/22, 12:38 pm
"""
import argparse
from src.backtesting.hull_ma_strategy import HullMABackTesting


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=str, required=True, help="Config file path")
    args = parser.parse_args()
    # config_file_path: str = "/Users/dibyaranjan/Upwork/client_arun_algotrading/HullAlgoTrading/" \
    #                         "data/config.json"
    HullMABackTesting(config_file_path=args.config).execute()


if __name__ == "__main__":
    main()
