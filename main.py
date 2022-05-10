"""
File:           main.py
Author:         Dibyaranjan Sathua
Created on:     16/04/22, 12:38 pm
"""
import argparse
from src.backtesting.hull_ma_strategy import HullMABackTesting
from src.strategies.hull_ma_strategy import HullMATradingStrategy


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=str, help="Config file path")
    parser.add_argument("--trading", action="store_true")
    args = parser.parse_args()
    # config_file_path: str = "/Users/dibyaranjan/Upwork/client_arun_algotrading/HullAlgoTrading/" \
    #                         "data/config.json"
    if args.trading:
        HullMATradingStrategy().execute()
    else:
        HullMABackTesting(config_file_path=args.config).execute()


if __name__ == "__main__":
    main()
