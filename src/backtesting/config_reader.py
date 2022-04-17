"""
File:           config_reader.py
Author:         Dibyaranjan Sathua
Created on:     16/04/22, 6:31 pm
"""
from typing import Dict
import json
import datetime
from pathlib import Path

from src.utils.logger import LogFacade


class CustomJSONDecoder(json.JSONDecoder):
    """ Custom JSON decoder to parse date or datetime object """
    # The keys that ends with datetime will be parsed for datetime and the keys that ends with date
    # will be parse for date


class ConfigReader:
    """ Singleton class that reads the config file and store it in memory """

    def __init__(self, config_file_path: Path):
        self._config_file_path = config_file_path
        self._logger: LogFacade = LogFacade("backtesting")
        if not self._config_file_path.is_file():
            raise FileNotFoundError(f"Config file {self._config_file_path} doesn't exist")
        with open(self._config_file_path, mode="r") as fp_:
            try:
                self._config: Dict = json.load(fp_, object_hook=self.json_object_hook)
            except json.JSONDecodeError as err:
                self._logger.error(f"Error decoding config file")
                self._logger.error(err)

    def __getitem__(self, item: str):
        return self._config[item]

    def __contains__(self, item: str):
        return item in self._config

    def get(self, item: str, default=None):
        return self._config.get(item, default)

    @staticmethod
    def json_object_hook(input_dict: Dict):
        """ Look for specific keys and convert them to python datetime object.
        This will be called for each dict type structure in json. If JSON file has a list of dicts,
        then it will be called for each dict. The return value will be used instead of the
        decoded dict.
        """
        output_dict = dict()
        for key, value in input_dict.items():
            if key.endswith("datetime"):
                output_dict[key] = datetime.datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
            elif key.endswith("date"):
                output_dict[key] = datetime.datetime.strptime(value, "%Y-%m-%d").date()
            else:
                output_dict[key] = value
        return output_dict
