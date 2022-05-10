"""
File:           fyers_api.py
Author:         Dibyaranjan Sathua
Created on:     05/05/22, 9:42 pm
"""
from typing import Optional, Dict, List, Any
from dataclasses import dataclass
import os
from pathlib import Path
import datetime
import string
import random
from urllib.parse import urlparse, parse_qs
import json
import threading

import requests
import pandas as pd
import numpy as np
from fyers_api import fyersModel
from fyers_api import accessToken
from fyers_api.Websocket import ws
from dotenv import load_dotenv

from src import BASE_DIR, DATA_DIR, LOG_DIR
from src.fyers.exception import FyersApiError


# Load env vars from .env
dotenv_path = BASE_DIR / 'env' / '.env'
load_dotenv(dotenv_path=dotenv_path)


class FyersApi:
    """ Class containing required methods for Fyers API request """
    BASE_URL: str = "https://api.fyers.in"
    OK: str = "ok"
    ERROR: str = "error"

    def __init__(self):
        self._user_id: str = os.getenv("FYERS_USER_ID")
        self._password: str = os.getenv("FYERS_PASSWORD")
        self._pin: str = os.getenv("FYERS_PIN")
        self._client_id: str = os.getenv("FYERS_CLIENT_ID")
        self._secret_id: str = os.getenv("FYERS_SECRET_ID")
        self._redirect_uri: str = os.getenv("FYERS_REDIRECT_URI")
        self._state: str = self._get_state_string()
        self._access_token: Optional[str] = None
        self._fyers: Optional[fyersModel.FyersModel] = None
        self._fyers_market_data: Optional[FyersMarketData] = None
        self._fyers_symbol_parser: Optional[FyersSymbolParser] = None

    def generate_auth_code(self) -> str:
        """ Get auth code neeeded to generate the access token """
        headers = {
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "en-IN,en;q=0.9"
        }
        session = requests.Session()
        session.headers.update(headers)
        # Authorize the session so that sending get request redirect url will give the access token
        payload = {
            "fy_id": self._user_id,
            "password": self._password,
            "app_id": "2",
            "imei": "",
            "recaptcha_token": ""
        }
        response = session.post(self.login_endpoint, json=payload)
        response_data = response.json()
        assert response.status_code == 200, f"Login failed.\n{response_data}"
        print("Login successfully")
        # This key will be used during verify pin API call
        request_key = response_data["request_key"]
        # Verify pin
        payload = {
            "request_key": request_key,
            "identity_type": "pin",
            "identifier": self._pin,
            "recaptcha_token": ""
        }
        response = session.post(self.verify_pin_endpoint, json=payload)
        response_data = response.json()
        assert response.status_code == 200, f"Pin verification failed.\n{response_data}"
        print("Pin verified successfully")
        access_token = response_data["data"]["access_token"]
        payload = {
            "fyers_id": self._user_id,
            "app_id": self._client_id.split("-")[0],
            "redirect_uri": self._redirect_uri,
            "appType": "100",
            "code_challenge": "",
            "state": self._state,
            "scope": "",
            "nonce": "",
            "response_type": "code",
            "create_cookie": True
        }
        headers = {"Authorization": f"Bearer {access_token}"}
        response = session.post(self.token_endpoint, headers=headers, json=payload)
        response_data = response.json()
        assert response.status_code == 308, f"Token API failed.\n{response_data}"
        print(f"Auth code generated successfully")
        # Parse the response URL
        parsed = urlparse(response_data["Url"])
        auth_code = parse_qs(parsed.query)["auth_code"].pop()
        state = parse_qs(parsed.query)["state"].pop()
        assert state == self._state, "State mismatch"
        print(auth_code)
        return auth_code

    def generate_access_token(self) -> None:
        """ Call Fyers login API to get the access token which will be used in other API """
        print("Generating access_token")
        auth_code = self.generate_auth_code()
        session = accessToken.SessionModel(
            client_id=self._client_id,
            secret_key=self._secret_id,
            redirect_uri=self._redirect_uri,
            response_type="code",
            state=self._state,
            grant_type="authorization_code"
        )
        session.set_token(auth_code)
        response = session.generate_token()
        self._access_token = response["access_token"]
        # Save the token to file
        self.write_token()

    def write_token(self) -> None:
        """ Save the token to data dir """
        data = {
            "timestamp": datetime.datetime.now().strftime("%d-%b-%Y %H:%M:%S"),
            "client_id": self._client_id,
            "access_token": self._access_token
        }
        with open(self.fyers_token_file, mode="w") as fp_:
            json.dump(data, fp_, indent=4)

    def read_token(self) -> None:
        """ Read token from file """
        print("Getting token from file")
        with open(self.fyers_token_file, mode="r") as fp_:
            data = json.load(fp_)
        self._access_token = data["access_token"]
        now = datetime.datetime.now()
        access_token_timestamp = datetime.datetime.strptime(data["timestamp"], "%d-%b-%Y %H:%M:%S")
        timedelta = now - access_token_timestamp
        # If access_token is generated 7 hrs ago, regenerate access_token
        # 1 day = 86400 secs
        if timedelta.days * 86400 + timedelta.seconds > 25200:   # 7 * 60 * 60
            print("Access token from file is expired. Generating a new token.")
            self.generate_access_token()

    def check(self) -> bool:
        """ Perform a check to see if we are able to access the profile """
        response = self._fyers.get_profile()
        if response["s"] == FyersApi.OK:
            print("Successfully connected to API")
            return True
        print("Error connecting to API")
        print(response["message"])
        if "Your token has expired" in response["message"]:
            self.generate_access_token()
            return True
        return False

    def setup_fyers_market_data(self):
        """ Setting up the market data websocket """
        print(f"Setting up market data websocket")
        print(f"Connecting to fyers market data websocket")
        self._fyers_market_data = FyersMarketData(
            client_id=self._client_id, access_token=self._access_token, log_dir=self.fyers_log_dir
        )
        self._fyers_market_data.start()

    def setup_fyers_symbol_parser(self):
        """ Setting up the symbol parser """
        print(f"Setting up symbol parser")
        self._fyers_symbol_parser = FyersSymbolParser()
        self._fyers_symbol_parser.setup()

    def setup(self):
        """ Setup access token used by other API """
        print(f"Setting up Fyers API")
        if self.fyers_token_file.is_file():
            print(f"Getting access token from {self.fyers_token_file}")
            self.read_token()
        else:
            self.generate_access_token()
        self._fyers = fyersModel.FyersModel(
            client_id=self._client_id, token=self._access_token, log_path=self.fyers_log_dir
        )
        self.check()
        self.setup_fyers_market_data()
        self.setup_fyers_symbol_parser()

    def get_market_quotes(self, symbol: str) -> Dict[str, Any]:
        """ Return the market quotes of the input symbol """
        data = {"symbols": symbol}
        response = self._fyers.quotes(data)
        assert response["s"] == FyersApi.OK, f"Error getting market quotes for {symbol}"
        return response["d"].pop()["v"]

    def get_market_depth(self, symbol: str) -> Dict[str, Any]:
        """ Return the complete market data of the symbol """
        data = {"symbol": symbol, "ohlcv_flag": "1"}
        response = self._fyers.depth(data)
        assert response["s"] == FyersApi.OK, f"Error getting market depth for {symbol}"
        return response["d"][symbol]

    @staticmethod
    def _get_state_string() -> str:
        """ Generate a random string of length 6 which will be used as state """
        return ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))

    @property
    def login_endpoint(self) -> str:
        return f"{self.BASE_URL}/vagator/v1/login"

    @property
    def verify_pin_endpoint(self) -> str:
        return f"{self.BASE_URL}/vagator/v1/verify_pin"

    @property
    def token_endpoint(self) -> str:
        return f"{self.BASE_URL}/api/v2/token"

    @property
    def data_dir(self) -> Path:
        return BASE_DIR / "data"

    @property
    def fyers_token_file(self) -> Path:
        return self.data_dir / "fyers_token.json"

    @property
    def fyers_log_dir(self) -> Path:
        return BASE_DIR / "logs"

    @property
    def fyers_market_data(self) -> Optional["FyersMarketData"]:
        return self._fyers_market_data

    @property
    def fyers_symbol_parser(self) -> Optional["FyersSymbolParser"]:
        return self._fyers_symbol_parser


@dataclass()
class MarketData:
    """ Dataclass to store market data received from websocket """
    symbol: str
    timestamp: int
    fyCode: int
    fyFlag: int
    pktLen: int
    ltp: float
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    min_open_price: float
    min_high_price: float
    min_low_price: float
    min_close_price: float
    min_volume: int
    last_traded_qty: int
    last_traded_time: int
    avg_trade_price: int
    vol_traded_today: int
    tot_buy_qty: int
    tot_sell_qty: int

    @classmethod
    def from_dict(cls, data: Dict[str, Any]):
        data.pop("market_pic")
        return cls(**data)


class FyersMarketData(threading.Thread):
    """ Singleton class for subscribing for ticker data """
    __instance: Optional["FyersMarketData"] = None
    __existing_subscribe_symbol: List = []              # Currently subscribed symbols
    # New symbols that should be subscribed to.
    # This list will be emptied once the symbols are subscribed
    # If no symbol subscribed, then the websocket will close.
    __symbols_to_subscribe: List = ["NSE:NIFTY50-INDEX", "NSE:NIFTYBANK-INDEX"]
    # Add the symbols that need to be unsubscribed.
    __symbols_to_unsubscribe: List = []
    __market_data: Dict[str, MarketData] = dict()

    def __init__(self, client_id: str, access_token: str, log_dir: Path):
        super(FyersMarketData, self).__init__()
        # The access token used in web socket should be in the following structure client_
        # id:access_token
        self._access_token: str = f"{client_id}:{access_token}"
        self._log_dir: Path = log_dir
        self._web_socket: Optional[ws.FyersSocket] = None

    def run(self) -> None:
        self._web_socket: ws.FyersSocket = ws.FyersSocket(
            access_token=self._access_token, run_background=False, log_path=self._log_dir
        )
        self._web_socket.websocket_data = self.add_market_data
        while True:
            self._subscribe()
            self._unsubscribe()
        # self._web_socket.keep_running()

    def _subscribe(self):
        """ Read __symbols_to_subscribe variable to subscribe symbols """
        if FyersMarketData.__symbols_to_subscribe:
            symbol = [
                x for x in FyersMarketData.__symbols_to_subscribe
                if x not in FyersMarketData.__existing_subscribe_symbol
            ]
            if symbol:
                print(f"Subscribing {symbol} for live market data")
                self._web_socket.subscribe(symbol=symbol, data_type="symbolData")
                FyersMarketData.__existing_subscribe_symbol.extend(symbol)
            FyersMarketData.__symbols_to_subscribe = []

    def _unsubscribe(self):
        if FyersMarketData.__symbols_to_unsubscribe:
            symbol = [
                x for x in FyersMarketData.__symbols_to_unsubscribe
                if x in FyersMarketData.__existing_subscribe_symbol
            ]
            if symbol:
                print(f"Unsubscribing {symbol} for live market data")
                self._web_socket.unsubscribe(symbol=symbol)
                FyersMarketData.__existing_subscribe_symbol = [
                    x for x in FyersMarketData.__existing_subscribe_symbol if x not in symbol
                ]
            FyersMarketData.__symbols_to_unsubscribe = []

    @staticmethod
    def subscribe(symbol: List):
        """ Subscribe to symbols to get live ticker data """
        FyersMarketData.__symbols_to_subscribe.extend(symbol)

    @staticmethod
    def unsubscribe(symbol: List):
        """ Unsubscribe ticket data """
        FyersMarketData.__symbols_to_unsubscribe.extend(symbol)

    @staticmethod
    def get_price(symbol: str):
        """ Return the last traded price of the symbol """
        return FyersMarketData.__market_data[symbol].ltp \
            if symbol in FyersMarketData.__market_data else -1

    @staticmethod
    def add_market_data(data: List):
        data = data.pop()
        symbol = data["symbol"]
        FyersMarketData.__market_data[symbol] = MarketData.from_dict(data)


class FyersSymbolParser:
    """ Get the symbol details from the master CSV file """

    def __init__(self, symbols: Optional[List] = None):
        self._nifty_df: Optional[pd.DataFrame] = None
        self._banknifty_df: Optional[pd.DataFrame] = None
        self._expiry: List[datetime.date] = []

    def setup(self):
        """ Need to pull the data once at the beginning of the process """
        df = pd.read_csv(self.nse_fo_symbol_master_file, header=None)
        # This will return a copy of slice from the original dataframe.
        # Then when we apply a function to it, it gives SettingWithCopyWarning because we are trying
        # to run the function on a subset of the dataframe.
        # Workaround is to get a copy of the subset dataframe or apply the function to the original
        # dataframe.
        # I am hesitant to apply to original df for two reason
        #   1. I am not sure if the strike for all instruments are integer
        #   2. Do not want to run to a function on a large dataset when I need only a subset of data
        # self._nifty_df = df[df[13] == "NIFTY"]
        self._nifty_df = df[df[13] == "NIFTY"].copy()
        self._banknifty_df = df[df[13] == "BANKNIFTY"].copy()
        # Strike prices are in float. Convert it to integer
        # Column 15 is the expiry
        self._nifty_df[15] = self._nifty_df[15].apply(np.int64)
        self._banknifty_df[15] = self._banknifty_df[15].apply(np.int64)
        # Convert the expiry in epoch to datetime.date
        self._nifty_df[8] = self._nifty_df[8].apply(self.epoch2date)
        self._banknifty_df[8] = self._banknifty_df[8].apply(self.epoch2date)
        # Column 8 is expiry in epoch. This will return a df will unique expiry
        unique_expiry_df = self._nifty_df.drop_duplicates(subset=[8])
        self._expiry = sorted(unique_expiry_df[8])

    def get_current_week_expiry(self, signal_date: datetime.date) -> datetime.date:
        """ Return current week expiry for the signal date. Signal date should be in IST """
        return next((x for x in self._expiry if x >= signal_date), None)

    def get_fyers_symbol_name(
            self, ticker: str, strike_price: int, expiry: datetime.date, option_type: str
    ):
        """ Get the fyers symbol by ticker, strike_price, expiry and option type """
        if ticker == "NIFTY":
            df = self._nifty_df[
                (self._nifty_df[15] == strike_price) &
                (self._nifty_df[8] == expiry) &
                (self._nifty_df[16] == option_type)
            ]
        else:
            df = self._banknifty_df[
                (self._banknifty_df[15] == strike_price) &
                (self._banknifty_df[8] == expiry) &
                (self._banknifty_df[16] == option_type)
                ]
        assert len(df.index) == 1, \
            f"More than one row found for {ticker} {strike_price} {option_type} for expiry {expiry}"
        row_index = df.index[0]
        return {
            "symbol": df.loc[row_index, 1],
            "symbol_code": df.loc[row_index, 9],
            "code": df.loc[row_index, 14]
        }

    @staticmethod
    def epoch2date(epoch: int):
        return datetime.datetime.fromtimestamp(epoch).date()

    @property
    def nse_fo_symbol_master_file(self) -> str:
        return "https://public.fyers.in/sym_details/NSE_FO.csv"



if __name__ == "__main__":
    # api = FyersApi()
    # api.setup()
    # print(f"After setup")
    # import time
    # time.sleep(5)
    # print(f"Subscribing to SBIN")
    # api.fyers_market_data.subscribe(symbol=["NSE:SBIN-EQ"])
    # print(f"--> After subscribing to SBIN")
    obj = FyersSymbolParser()
    obj.setup()
    print(obj.get_current_week_expiry(datetime.date.today()))
    output = obj.get_fyers_symbol_name(
        "NIFTY", 18150, datetime.date(year=2022, month=5, day=12), "CE"
    )
    print(output)
