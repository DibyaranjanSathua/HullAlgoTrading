"""
File:           fyers_api.py
Author:         Dibyaranjan Sathua
Created on:     05/05/22, 9:42 pm
"""
from typing import Optional, Dict, List
from dataclasses import dataclass
import os
from pathlib import Path
import datetime
import string
import random
from urllib.parse import urlparse, parse_qs
import json

import requests
from fyers_api import fyersModel
from fyers_api import accessToken
from fyers_api.Websocket import ws
from dotenv import load_dotenv

from src import BASE_DIR
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
        self._fyers: Optional[fyersModel] = None

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

    def setup(self):
        """ Setup access token used by other API """
        if self.fyers_token_file.is_file():
            print(f"Getting access token from {self.fyers_token_file}")
            self.read_token()
        else:
            self.generate_access_token()
        self._fyers = fyersModel.FyersModel(
            client_id=self._client_id, token=self._access_token, log_path=self.fyers_log_dir
        )
        self.check()

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
            print("Token in file is expired. Generating a new token.")
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

    def market_data(self):
        obj = FyersMarketData(
            client_id=self._client_id, access_token=self._access_token, log_dir=self.fyers_log_dir
        )
        obj.subscribe([])
        obj.run_process()

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


class FyersMarketData:
    """ Singleton class for subscribing for ticket data """

    def __init__(self, client_id: str, access_token: str, log_dir: Path):
        # The access token used in web socket should be in the following structure client_
        # id:access_token
        self._access_token: str = f"{client_id}:{access_token}"
        self._log_dir: Path = log_dir
        self._web_socket: ws.FyersSocket = ws.FyersSocket(
            access_token=self._access_token, run_background=False, log_path=self._log_dir
        )
        self._web_socket.websocket_data = self.print_message

    def subscribe(self, symbol: List):
        symbol = ["NSE:NIFTY50-INDEX", "NSE:NIFTYBANK-INDEX", "NSE:SBIN-EQ", "NSE:HDFC-EQ",
                  "NSE:IOC-EQ"]
        self._web_socket.subscribe(symbol=symbol, data_type="symbolData")

    def run_process(self):
        self._web_socket.keep_running()

    @staticmethod
    def print_message(message):
        print(f"Custom message: {message}")


if __name__ == "__main__":
    api = FyersApi()
    api.setup()
    api.market_data()
