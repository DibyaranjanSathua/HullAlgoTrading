"""
File:           fyers_websocket.py
Author:         Dibyaranjan Sathua
Created on:     13/05/22, 1:00 pm
"""
import json

from fyers_api.Websocket import ws


class FyersSocket(ws.FyersSocket):
    """
    Override __on_message function to send the web socket data to data function when
    background is set to True
    """
    def __on_message(self, ws, msg):
        if self.__data_type == "symbolData":
            self.response = self.parse_symbol_data(msg)
            if type(msg) == str:
                if "error" in msg:
                    msg = json.loads(msg)
                    self.response_out["s"] = msg["s"]
                    self.response_out["code"] = msg["code"]
                    self.response_out["message"] = msg["message"]
                    self.response = self.response_out
                    self.logger.error(self.response)
                    if self.websocket_data is not None:
                        self.websocket_data(self.response)
                    else:
                        print(f"Response:{self.response}")
            else:
                if self.websocket_data is not None:
                    self.websocket_data(self.response)
                else:
                    print(f"Response:{self.response}")

        else:
            self.response = msg
            if type(msg) == str:
                if "error" in msg:
                    msg = json.loads(msg)
                    self.response_out["s"] = msg["s"]
                    self.response_out["code"] = msg["code"]
                    self.response_out["message"] = msg["message"]
                    self.response = self.response_out
                    self.logger.error(self.response)
                    if self.websocket_data is not None:
                        self.websocket_data(self.response)
                    else:
                        print(f"Response:{self.response}")
                else:
                    if self.websocket_data is not None:
                        self.websocket_data(self.response)
                    else:
                        print(f"Response:{self.response}")
            else:
                if self.websocket_data is not None:
                    self.websocket_data(self.response)
                else:
                    print(f"Response:{self.response}")
        return
