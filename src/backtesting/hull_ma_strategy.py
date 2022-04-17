"""
File:           hull_ma_strategy.py
Author:         Dibyaranjan Sathua
Created on:     16/04/22, 12:06 pm
"""
from typing import List, Tuple, Optional
import datetime
from pathlib import Path
import pandas as pd
import sqlite3

from src.backtesting.base_backtesting import BaseBackTesting
from src.backtesting.constant import SignalType, ExitType
from src.backtesting.instrument import Instrument
from src.backtesting.historical_data.db_api import DBApi
from src.backtesting.exception import BackTestingError
from src.utils.logger import LogFacade


class HullMABackTesting(BaseBackTesting):
    """ Hull moving average backtesting """
    OUTPUT_EXCEL_COLUMNS = [
        "Trade", "Script", "Strike", "Expiry", "LotSize", "PEEntryExitTime", "PESell",
        "PEProfitLoss", "PEExitType", "CEEntryExitTime", "CEBuy", "CEProfitLoss", "CEExitType"
    ]

    def __init__(self, config_file_path: str):
        super(HullMABackTesting, self).__init__(config_file_path=config_file_path)
        self._ce_historical_data: List[sqlite3.Row] = []
        self._pe_historical_data: List[sqlite3.Row] = []
        self._logger: LogFacade = LogFacade("hull_ma_backtesting")
        self._output_df: pd.DataFrame = pd.DataFrame(columns=[])
        self._trade_count: int = 0

    def process_input(self):
        """ Process the input excel row by row """
        self._logger.info(
            f"Reading and processing input excel file {self.config['input_excel_file_path']}"
        )
        input_df = self.read_input_excel_to_df(Path(self.config["input_excel_file_path"]))
        for index, row in input_df.iterrows():
            # Signal type is entry and no entry has taken
            if row["Signal"] == SignalType.ENTRY and not self.is_entry_taken():
                self._logger.info(
                    f"Entry signal triggered at {row['Date/Time']} for price {row['Price']}"
                )
                self.entry(row)
            elif row["Signal"] == SignalType.EXIT and self.is_entry_taken():
                self._logger.info(
                    f"Exit signal triggered at {row['Date/Time']} for price {row['Price']}"
                )
                self.exit(row)

    def fetch_historical_price(self, index: str, strike: int, expiry: datetime.date) -> None:
        """ Get the list of CE historical price data """
        self._logger.info(f"Fetching historical data for {index} {strike} for expiry {expiry}")
        with DBApi(Path(self.config["db_file_path"])) as db_api:
            self._ce_historical_data = db_api.fetch_historical_data(
                index=index,
                strike=strike,
                option_type="CE",
                expiry=expiry
            )
            self._pe_historical_data = db_api.fetch_historical_data(
                index=index,
                strike=strike,
                option_type="PE",
                expiry=expiry
            )

    def get_historical_price_by_datetime(self, option_type: str, dt: datetime.datetime) -> float:
        """ Filter out the price by datetime """
        if option_type == "CE":
            historical_data = self._ce_historical_data
        else:
            historical_data = self._pe_historical_data

        price_data = next(
            (
                x
                for x in historical_data
                # Some of the minutes data is not available. So we take the next minute available
                # data for the same day
                if datetime.datetime.strptime(x["dt"], "%Y-%m-%d %H:%M:%S.%f").date() == dt.date()
                   and datetime.datetime.strptime(x["dt"], "%Y-%m-%d %H:%M:%S.%f") >= dt
            ),
            None
        )
        if price_data is None:
            raise BackTestingError(f"price data is missing for {dt}")
        return float(price_data["close"])

    def get_ce_entry_instrument(
            self,
            entry_strike: int,
            entry_datetime: datetime.datetime,
            lot_size: int,
            expiry: datetime.date,

    ) -> Instrument:
        symbol = f"{self.config['script']} {entry_strike} CE"
        # Get the price for CE strike at entry_datetime
        try:
            ce_entry_price = self.get_historical_price_by_datetime(
                option_type="CE", dt=entry_datetime
            )
        except BackTestingError:
            # Checking if data actually exist in database
            if self.is_data_missing(entry_strike, entry_datetime):
                msg = f"Data is missing for {symbol} at {entry_datetime} for expiry {expiry}"
                self._logger.error(msg)
                ce_entry_price = 0
            else:
                msg = f"No price data found for {symbol} at {entry_datetime} for expiry {expiry}"
                self._logger.error(msg)
                raise BackTestingError(msg)

        return Instrument(
            symbol=symbol,
            lot_size=int(lot_size),
            entry=entry_datetime,
            expiry=expiry,
            option_type="CE",
            strike=entry_strike,
            price=ce_entry_price
        )

    def get_pe_entry_instrument(
            self,
            entry_strike: int,
            entry_datetime: datetime.datetime,
            lot_size: int,
            expiry: datetime.date,
    ) -> Instrument:
        symbol = f"{self.config['script']} {entry_strike} PE"
        # Get the price for PE strike at entry_datetime
        try:
            pe_entry_price = self.get_historical_price_by_datetime(
                option_type="PE", dt=entry_datetime
            )
        except BackTestingError:
            # Checking if data actually exist in database
            if self.is_data_missing(entry_strike, entry_datetime):
                msg = f"Data is missing for {symbol} at {entry_datetime} for expiry {expiry}"
                self._logger.error(msg)
                pe_entry_price = 0
            else:
                msg = f"No price data found for {symbol} at {entry_datetime} for expiry {expiry}"
                self._logger.error(msg)
                raise BackTestingError(msg)

        return Instrument(
            symbol=symbol,
            lot_size=int(lot_size),
            entry=entry_datetime,
            expiry=expiry,
            option_type="PE",
            strike=entry_strike,
            price=pe_entry_price
        )

    def trade_ce_instrument(self, ce_instrument: Instrument) -> bool:
        """
        If ce_premium_check is defined in config file, then check if the ce premium is exceeding
        the amount mentioned in config file. Skip ce trade if premium amount exceeds.
        Returns True when ce premium < premium in config file or ce_premium_check is not defined or
        set to null in json file.
        Returns False when ce premium > premium in config file
        """
        premium_check = self.config.get("ce_premium_check")
        if premium_check is not None:
            # Premium in config file is per lot. So multiply it by lot_size
            ce_instrument_premium = ce_instrument.price * ce_instrument.lot_size * \
                                    self.config["quantity_per_lot"]
            if ce_instrument_premium > premium_check["premium"] * ce_instrument.lot_size:
                return False
        return True

    def get_historical_price_range_data(
            self,
            start_datetime: datetime.datetime,
            end_datetime: datetime.datetime,
            option_type: str
    ) -> List[sqlite3.Row]:
        """
        Returns price range minute by minute data between start datetime and end datetime.
        This is use for checking SL hit. Start datetime is entry datetime and end datetime is
        actual exit datetime.
        """
        if option_type == "CE":
            historical_data = self._ce_historical_data
        else:
            historical_data = self._pe_historical_data

        return [
            x
            for x in historical_data
            if start_datetime < datetime.datetime.strptime(x["dt"], "%Y-%m-%d %H:%M:%S.%f") <
               end_datetime
        ]

    def instrument_stop_loss_hit(
            self, instrument: Instrument, stop_loss: float, exit_datetime: datetime.datetime
    ) -> Tuple[bool, Optional[float], Optional[datetime.datetime]]:
        """
        Check if SL hit for an instrument.
        Return a tuple with first element a boolean indicate if SL hit or not. Second element is
        the price at which the SL hit. Third element is the time SL hit.
        stop_loss is in percentage like 20%, 25%.
        """
        price_data = self.get_historical_price_range_data(
            start_datetime=instrument.entry,
            end_datetime=exit_datetime,
            option_type=instrument.option_type
        )
        if instrument.option_type == "CE":
            # Its a long trade. So Stop Loss will be below the long price
            instrument_sl_price = round((100 - stop_loss) * instrument.price / 100, 2)
        else:
            # Its a short trade. So Stop Loss will be above the short price
            instrument_sl_price = round((100 + stop_loss) * instrument.price / 100, 2)
        for data in price_data:
            # Long trade. If price goes below instrument_sl_price, SL hit
            if instrument.option_type == "CE" and float(data["close"]) < instrument_sl_price:
                return True, float(data["close"]), \
                       datetime.datetime.strptime(data["dt"], "%Y-%m-%d %H:%M:%S.%f")
            # Short trade. If price goes above instrument_sl_price, SL hit
            elif instrument.option_type == "PE" and float(data["close"]) > instrument_sl_price:
                return True, float(data["close"]), \
                       datetime.datetime.strptime(data["dt"], "%Y-%m-%d %H:%M:%S.%f")
        return False, None, None

    def entry(self, entry_row) -> None:
        """ Logic for trade entry """
        entry_datetime = self.get_market_hour_datetime(entry_row["Date/Time"])
        entry_strike = self.get_entry_strike(entry_row["Price"])
        expiry = self.get_current_week_expiry(entry_datetime.date(), self.config["db_file_path"])
        lot_size = int(entry_row["Contracts"])
        # Get historical data for both PE and CE for entry strike and expiry
        self.fetch_historical_price(
            index=self.config["script"], strike=entry_strike, expiry=expiry
        )
        self.CE_ENTRY_INSTRUMENT = self.get_ce_entry_instrument(
            entry_strike=entry_strike,
            entry_datetime=entry_datetime,
            lot_size=lot_size,
            expiry=expiry
        )
        ce_instrument_price = self.CE_ENTRY_INSTRUMENT.price
        # Check CE instrument premium
        if not self.trade_ce_instrument(self.CE_ENTRY_INSTRUMENT):
            self._logger.info(
                f"CE premium for {self.CE_ENTRY_INSTRUMENT.symbol} exceeds the premium specified "
                f"in config file. Ignoring CE trading."
            )
            self.CE_ENTRY_INSTRUMENT = None
        self.PE_ENTRY_INSTRUMENT = self.get_pe_entry_instrument(
            entry_strike=entry_strike,
            entry_datetime=entry_datetime,
            lot_size=lot_size,
            expiry=expiry
        )
        self._trade_count += 1
        df_data = {
            "Trade": self._trade_count,
            "Script": self.config["script"],
            "Strike": entry_strike,
            "Expiry": expiry,
            "LotSize": self.PE_ENTRY_INSTRUMENT.lot_size,
            "PEEntryExitTime": entry_datetime,
            "PESell": self.PE_ENTRY_INSTRUMENT.price,
            "PEProfitLoss": 0,
            "PEExitType": "",
            "CEEntryExitTime": entry_datetime,
            "CEBuy": ce_instrument_price,
            "CEProfitLoss": 0,
            "CEExitType": ""
        }
        df = pd.DataFrame([list(df_data.values())], columns=list(df_data.keys()))
        self._output_df = pd.concat([self._output_df, df], axis=0)

    def exit(self, exit_row) -> None:
        """ Logic for trade exit """
        exit_datetime = self.get_market_hour_datetime(exit_row["Date/Time"])
        lot_size = int(exit_row["Contracts"])
        quantity_per_lot = self.config["quantity_per_lot"]
        expiry = self.PE_ENTRY_INSTRUMENT.expiry
        if exit_datetime.date() > expiry:
            ce_exit_type = pe_exit_type = ExitType.EXPIRY_EXIT
            ce_actual_exit_datetime = pe_actual_exit_datetime = datetime.datetime.combine(
                expiry, datetime.time(hour=15, minute=29)
            )
        else:
            ce_exit_type = pe_exit_type = ExitType.EXIT_SIGNAL
            ce_actual_exit_datetime = pe_actual_exit_datetime = exit_datetime

        # Variables to track if the instrument is exited
        ce_instrument_exited = False
        pe_instrument_exited = False
        sl_check = self.config.get("SL_check")      # Get SL_check from config file
        # Get CE exit price
        ce_exit_price = 0
        ce_profit_loss = 0
        if self.CE_ENTRY_INSTRUMENT is not None:
            # Check if SL exit mode is set in config file. If yes, check minute by minute data
            # to see if SL is getting hit
            if sl_check is not None:
                ce_instrument_exited, ce_exit_price, sl_datetime = self.instrument_stop_loss_hit(
                    instrument=self.CE_ENTRY_INSTRUMENT,
                    stop_loss=sl_check["CE"],
                    exit_datetime=ce_actual_exit_datetime
                )
            if ce_instrument_exited:
                # This is True when SL check is ON and CE SL hits
                ce_actual_exit_datetime = sl_datetime
                ce_exit_type = ExitType.SL_EXIT
            else:
                # For this part the exit type is already calculated in top based on expiry date
                try:
                    ce_exit_price = self.get_historical_price_by_datetime(
                        option_type="CE", dt=ce_actual_exit_datetime
                    )
                except BackTestingError:
                    # Checking if data actually exist in database
                    if self.is_data_missing(self.CE_ENTRY_INSTRUMENT.strike, ce_actual_exit_datetime):
                        msg = f"Data is missing for {self.CE_ENTRY_INSTRUMENT.symbol} at " \
                              f"{ce_actual_exit_datetime} for expiry {expiry}"
                        self._logger.error(msg)
                        ce_exit_price = 0
                    else:
                        msg = f"No price data found for {self.CE_ENTRY_INSTRUMENT.symbol} at " \
                              f"{ce_actual_exit_datetime} for expiry {expiry}"
                        self._logger.error(msg)
                        raise BackTestingError(msg)
            ce_profit_loss = (ce_exit_price - self.CE_ENTRY_INSTRUMENT.price) * lot_size * \
                             quantity_per_lot
            self.CE_ENTRY_INSTRUMENT.lot_size -= lot_size
        else:
            ce_exit_type = ExitType.CE_PREMIUM_EXIT

        # Get PE exit price
        # Check if SL exit mode is set in config file. If yes, check minute by minute data
        # to see if SL is getting hit
        pe_exit_price = 0
        if sl_check is not None:
            pe_instrument_exited, pe_exit_price, sl_datetime = self.instrument_stop_loss_hit(
                instrument=self.PE_ENTRY_INSTRUMENT,
                stop_loss=sl_check["PE"],
                exit_datetime=pe_actual_exit_datetime
            )
        if pe_instrument_exited:
            # This is True when SL check is ON and CE SL hits
            pe_actual_exit_datetime = sl_datetime
            pe_exit_type = ExitType.SL_EXIT
        else:
            try:
                pe_exit_price = self.get_historical_price_by_datetime(
                    option_type="PE", dt=pe_actual_exit_datetime
                )
            except BackTestingError:
                # Checking if data actually exist in database
                if self.is_data_missing(self.PE_ENTRY_INSTRUMENT.strike, pe_actual_exit_datetime):
                    msg = f"Data is missing for {self.PE_ENTRY_INSTRUMENT.symbol} at " \
                          f"{pe_actual_exit_datetime} for expiry {expiry}"
                    self._logger.error(msg)
                    pe_exit_price = 0
                else:
                    msg = f"No price data found for {self.PE_ENTRY_INSTRUMENT.symbol} at " \
                          f"{pe_actual_exit_datetime} for expiry {expiry}"
                    self._logger.error(msg)
                    raise BackTestingError(msg)

        pe_profit_loss = (self.PE_ENTRY_INSTRUMENT.price - pe_exit_price) * lot_size * \
                         quantity_per_lot
        # Decrease the lot size from the entry instrument
        self.PE_ENTRY_INSTRUMENT.lot_size -= lot_size
        # Add to output dataframe
        df_data = {
            "Trade": self._trade_count,
            "Script": self.config["script"],
            "Strike": self.PE_ENTRY_INSTRUMENT.strike,
            "Expiry": expiry,
            "LotSize": self.PE_ENTRY_INSTRUMENT.lot_size,
            "PEEntryExitTime": pe_actual_exit_datetime,
            "PESell": pe_exit_price,
            "PEProfitLoss": pe_profit_loss,
            "PEExitType": pe_exit_type,
            "CEEntryExitTime": ce_actual_exit_datetime,
            "CEBuy": ce_exit_price,
            "CEProfitLoss": ce_profit_loss,
            "CEExitType": ce_exit_type
        }
        df = pd.DataFrame([list(df_data.values())], columns=list(df_data.keys()))
        self._output_df = pd.concat([self._output_df, df], axis=0)
        # Check if all the lots are exit, make the instrument as None so as to take next
        # entry signal
        if self.PE_ENTRY_INSTRUMENT.lot_size == 0:
            self.PE_ENTRY_INSTRUMENT = None
        if self.CE_ENTRY_INSTRUMENT is not None and self.CE_ENTRY_INSTRUMENT.lot_size == 0:
            self.CE_ENTRY_INSTRUMENT = None

    def execute(self) -> None:
        """ Execute backtesting """
        super(HullMABackTesting, self).execute()
        self.process_input()
        self.save_df_to_excel(self._output_df, self.config["output_excel_file_path"])
        self._logger.info(f"Output excel is saved to {self.config['output_excel_file_path']}")

