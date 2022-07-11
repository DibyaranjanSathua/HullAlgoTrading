"""
File:           rule_engine2.py
Author:         Dibyaranjan Sathua
Created on:     28/06/22, 11:39 am
"""
from typing import List, Tuple, Optional
import datetime
import time
import pandas as pd

from src.backtesting.base_backtesting import BaseBackTesting, PairInstrument
from src.backtesting.constant import SignalType, ExitType, EntryType
from src.backtesting.instrument import Instrument
from src.backtesting.historical_data.db_api import DBApiPostgres
from src.backtesting.historical_data.models import HistoricalPrice
from src.backtesting.exception import BackTestingError
from src.backtesting.strategy_analysis import StrategyAnalysis, ConsecutiveWinLoss
from src.utils.logger import LogFacade


class RuleEngine2(BaseBackTesting, PairInstrument):
    """ RuleEngine2 backtesting. Strangle or straddle logic """

    def __init__(
            self,
            config_file_path: str,
            input_excel_file_path: Optional[str] = None,
            output_excel_file_path: Optional[str] = None
    ):
        super(RuleEngine2, self).__init__(
            config_file_path=config_file_path,
            input_excel_file_path=input_excel_file_path,
            output_excel_file_path=output_excel_file_path
        )
        PairInstrument.__init__(self)
        self._ce_historical_data: List[HistoricalPrice] = []
        self._pe_historical_data: List[HistoricalPrice] = []
        self._logger: LogFacade = LogFacade("rule_engine2")
        self._output_df: pd.DataFrame = pd.DataFrame(columns=[])
        self._trade_count: int = 0
        # Strategy analysis
        self._pe_strategy_analysis: StrategyAnalysis = StrategyAnalysis()
        self._pe_strategy_analysis.consecutive_win_loss = ConsecutiveWinLoss()
        self._ce_strategy_analysis: StrategyAnalysis = StrategyAnalysis()
        self._ce_strategy_analysis.consecutive_win_loss = ConsecutiveWinLoss()

    def execute(self):
        """ Execute backtesting """
        start_time = time.time()
        super(RuleEngine2, self).execute()
        # self._ce_strategy_analysis.initial_capital = self.config.get("initial_capital_ce")
        # self._pe_strategy_analysis.initial_capital = self.config.get("initial_capital_pe")
        self.trade_straddle_strangle()
        execution_time = round(time.time() - start_time, 2)
        self._logger.info(f"Execution time: {execution_time} seconds")
        self.save_df_to_excel(self._output_df, self.config["output_excel_file_path"])
        self._logger.info(f"Output excel is saved to {self.config['output_excel_file_path']}")
        # Print strategy analysis
        print("CE Buy Analysis")
        self._ce_strategy_analysis.print_analysis()
        print("PE Sell Analysis")
        self._pe_strategy_analysis.print_analysis()

    def trade_straddle_strangle(self):
        """ Take a straddle or strangle entry each day """
        current_date = self.config["backtesting_start_date"]
        while current_date <= self.config["backtesting_end_date"]:
            # Check if the current date is a valid trading day
            if self.is_valid_trading_day(current_date):
                entry_time = self.config["entry_time"]
                exit_time = self.config["exit_time"]
                entry_datetime = datetime.datetime.combine(current_date, entry_time)
                try:
                    atm_strike = self.get_atm_strike(date=current_date, time=entry_time)
                except BackTestingError as err:
                    # Nifty minute data is missing for that date
                    # Checking if data actually exist in database
                    if self.is_nifty_data_missing(entry_datetime):
                        msg = f"Nifty minute data is missing for {entry_datetime}."
                        self._logger.error(msg)
                        current_date += datetime.timedelta(days=1)
                        continue
                    else:
                        self._logger.error(err)
                        raise BackTestingError("Failed trading straddle/strangle") from err

                expiry = self.get_expiry(current_date)
                self.entry(
                    ce_entry_strike=self.get_ce_entry_strike(atm_strike),
                    pe_entry_strike=self.get_pe_entry_strike(atm_strike),
                    entry_datetime=entry_datetime,
                    lot_size=self.config["lot_size"],
                    expiry=expiry
                )
                self.exit(
                    exit_datetime=datetime.datetime.combine(current_date, exit_time),
                    expiry=expiry
                )
            current_date += datetime.timedelta(days=1)

    def entry(
            self,
            ce_entry_strike: int,
            pe_entry_strike: int,
            entry_datetime: datetime.datetime,
            lot_size: int,
            expiry: datetime.date,
    ) -> None:
        """ Entry logic for straddle or strangle """
        self._logger.info(f"Entry taken at {entry_datetime}")
        # Get historical data for both PE and CE for entry strike and expiry
        self.get_historical_price(
            index=self.config["script"],
            ce_strike=ce_entry_strike,
            pe_strike=pe_entry_strike,
            expiry=expiry
        )
        ce_stop_loss = pe_stop_loss = None
        sl_check = self.config.get("SL_check")  # Get SL_check from config file
        if sl_check is not None:
            ce_stop_loss = sl_check["CE"]
            pe_stop_loss = sl_check["PE"]
        # Trade a CE instrument
        self.CE_ENTRY_INSTRUMENT = self.get_ce_entry_instrument(
            entry_strike=ce_entry_strike,
            entry_datetime=entry_datetime,
            lot_size=lot_size,
            expiry=expiry,
            stop_loss=ce_stop_loss,
        )
        # Trade a PE instrument
        self.PE_ENTRY_INSTRUMENT = self.get_pe_entry_instrument(
            entry_strike=pe_entry_strike,
            entry_datetime=entry_datetime,
            lot_size=lot_size,
            expiry=expiry,
            stop_loss=pe_stop_loss,
        )
        self._trade_count += 1
        # Strategy Analysis
        self._pe_strategy_analysis.total_trades += 1
        self._ce_strategy_analysis.total_trades += 1
        df_data = {
            "Trade": self._trade_count,
            "Script": self.config["script"],
            "Expiry": expiry,
            "LotSize": lot_size,
            "PEStrike": pe_entry_strike,
            "PEEntryExitTime": entry_datetime,
            "PESell": self.PE_ENTRY_INSTRUMENT.price,
            "PEProfitLoss": 0,
            "PEEntryExitType": EntryType.ENTRY_SIGNAL,
            "CEStrike": ce_entry_strike,
            "CEEntryExitTime": entry_datetime,
            "CEBuy": self.CE_ENTRY_INSTRUMENT.price,
            "CEProfitLoss": 0,
            "CEEntryExitType": EntryType.ENTRY_SIGNAL
        }
        df = pd.DataFrame([list(df_data.values())], columns=list(df_data.keys()))
        self._output_df = pd.concat([self._output_df, df], axis=0)

    def exit(self, exit_datetime: datetime.datetime, expiry: datetime.date):
        """ Exit logic """
        self._logger.info(f"Exiting trade at {exit_datetime}")
        quantity_per_lot = self.config["quantity_per_lot"]
        # Variables to track if the instrument is exited due to SL
        ce_instrument_sl_hit = False
        pe_instrument_sl_hit = False
        sl_check = self.config.get("SL_check")  # Get SL_check from config file
        ce_actual_exit_datetime = pe_actual_exit_datetime = exit_datetime
        ce_exit_type = pe_exit_type = ExitType.EXIT_SIGNAL
        if sl_check is not None:
            ce_stop_loss = sl_check["CE"]
            pe_stop_loss = sl_check["PE"]
            ce_instrument_sl_hit, ce_sl_exit_price, ce_sl_datetime = self.instrument_stop_loss_hit(
                instrument=self.CE_ENTRY_INSTRUMENT,
                exit_datetime=exit_datetime,
                stop_loss=ce_stop_loss
            )
            pe_instrument_sl_hit, pe_sl_exit_price, pe_sl_datetime = self.instrument_stop_loss_hit(
                instrument=self.PE_ENTRY_INSTRUMENT,
                exit_datetime=exit_datetime,
                stop_loss=pe_stop_loss
            )
        if ce_instrument_sl_hit:
            # This is True when SL check is ON and CE SL hits
            self._logger.info(f"SL hit for {self.CE_ENTRY_INSTRUMENT.symbol}")
            ce_exit_price = ce_sl_exit_price
            ce_actual_exit_datetime = ce_sl_datetime
            ce_exit_type = ExitType.SL_EXIT
        else:
            try:
                ce_exit_price = self.get_historical_price_by_datetime(
                    option_type="CE", dt=exit_datetime
                )
            except BackTestingError:
                # Checking if data actually exist in database
                if self.is_data_missing(self.CE_ENTRY_INSTRUMENT.strike, exit_datetime):
                    msg = f"Data is missing for {self.CE_ENTRY_INSTRUMENT.symbol} at " \
                          f"{exit_datetime} for expiry {expiry}"
                    self._logger.error(msg)
                    ce_exit_price = 0
                else:
                    msg = f"No price data found for {self.CE_ENTRY_INSTRUMENT.symbol} at " \
                          f"{exit_datetime} for expiry {expiry}"
                    self._logger.error(msg)
                    raise BackTestingError(msg)
        ce_profit_loss = (self.CE_ENTRY_INSTRUMENT.price - ce_exit_price) * \
                         self.CE_ENTRY_INSTRUMENT.lot_size * quantity_per_lot
        # Strategy analysis
        if ce_profit_loss > 0:
            self._ce_strategy_analysis.profit += ce_profit_loss
            self._ce_strategy_analysis.win_trades += 1
        else:
            self._ce_strategy_analysis.loss += ce_profit_loss
            self._ce_strategy_analysis.loss_trades += 1
        self._ce_strategy_analysis.consecutive_win_loss.compute(ce_profit_loss)
        self._ce_strategy_analysis.compute_equity_curve(ce_profit_loss)
        if pe_instrument_sl_hit:
            # This is True when SL check is ON and PE SL hits
            self._logger.info(f"SL hit for {self.PE_ENTRY_INSTRUMENT.symbol}")
            pe_exit_price = pe_sl_exit_price
            pe_actual_exit_datetime = pe_sl_datetime
            pe_exit_type = ExitType.SL_EXIT
        else:
            try:
                pe_exit_price = self.get_historical_price_by_datetime(
                    option_type="PE", dt=exit_datetime
                )
            except BackTestingError:
                # Checking if data actually exist in database
                if self.is_data_missing(self.PE_ENTRY_INSTRUMENT.strike, exit_datetime):
                    msg = f"Data is missing for {self.PE_ENTRY_INSTRUMENT.symbol} at " \
                          f"{exit_datetime} for expiry {expiry}"
                    self._logger.error(msg)
                    pe_exit_price = 0
                else:
                    msg = f"No price data found for {self.PE_ENTRY_INSTRUMENT.symbol} at " \
                          f"{exit_datetime} for expiry {expiry}"
                    self._logger.error(msg)
                    raise BackTestingError(msg)
        pe_profit_loss = (self.PE_ENTRY_INSTRUMENT.price - pe_exit_price) * \
                         self.PE_ENTRY_INSTRUMENT.lot_size * quantity_per_lot
        # Strategy analysis
        if pe_profit_loss > 0:
            self._pe_strategy_analysis.profit += pe_profit_loss
            self._pe_strategy_analysis.win_trades += 1
        else:
            self._pe_strategy_analysis.loss += pe_profit_loss
            self._pe_strategy_analysis.loss_trades += 1
        self._pe_strategy_analysis.consecutive_win_loss.compute(pe_profit_loss)
        self._pe_strategy_analysis.compute_equity_curve(pe_profit_loss)
        # Add to output dataframe
        df_data = {
            "Trade": self._trade_count,
            "Script": self.config["script"],
            "Expiry": expiry,
            "LotSize": self.PE_ENTRY_INSTRUMENT.lot_size,
            "PEStrike": self.PE_ENTRY_INSTRUMENT.strike,
            "PEEntryExitTime": pe_actual_exit_datetime,
            "PESell": pe_exit_price,
            "PEProfitLoss": pe_profit_loss,
            "PEEntryExitType": pe_exit_type,
            "CEStrike": self.CE_ENTRY_INSTRUMENT.strike,
            "CEEntryExitTime": ce_actual_exit_datetime,
            "CEBuy": ce_exit_price,
            "CEProfitLoss": ce_profit_loss,
            "CEEntryExitType": ce_exit_type
        }
        df = pd.DataFrame([list(df_data.values())], columns=list(df_data.keys()))
        self._output_df = pd.concat([self._output_df, df], axis=0)
        self.CE_ENTRY_INSTRUMENT = self.PE_ENTRY_INSTRUMENT = None

    def get_atm_strike(self, date: datetime.date, time: datetime.time) -> int:
        """ Get the atm strike price """
        script = self.config["script"]
        self._logger.info(
            f"Fetching {script} minute data to determine ATM strike price for {date} {time}"
        )
        minute_data = None
        if script == "NIFTY":
            # Return all minutes data for that specific date
            minute_data = DBApiPostgres.get_nifty_minute_data(
                session=self.session,        # Defined in base class
                date=date
            )
        # Some of the minutes data might not be available.
        # So we take the next minute available data for the same day
        minute_data = next((x for x in minute_data or [] if x.ticker_time >= time), None)
        if minute_data is None:
            raise BackTestingError(f"Minute {script} data is missing for {date} {time}")
        return self.get_nearest_50_strike(minute_data.close)

    def get_ce_entry_strike(self, atm_strike: int) -> int:
        """ Get the CE entry strike based on the configuration """
        return int(atm_strike + self.config["ce_strike"])

    def get_pe_entry_strike(self, atm_strike: int) -> int:
        """ Get the PE entry strike based on the configuration """
        return int(atm_strike + self.config["pe_strike"])

    def get_ce_entry_instrument(
            self,
            entry_strike: int,
            entry_datetime: datetime.datetime,
            lot_size: int,
            expiry: datetime.date,
            stop_loss: Optional[float] = None
    ) -> Instrument:
        """ Return a CE instrument. Stop loss will be in percent like 25%, 50% """
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
        sl_price = None
        if stop_loss is not None:
            # Its a short trade. So Stop Loss will be above the short price
            sl_price = round((100 + stop_loss) * ce_entry_price / 100, 2)
        return Instrument(
            symbol=symbol,
            lot_size=int(lot_size),
            entry=entry_datetime,
            expiry=expiry,
            option_type="CE",
            strike=entry_strike,
            price=ce_entry_price,
            sl_price=sl_price
        )

    def get_pe_entry_instrument(
            self,
            entry_strike: int,
            entry_datetime: datetime.datetime,
            lot_size: int,
            expiry: datetime.date,
            stop_loss: Optional[float] = None
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
        sl_price = None
        if stop_loss is not None:
            # Its a short trade. So Stop Loss will be above the short price
            sl_price = round((100 + stop_loss) * pe_entry_price / 100, 2)
        return Instrument(
            symbol=symbol,
            lot_size=int(lot_size),
            entry=entry_datetime,
            expiry=expiry,
            option_type="PE",
            strike=entry_strike,
            price=pe_entry_price,
            sl_price=sl_price
        )

    def get_historical_price_by_datetime(self, option_type: str, dt: datetime.datetime) -> float:
        """ Filter out the price by datetime. Return a minute data for the given datetime """
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
                if x.ticker_datetime.date() == dt.date() and x.ticker_datetime >= dt
            ),
            None
        )
        if price_data is None:
            raise BackTestingError(f"price data is missing for {dt}")
        return price_data.close

    def get_historical_price(
            self, index: str, ce_strike: int, pe_strike: int, expiry: datetime.date
    ) -> None:
        """
        Get the list of CE and PE historical price data from database for specific strike
        for the given expiry.
        """
        self._logger.info(
            f"Getting historical data for {index} {ce_strike} CE for expiry {expiry}"
        )
        self._ce_historical_data = self.historical_data.get_data(
            index=index,
            strike=ce_strike,
            option_type="CE",
            expiry=expiry
        )
        self._logger.info(
            f"Getting historical data for {index} {pe_strike} PE for expiry {expiry}"
        )
        self._pe_historical_data = self.historical_data.get_data(
            index=index,
            strike=pe_strike,
            option_type="PE",
            expiry=expiry
        )

    def instrument_stop_loss_hit(
            self,
            instrument: Instrument,
            exit_datetime: datetime.datetime,
            stop_loss: Optional[float] = None
    ) -> Tuple[bool, Optional[float], Optional[datetime.datetime]]:
        """
        Check if SL hit for an instrument.
        Return a tuple with first element a boolean indicate if SL hit or not. Second element is
        the price at which the SL hit. Third element is the time SL hit.
        """
        price_data = self.get_historical_price_range_data(
            start_datetime=instrument.entry,
            end_datetime=exit_datetime,
            option_type=instrument.option_type
        )
        # Stop loss logic is same for both PE and CE as we are shorting both the instruments
        for data in price_data:
            # Its a short trade. So Stop Loss will be above the short price
            sl_price = round((100 + stop_loss) * data.close / 100, 2)
            # Trailing stop loss. If the current sl_price is less than previous sl_price, trail it.
            # This is a short trade. So when price is going down, we need to trail the stop loss.
            # And when price is going down, the new sl will be less then the prev sl.
            if sl_price < instrument.sl_price:
                instrument.sl_price = sl_price
            # Short trade. If price goes above instrument_sl_price, SL hit
            if data.close > instrument.sl_price:
                return True, data.close, data.ticker_datetime
        return False, None, None

    def get_historical_price_range_data(
            self,
            start_datetime: datetime.datetime,
            end_datetime: datetime.datetime,
            option_type: str
    ) -> List[HistoricalPrice]:
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
            if start_datetime < x.ticker_datetime < end_datetime
        ]

    @property
    def ce_strategy_analysis(self) -> StrategyAnalysis:
        return self._ce_strategy_analysis

    @property
    def pe_strategy_analysis(self) -> StrategyAnalysis:
        return self._pe_strategy_analysis
