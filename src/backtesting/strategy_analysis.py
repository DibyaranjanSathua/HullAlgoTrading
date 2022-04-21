"""
File:           strategy_analysis.py
Author:         Dibyaranjan Sathua
Created on:     20/04/22, 9:36 pm
"""
from typing import Optional
from dataclasses import dataclass


@dataclass()
class ConsecutiveWinLoss:
    prev_trade: int = 0     # -1 loss, 1 profit, 0 initial value
    temp_consecutive_win: int = 0
    consecutive_win: int = 0
    temp_consecutive_loss: int = 0
    consecutive_loss: int = 0

    def compute(self, profit_loss: int):
        # current trade is loss and previous trade was also in loss
        if profit_loss <= 0 and self.prev_trade <= 0:
            self.prev_trade = -1        # Setting this to override the initial value of prev_trade
            self.temp_consecutive_loss += 1
        # current trade is profit and previous trade was also in profit
        elif profit_loss > 0 and self.prev_trade >= 0:
            self.prev_trade = 1         # Setting this to override the initial value of prev_trade
            self.temp_consecutive_win += 1
        # current trade is loss but previous trade was in profit. Breaks the winning streak
        elif profit_loss <= 0 < self.prev_trade:
            self.prev_trade = -1
            self.consecutive_win = max(self.consecutive_win, self.temp_consecutive_win)
        # current trade is profit but previous trade was in loss. Breaks the losing streak
        elif self.prev_trade < 0 < profit_loss:
            self.prev_trade = 1
            self.consecutive_loss = max(self.consecutive_loss, self.temp_consecutive_loss)


@dataclass()
class StrategyAnalysis:
    """ Contains parameters for strategy analysis """
    lot_size: int = 0
    total_trades: int = 0
    profit: float = 0
    loss: float = 0
    win_trades: int = 0
    loss_trades: int = 0
    consecutive_win_loss: Optional[ConsecutiveWinLoss] = None
    initial_capital: Optional[float] = None

    @property
    def profit_loss(self) -> float:
        return round(self.profit + self.loss, 2)

    @property
    def avg_win(self) -> float:
        return round(self.profit / self.win_trades, 2)

    @property
    def avg_loss(self) -> float:
        return round(self.loss / self.loss_trades, 2)

    @property
    def ending_capital(self) -> Optional[float]:
        if self.initial_capital is None:
            return None
        return self.initial_capital + self.profit_loss

    @property
    def capital_returns(self) -> Optional[float]:
        if self.initial_capital is None:
            return None
        return self.initial_capital / self.ending_capital

    @property
    def win_percent(self) -> float:
        return round(self.win_trades / self.total_trades * 100, 2)

    @property
    def loss_percent(self) -> float:
        return round(self.loss_trades / self.total_trades * 100, 2)

    @property
    def win_ratio(self) -> float:
        return round(self.win_trades / self.loss_trades, 2)

    @property
    def avg_win_loss(self) -> float:
        return round(self.avg_win / abs(self.avg_loss), 2)

    @property
    def profit_potential(self) -> float:
        return round((self.avg_win * self.win_trades) / (abs(self.avg_loss) * self.loss_trades), 2)

    def print_analysis(self):
        print(f"Initial Capital: {self.initial_capital}")
        print(f"Lot Size: {self.lot_size}")
        print(f"Total Trades: {self.total_trades}")
        print(f"Profit/Loss: {self.profit_loss}")
        print(f"Ending Capital: {self.ending_capital}")
        print(f"Returns: {self.capital_returns}")
        print(f"Win Trades: {self.win_trades}")
        print(f"Win %: {self.win_percent}")
        print(f"Loss Trades: {self.loss_trades}")
        print(f"Loss %: {self.loss_percent}")
        print(f"Win Ratio: {self.win_ratio}")
        print(f"Avg Win: {self.avg_win}")
        print(f"Avg Loss: {self.avg_loss}")
        print(f"Avg Win / Avg Loss: {self.avg_win_loss}")
        print(f"Profit Potential: {self.profit_potential}")
        print(f"Consecutive Wins: {self.consecutive_win_loss.consecutive_win}")
        print(f"Consecutive Losses: {self.consecutive_win_loss.consecutive_loss}")
