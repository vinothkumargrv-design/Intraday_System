"""
Transaction cost model for the Rapid Fire Intraday Trading System.

Shared by the backtester and the live execution engine so that simulated and
live trade records carry identical charges.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict

from .models import OrderSide

Config = Dict[str, Any]


@dataclass
class CostModel:
    """Transaction cost model for Indian intraday equity."""
    brokerage_rate: float = 0.0003
    brokerage_cap: float = 20.0          # Max brokerage per executed order
    exchange_fee_rate: float = 0.0000325
    stt_rate: float = 0.00025            # Sell side only (intraday)
    gst_rate: float = 0.18               # On brokerage + exchange fees
    stamp_duty_rate: float = 0.00003     # Buy side only
    slippage_rate: float = 0.0005        # Adverse price move per fill
    simulate_slippage: bool = True
    simulate_commissions: bool = True

    @classmethod
    def from_config(cls, config: Config) -> "CostModel":
        costs = config.get('backtester', {}).get('costs', {}) or {}
        return cls(
            brokerage_rate=float(costs.get('brokerage_rate', cls.brokerage_rate)),
            brokerage_cap=float(costs.get('brokerage_cap', cls.brokerage_cap)),
            exchange_fee_rate=float(costs.get('exchange_fee_rate', cls.exchange_fee_rate)),
            stt_rate=float(costs.get('stt_rate', cls.stt_rate)),
            gst_rate=float(costs.get('gst_rate', cls.gst_rate)),
            stamp_duty_rate=float(costs.get('stamp_duty_rate', cls.stamp_duty_rate)),
            slippage_rate=float(costs.get('slippage_factor', cls.slippage_rate)),
            simulate_slippage=bool(costs.get('simulate_slippage', True)),
            simulate_commissions=bool(costs.get('simulate_commissions', True)),
        )

    def fill_price(self, price: float, side: OrderSide) -> float:
        """Price actually received after adverse slippage."""
        if not self.simulate_slippage:
            return price
        return price * (1 + self.slippage_rate) if side == OrderSide.BUY else price * (1 - self.slippage_rate)

    def charges(self, buy_value: float, sell_value: float) -> Dict[str, float]:
        """Statutory and broker charges for a round trip."""
        if not self.simulate_commissions:
            return {"brokerage": 0.0, "exchange_fees": 0.0, "stt": 0.0, "gst": 0.0, "stamp_duty": 0.0}
        brokerage = (min(self.brokerage_cap, buy_value * self.brokerage_rate) +
                     min(self.brokerage_cap, sell_value * self.brokerage_rate))
        exchange_fees = (buy_value + sell_value) * self.exchange_fee_rate
        return {
            "brokerage": brokerage,
            "exchange_fees": exchange_fees,
            "stt": sell_value * self.stt_rate,
            "gst": (brokerage + exchange_fees) * self.gst_rate,
            "stamp_duty": buy_value * self.stamp_duty_rate,
        }
