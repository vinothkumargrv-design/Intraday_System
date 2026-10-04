"""
Tests for the Zerodha adapter using a stub ``kiteconnect`` module, so the
order mapping is verified without network access or credentials.
"""

import asyncio
import os
import sys
import types

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from intraday_system.models import Order, OrderSide, OrderStatus, OrderType, OrderValidity


class FakeKite:
    def __init__(self, api_key):
        self.api_key = api_key
        self.access_token = None
        self.placed = []
        self.modified = []
        self.cancelled = []
        self.history = {}
    
    def set_access_token(self, token):
        self.access_token = token
    
    def generate_session(self, request_token, api_secret):
        return {"access_token": f"token-for-{request_token}"}
    
    def profile(self):
        return {"user_id": "AB1234"}
    
    def place_order(self, **params):
        self.placed.append(params)
        return f"KITE{len(self.placed)}"
    
    def modify_order(self, variety, order_id, **params):
        self.modified.append((variety, order_id, params))
        return order_id
    
    def cancel_order(self, variety, order_id):
        self.cancelled.append(order_id)
        return order_id
    
    def order_history(self, order_id):
        return self.history.get(order_id, [])


@pytest.fixture
def broker(monkeypatch):
    fake_module = types.ModuleType("kiteconnect")
    fake_module.KiteConnect = FakeKite
    monkeypatch.setitem(sys.modules, "kiteconnect", fake_module)
    from intraday_system.brokers.base_broker import BrokerType
    from intraday_system.brokers.zerodha_broker import ZerodhaBroker
    b = ZerodhaBroker(BrokerType.ZERODHA, {"zerodha": {"api_key": "key", "request_token": "req",
                                                        "api_secret": "secret"}})
    asyncio.run(b.connect())
    return b


def _order(**kwargs):
    defaults = dict(order_id="ORD-1", symbol="INFY", order_side=OrderSide.BUY,
                    order_type=OrderType.MARKET, quantity=10, validity=OrderValidity.MIS)
    defaults.update(kwargs)
    return Order(**defaults)


def test_connect_exchanges_request_token(broker):
    assert broker.kite.access_token == "token-for-req"


def test_market_order_mapping(broker):
    response = asyncio.run(broker.place_order(_order()))
    assert response.success and response.broker_order_id == "KITE1"
    params = broker.kite.placed[0]
    assert params["exchange"] == "NSE" and params["tradingsymbol"] == "INFY"
    assert params["transaction_type"] == "BUY" and params["order_type"] == "MARKET"
    assert params["product"] == "MIS" and params["quantity"] == 10
    assert "price" not in params and "trigger_price" not in params


def test_stop_and_limit_mapping(broker):
    asyncio.run(broker.place_order(_order(order_side=OrderSide.SELL, order_type=OrderType.SL_M,
                                          trigger_price=1490.123)))
    asyncio.run(broker.place_order(_order(symbol="BSE:TCS", order_type=OrderType.LIMIT, price=3500.5)))
    sl, limit = broker.kite.placed
    assert sl["order_type"] == "SL-M" and sl["trigger_price"] == 1490.12
    assert sl["transaction_type"] == "SELL"
    assert limit["exchange"] == "BSE" and limit["tradingsymbol"] == "TCS"
    assert limit["order_type"] == "LIMIT" and limit["price"] == 3500.5


def test_order_status_and_fill_mapping(broker):
    asyncio.run(broker.place_order(_order()))
    broker.kite.history["KITE1"] = [
        {"order_id": "KITE1", "status": "OPEN", "filled_quantity": 0, "quantity": 10,
         "transaction_type": "BUY", "order_type": "MARKET"},
        {"order_id": "KITE1", "status": "COMPLETE", "filled_quantity": 10, "quantity": 10,
         "average_price": 1501.25, "transaction_type": "BUY", "order_type": "MARKET"},
    ]
    order = asyncio.run(broker.get_order("KITE1"))
    assert order.status == OrderStatus.COMPLETED
    assert order.filled_quantity == 10 and order.average_fill_price == 1501.25
    assert order.order_id == "ORD-1" and order.symbol == "INFY"
    
    broker.kite.history["KITE1"].append({"order_id": "KITE1", "status": "TRIGGER PENDING",
                                         "filled_quantity": 0, "quantity": 10})
    assert asyncio.run(broker.get_order_status("KITE1")) == OrderStatus.PLACED


def test_modify_and_cancel(broker):
    assert asyncio.run(broker.modify_order("KITE9", trigger_price=1495.0)).success
    assert broker.kite.modified == [("regular", "KITE9", {"trigger_price": 1495.0})]
    assert asyncio.run(broker.cancel_order("KITE9"))
    assert broker.kite.cancelled == ["KITE9"]


def test_missing_sdk_gives_clear_error(monkeypatch):
    monkeypatch.setitem(sys.modules, "kiteconnect", None)
    from intraday_system.brokers.base_broker import BrokerError, BrokerType
    from intraday_system.brokers.zerodha_broker import ZerodhaBroker
    with pytest.raises(BrokerError, match="kiteconnect"):
        ZerodhaBroker(BrokerType.ZERODHA, {"zerodha": {"api_key": "key"}})


def test_unimplemented_brokers_are_rejected():
    from intraday_system.brokers.base_broker import BrokerError
    from intraday_system.brokers.broker_factory import get_broker
    with pytest.raises(BrokerError, match="not implemented"):
        get_broker("ALPACA", {})
