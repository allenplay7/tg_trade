"""Thin wrapper around python-binance for the bot's spot-trading needs.

Responsibilities:
- Resolve a ticker like "BTC" to a Binance spot symbol like "BTCUSDT".
- Verify the symbol exists, is TRADING, and supports the OCO order type.
- Round quantities and prices to the symbol's stepSize / tickSize filters.
- Place market or limit BUY entries.
- Place an OCO SELL with TP + SL after the entry fills.
- Cancel an OCO group.
- Paper-trading mode that simulates fills using the live market price.
"""
from __future__ import annotations

import logging
import math
import time
from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from typing import Optional

from binance.client import Client
from binance.exceptions import BinanceAPIException

log = logging.getLogger(__name__)


@dataclass
class SymbolInfo:
    symbol: str
    base: str
    quote: str
    step_size: Decimal  # LOT_SIZE
    tick_size: Decimal  # PRICE_FILTER
    min_qty: Decimal
    min_notional: Decimal
    supports_oco: bool


@dataclass
class EntryFill:
    order_id: int
    avg_price: float
    filled_qty: float
    quote_spent: float


@dataclass
class OcoOrder:
    order_list_id: int
    tp_price: float
    sl_trigger: float
    sl_limit: float
    qty: float


class BinanceWrapper:
    def __init__(
        self,
        api_key: str,
        api_secret: str,
        *,
        testnet: bool = True,
        paper_trading: bool = True,
        allowed_quotes: list[str] | None = None,
    ):
        self.paper_trading = paper_trading
        self.allowed_quotes = allowed_quotes or ["USDT"]
        self.testnet = testnet
        self._symbol_cache: dict[str, Optional[SymbolInfo]] = {}
        self._paper_balances: dict[str, float] = {"USDT": 1000.0}  # for paper mode
        self._paper_orders: dict[int, dict] = {}
        self._paper_next_id = 1

        if api_key and api_secret:
            self.client = Client(api_key, api_secret, testnet=testnet)
        else:
            # Allow paper mode without keys, for offline testing
            self.client = None
            if not paper_trading:
                raise RuntimeError("Binance API keys missing and paper trading disabled")

    # -----------------------------------------------------------------
    # Symbol resolution + filters
    # -----------------------------------------------------------------

    def resolve_symbol(self, ticker: str) -> Optional[SymbolInfo]:
        ticker = ticker.upper()
        if ticker in self._symbol_cache:
            return self._symbol_cache[ticker]

        info = None
        if self.client is not None:
            for quote in self.allowed_quotes:
                symbol = f"{ticker}{quote}"
                try:
                    raw = self.client.get_symbol_info(symbol)
                except BinanceAPIException as e:
                    log.warning("get_symbol_info(%s) failed: %s", symbol, e)
                    continue
                if not raw or raw.get("status") != "TRADING":
                    continue
                if not raw.get("isSpotTradingAllowed", False):
                    continue
                info = _parse_symbol_info(raw)
                break
        else:
            # Paper mode with no client: fake a generic USDT pair.
            info = SymbolInfo(
                symbol=f"{ticker}USDT",
                base=ticker,
                quote="USDT",
                step_size=Decimal("0.0001"),
                tick_size=Decimal("0.00000001"),
                min_qty=Decimal("0.0001"),
                min_notional=Decimal("5"),
                supports_oco=True,
            )

        self._symbol_cache[ticker] = info
        return info

    # -----------------------------------------------------------------
    # Balances + price
    # -----------------------------------------------------------------

    def available_quote_balance(self, quote: str = "USDT") -> float:
        if self.paper_trading or self.client is None:
            return float(self._paper_balances.get(quote, 0.0))
        try:
            bal = self.client.get_asset_balance(asset=quote) or {}
            return float(bal.get("free", 0.0))
        except BinanceAPIException as e:
            log.error("get_asset_balance failed: %s", e)
            return 0.0

    def get_price(self, symbol: str) -> float:
        if self.client is None:
            # Pure offline paper mode - no price available
            return 0.0
        ticker = self.client.get_symbol_ticker(symbol=symbol)
        return float(ticker["price"])

    # -----------------------------------------------------------------
    # Order placement
    # -----------------------------------------------------------------

    def market_buy(self, info: SymbolInfo, quote_amount: float) -> Optional[EntryFill]:
        """Buy `quote_amount` worth of `info.base`. Returns the fill, or None on failure."""
        price = self.get_price(info.symbol) if not self.paper_trading else self.get_price(info.symbol)
        if price <= 0:
            log.error("No live price for %s, cannot size order", info.symbol)
            return None

        raw_qty = quote_amount / price
        qty = self._round_qty(info, raw_qty)
        if Decimal(str(qty)) < info.min_qty:
            log.warning("Order qty %s below min %s for %s", qty, info.min_qty, info.symbol)
            return None
        if Decimal(str(qty * price)) < info.min_notional:
            log.warning("Order notional %.4f below min %s", qty * price, info.min_notional)
            return None

        if self.paper_trading:
            order_id = self._paper_id()
            self._paper_balances[info.quote] = self.available_quote_balance(info.quote) - qty * price
            self._paper_balances[info.base] = self._paper_balances.get(info.base, 0.0) + qty
            log.info("[PAPER] BUY %s %s @ ~%s", qty, info.symbol, price)
            return EntryFill(order_id=order_id, avg_price=price, filled_qty=qty, quote_spent=qty * price)

        try:
            order = self.client.order_market_buy(symbol=info.symbol, quantity=_str_qty(info, qty))
        except BinanceAPIException as e:
            log.error("Market buy failed for %s: %s", info.symbol, e)
            return None

        return _summarize_fill(order)

    def limit_buy(self, info: SymbolInfo, quote_amount: float, price: float) -> Optional[EntryFill]:
        price = float(self._round_price(info, price))
        raw_qty = quote_amount / price
        qty = self._round_qty(info, raw_qty)
        if Decimal(str(qty)) < info.min_qty:
            return None

        if self.paper_trading:
            order_id = self._paper_id()
            self._paper_balances[info.quote] = self.available_quote_balance(info.quote) - qty * price
            self._paper_balances[info.base] = self._paper_balances.get(info.base, 0.0) + qty
            return EntryFill(order_id=order_id, avg_price=price, filled_qty=qty, quote_spent=qty * price)

        try:
            order = self.client.order_limit_buy(
                symbol=info.symbol,
                quantity=_str_qty(info, qty),
                price=_str_price(info, price),
            )
        except BinanceAPIException as e:
            log.error("Limit buy failed: %s", e)
            return None
        return _summarize_fill(order)

    def place_oco_sell(
        self,
        info: SymbolInfo,
        qty: float,
        tp_price: float,
        sl_price: float,
        sl_limit_offset_pct: float = 0.5,
    ) -> Optional[OcoOrder]:
        """Place an OCO SELL: take-profit limit + stop-loss-limit pair."""
        qty = float(self._round_qty(info, qty))
        tp_price = float(self._round_price(info, tp_price))
        sl_trigger = float(self._round_price(info, sl_price))
        # SL-limit slightly below the trigger so it actually fills in a fast drop.
        sl_limit = float(self._round_price(info, sl_trigger * (1 - sl_limit_offset_pct / 100)))

        if self.paper_trading:
            order_list_id = self._paper_id()
            self._paper_orders[order_list_id] = {
                "symbol": info.symbol,
                "qty": qty,
                "tp": tp_price,
                "sl": sl_trigger,
                "status": "OPEN",
            }
            log.info(
                "[PAPER] OCO SELL %s %s TP=%s SL=%s",
                qty, info.symbol, tp_price, sl_trigger,
            )
            return OcoOrder(order_list_id, tp_price, sl_trigger, sl_limit, qty)

        try:
            resp = self.client.create_oco_order(
                symbol=info.symbol,
                side=Client.SIDE_SELL,
                quantity=_str_qty(info, qty),
                price=_str_price(info, tp_price),
                stopPrice=_str_price(info, sl_trigger),
                stopLimitPrice=_str_price(info, sl_limit),
                stopLimitTimeInForce="GTC",
            )
        except BinanceAPIException as e:
            log.error("OCO sell failed for %s: %s", info.symbol, e)
            return None

        return OcoOrder(
            order_list_id=int(resp["orderListId"]),
            tp_price=tp_price,
            sl_trigger=sl_trigger,
            sl_limit=sl_limit,
            qty=qty,
        )

    def cancel_oco(self, info: SymbolInfo, order_list_id: int) -> bool:
        if self.paper_trading:
            self._paper_orders.pop(order_list_id, None)
            return True
        try:
            self.client.cancel_order_list(symbol=info.symbol, orderListId=order_list_id)
            return True
        except BinanceAPIException as e:
            log.error("cancel_oco failed: %s", e)
            return False

    def market_sell_all(self, info: SymbolInfo, qty: float) -> Optional[EntryFill]:
        qty = float(self._round_qty(info, qty))
        if self.paper_trading:
            price = self.get_price(info.symbol) or 0.0
            self._paper_balances[info.base] = max(0.0, self._paper_balances.get(info.base, 0.0) - qty)
            self._paper_balances[info.quote] = self.available_quote_balance(info.quote) + qty * price
            return EntryFill(order_id=self._paper_id(), avg_price=price, filled_qty=qty, quote_spent=qty * price)
        try:
            order = self.client.order_market_sell(symbol=info.symbol, quantity=_str_qty(info, qty))
        except BinanceAPIException as e:
            log.error("Market sell failed: %s", e)
            return None
        return _summarize_fill(order)

    # -----------------------------------------------------------------
    # Helpers
    # -----------------------------------------------------------------

    def _round_qty(self, info: SymbolInfo, qty: float) -> float:
        step = info.step_size
        q = (Decimal(str(qty)) / step).quantize(Decimal("1"), rounding=ROUND_DOWN) * step
        return float(q)

    def _round_price(self, info: SymbolInfo, price: float) -> Decimal:
        tick = info.tick_size
        return (Decimal(str(price)) / tick).quantize(Decimal("1"), rounding=ROUND_DOWN) * tick

    def _paper_id(self) -> int:
        self._paper_next_id += 1
        return self._paper_next_id


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def _parse_symbol_info(raw: dict) -> SymbolInfo:
    lot = _find_filter(raw["filters"], "LOT_SIZE") or {}
    price = _find_filter(raw["filters"], "PRICE_FILTER") or {}
    notional = (
        _find_filter(raw["filters"], "NOTIONAL")
        or _find_filter(raw["filters"], "MIN_NOTIONAL")
        or {}
    )
    return SymbolInfo(
        symbol=raw["symbol"],
        base=raw["baseAsset"],
        quote=raw["quoteAsset"],
        step_size=Decimal(lot.get("stepSize", "0.0001")),
        tick_size=Decimal(price.get("tickSize", "0.00000001")),
        min_qty=Decimal(lot.get("minQty", "0")),
        min_notional=Decimal(notional.get("minNotional", notional.get("notional", "0"))),
        supports_oco=raw.get("ocoAllowed", True),
    )


def _find_filter(filters: list[dict], name: str) -> Optional[dict]:
    for f in filters:
        if f.get("filterType") == name:
            return f
    return None


def _summarize_fill(order: dict) -> EntryFill:
    fills = order.get("fills") or []
    total_qty = 0.0
    total_quote = 0.0
    for f in fills:
        q = float(f["qty"])
        p = float(f["price"])
        total_qty += q
        total_quote += q * p
    if total_qty == 0:
        # Fall back to executedQty (e.g. unfilled limit order)
        total_qty = float(order.get("executedQty", 0.0))
        if total_qty == 0:
            total_qty = float(order.get("origQty", 0.0))
        total_quote = float(order.get("cummulativeQuoteQty", 0.0))
    avg = (total_quote / total_qty) if total_qty else 0.0
    return EntryFill(
        order_id=int(order["orderId"]),
        avg_price=avg,
        filled_qty=total_qty,
        quote_spent=total_quote,
    )


def _str_qty(info: SymbolInfo, qty: float) -> str:
    # Format with enough decimals to match stepSize, no scientific notation.
    decimals = max(0, -info.step_size.as_tuple().exponent)
    return f"{qty:.{decimals}f}"


def _str_price(info: SymbolInfo, price: float) -> str:
    decimals = max(0, -info.tick_size.as_tuple().exponent)
    return f"{price:.{decimals}f}"
