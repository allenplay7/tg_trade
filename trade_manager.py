"""Glue layer between parsed signals and Binance.

Workflow for an ENTRY signal:
    1. Sanity-check (concurrent cap, daily loss, per-ticker dedup)
    2. Resolve $TICKER to Binance spot symbol
    3. Compute position size (USDT amount) from fixed-USDT setting or % of balance
    4. Place entry order (market or limit)
    5. On fill: compute TP1 (from price or %) and SL
    6. Place OCO sell (TP + SL)
    7. Persist position row in SQLite, notify user

For management signals (CLOSE / UPDATE_SL) we operate on the most recent OPEN
position for that ticker.
"""
from __future__ import annotations

import logging
import time
from typing import Optional

from config import Settings
from db import DB
from parser import ParsedSignal, SignalKind
from binance_client import BinanceWrapper

log = logging.getLogger(__name__)


class TradeManager:
    def __init__(self, settings: Settings, binance: BinanceWrapper, db: DB, notifier):
        self.s = settings
        self.b = binance
        self.db = db
        self.n = notifier
        self._day_start_ts = self._today_start()
        self._day_start_balance: Optional[float] = None
        self._circuit_tripped = False

    async def handle(self, parsed: ParsedSignal, tg_message_id: Optional[int] = None) -> None:
        self.db.log_signal(tg_message_id, parsed)

        if parsed.kind == SignalKind.IGNORE:
            return

        if parsed.kind == SignalKind.AMBIGUOUS:
            await self.n.send(
                f"AMBIGUOUS message - manual review needed\n"
                f"Notes: {'; '.join(parsed.notes)}\n\n"
                f"Text: {parsed.raw_text[:500]}"
            )
            return

        if parsed.kind == SignalKind.TP_HIT_INFO:
            await self.n.send(
                "Channel reports TP hit"
                + (f" for ${parsed.ticker}" if parsed.ticker else "")
                + " - our OCO should handle this automatically."
            )
            return

        if parsed.kind == SignalKind.ENTRY:
            await self._handle_entry(parsed)
            return

        if not self.s.parse_mgmt_messages:
            await self.n.send(
                f"Management message for ${parsed.ticker or '?'} but "
                f"PARSE_MGMT_MESSAGES is off. Review manually:\n{parsed.raw_text[:400]}"
            )
            return

        if parsed.kind == SignalKind.CLOSE:
            await self._handle_close(parsed)
        elif parsed.kind == SignalKind.UPDATE_SL:
            await self._handle_update_sl(parsed)

    # -----------------------------------------------------------------
    # Entry
    # -----------------------------------------------------------------

    async def _handle_entry(self, parsed: ParsedSignal) -> None:
        ticker = parsed.ticker
        assert ticker is not None

        if self._daily_circuit_tripped():
            await self.n.send(f"Skipping ${ticker}: daily loss circuit-breaker tripped.")
            return

        if self.s.one_trade_per_ticker and self.db.open_positions(ticker):
            await self.n.send(f"Skipping ${ticker}: already an open position for this ticker.")
            return

        if self.s.max_concurrent_positions > 0:
            if len(self.db.open_positions()) >= self.s.max_concurrent_positions:
                await self.n.send(f"Skipping ${ticker}: max concurrent positions reached.")
                return

        info = self.b.resolve_symbol(ticker)
        if info is None:
            await self.n.send(
                f"${ticker}: no Binance spot pair found in allowed quotes "
                f"({','.join(self.s.allowed_quote_assets)}). Skipping."
            )
            return

        if info.quote not in self.s.allowed_quote_assets:
            await self.n.send(f"${ticker}: quote {info.quote} not allowed. Skipping.")
            return

        # Position sizing
        free_quote = self.b.available_quote_balance(info.quote)
        if free_quote <= 0:
            await self.n.send(f"${ticker}: no free {info.quote} balance.")
            return

        if self.s.uses_fixed_usdt_sizing:
            quote_amount = (
                self.s.risky_position_size_usdt
                if parsed.is_risky and self.s.risky_position_size_usdt > 0
                else self.s.position_size_usdt
            )
            if free_quote < quote_amount:
                await self.n.send(
                    f"${ticker}: free balance {free_quote:.2f} {info.quote} "
                    f"< required {quote_amount:.2f}. Skipping."
                )
                return
            size_desc = f"{quote_amount:.2f} {info.quote} (fixed)"
            notes = [f"size_usdt={quote_amount}", f"risky={parsed.is_risky}"]
        else:
            size_pct = (
                self.s.risky_position_size_pct if parsed.is_risky
                else self.s.position_size_pct
            )
            quote_amount = free_quote * (size_pct / 100.0)
            size_desc = f"{quote_amount:.2f} {info.quote} ({size_pct}% of free balance)"
            notes = [f"size_pct={size_pct}", f"risky={parsed.is_risky}"]

        # Place entry
        if parsed.entry_is_market or self.s.entry_order_type == "MARKET":
            fill = self.b.market_buy(info, quote_amount)
            entry_mode = "MARKET"
        else:
            price = parsed.entry_price or self.b.get_price(info.symbol)
            fill = self.b.limit_buy(info, quote_amount, price)
            entry_mode = "LIMIT"

        if fill is None or fill.filled_qty <= 0:
            await self.n.send(f"${ticker}: entry order failed or unfilled.")
            self.db.log_event("ERROR", f"entry failed for {ticker}",
                              {"parsed": parsed.raw_text[:300]})
            return

        # Compute TP
        tp_price = self._compute_tp_price(parsed, fill.avg_price)
        if tp_price is None or tp_price <= fill.avg_price:
            await self.n.send(
                f"${ticker}: invalid TP ({tp_price}) vs fill ({fill.avg_price}). "
                f"Position open without TP/SL - close manually."
            )
            pos_id = self.db.open_position(
                ticker=ticker, symbol=info.symbol, qty=fill.filled_qty,
                avg_entry=fill.avg_price, tp_price=None, sl_price=None,
                oco_order_list_id=None, entry_order_id=fill.order_id,
                notes="; ".join(notes + ["no valid TP - manual exit"]),
            )
            self.db.mark_position_error(pos_id, "TP calculation failed")
            return

        sl_price = parsed.stop_loss
        if sl_price is None or sl_price >= fill.avg_price:
            await self.n.send(
                f"${ticker}: SL ({sl_price}) >= entry ({fill.avg_price}). "
                f"Position open without SL - close manually."
            )
            pos_id = self.db.open_position(
                ticker=ticker, symbol=info.symbol, qty=fill.filled_qty,
                avg_entry=fill.avg_price, tp_price=tp_price, sl_price=None,
                oco_order_list_id=None, entry_order_id=fill.order_id,
                notes="; ".join(notes + ["bad SL - manual exit"]),
            )
            self.db.mark_position_error(pos_id, "Bad SL")
            return

        # Place OCO
        oco = self.b.place_oco_sell(info, fill.filled_qty, tp_price, sl_price)
        if oco is None:
            await self.n.send(
                f"${ticker}: OCO placement failed after entry. Position open at "
                f"~{fill.avg_price}, qty {fill.filled_qty}. Manage manually."
            )
            self.db.open_position(
                ticker=ticker, symbol=info.symbol, qty=fill.filled_qty,
                avg_entry=fill.avg_price, tp_price=tp_price, sl_price=sl_price,
                oco_order_list_id=None, entry_order_id=fill.order_id,
                notes="; ".join(notes + ["OCO failed"]),
            )
            return

        self.db.open_position(
            ticker=ticker, symbol=info.symbol, qty=fill.filled_qty,
            avg_entry=fill.avg_price, tp_price=tp_price, sl_price=sl_price,
            oco_order_list_id=oco.order_list_id, entry_order_id=fill.order_id,
            notes="; ".join(notes),
        )
        mode = "[PAPER]" if self.s.paper_trading else "[LIVE]"
        await self.n.send(
            f"{mode} OPENED ${ticker}\n"
            f"Entry: {fill.avg_price} ({entry_mode})\n"
            f"Qty: {fill.filled_qty}\n"
            f"TP: {tp_price}  SL: {sl_price}\n"
            f"Size: {size_desc}"
        )

    def _compute_tp_price(self, parsed: ParsedSignal, fill_price: float) -> Optional[float]:
        if not parsed.take_profits:
            return None
        tp1 = parsed.take_profits[0]
        if parsed.take_profits_are_pct:
            return round(fill_price * (1 + tp1 / 100.0), 10)
        return tp1

    # -----------------------------------------------------------------
    # Close
    # -----------------------------------------------------------------

    async def _handle_close(self, parsed: ParsedSignal) -> None:
        ticker = parsed.ticker
        if not ticker:
            await self.n.send("Close message with no ticker - skipping.")
            return
        positions = self.db.open_positions(ticker)
        if not positions:
            await self.n.send(f"Close ${ticker}: no open position found.")
            return
        pos = positions[0]
        info = self.b.resolve_symbol(ticker)
        if info is None:
            await self.n.send(f"Close ${ticker}: could not resolve symbol.")
            return

        if pos["oco_order_list_id"]:
            self.b.cancel_oco(info, pos["oco_order_list_id"])
        fill = self.b.market_sell_all(info, pos["qty"])
        if fill is None:
            await self.n.send(f"Close ${ticker}: market sell failed. Check Binance.")
            return

        pnl = (fill.avg_price - pos["avg_entry"]) * pos["qty"]
        self.db.close_position(pos["id"], pnl_quote=pnl, notes="closed by channel signal")
        await self.n.send(
            f"CLOSED ${ticker} via channel signal\n"
            f"Entry {pos['avg_entry']} -> Exit {fill.avg_price}\n"
            f"PnL: {pnl:+.4f} {info.quote}"
        )

    # -----------------------------------------------------------------
    # Update SL
    # -----------------------------------------------------------------

    async def _handle_update_sl(self, parsed: ParsedSignal) -> None:
        ticker = parsed.ticker
        new_sl = parsed.stop_loss
        if not ticker or new_sl is None:
            await self.n.send("SL-update without explicit ticker and price - skipping.")
            return
        positions = self.db.open_positions(ticker)
        if not positions:
            await self.n.send(f"Update SL ${ticker}: no open position.")
            return
        pos = positions[0]
        info = self.b.resolve_symbol(ticker)
        if info is None:
            return

        if pos["oco_order_list_id"]:
            self.b.cancel_oco(info, pos["oco_order_list_id"])
        if not pos["tp_price"]:
            await self.n.send(f"${ticker}: no TP stored - cannot re-place OCO.")
            return

        if new_sl >= pos["avg_entry"]:
            new_sl = pos["avg_entry"] * 0.999

        oco = self.b.place_oco_sell(info, pos["qty"], pos["tp_price"], new_sl)
        if oco is None:
            await self.n.send(f"${ticker}: failed to re-place OCO with new SL.")
            return
        self.db.update_position_sl(pos["id"], new_sl, oco.order_list_id)
        await self.n.send(f"Moved SL for ${ticker} to {new_sl}")

    # -----------------------------------------------------------------
    # Daily circuit-breaker
    # -----------------------------------------------------------------

    def _today_start(self) -> int:
        t = time.gmtime()
        return int(time.mktime((t.tm_year, t.tm_mon, t.tm_mday, 0, 0, 0, 0, 0, 0)))

    def _daily_circuit_tripped(self) -> bool:
        if self.s.daily_loss_circuit_pct <= 0:
            return False
        today = self._today_start()
        if today != self._day_start_ts:
            self._day_start_ts = today
            self._day_start_balance = None
            self._circuit_tripped = False
        if self._day_start_balance is None:
            self._day_start_balance = self.b.available_quote_balance("USDT")
        loss = -self.db.daily_realized_pnl_quote(self._day_start_ts)
        if self._day_start_balance and loss / self._day_start_balance * 100 >= self.s.daily_loss_circuit_pct:
            self._circuit_tripped = True
        return self._circuit_tripped
