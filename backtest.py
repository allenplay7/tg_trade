"""Backtest the channel's ENTRY signals against historical Binance klines.

Inputs:
    backtest_signals.jsonl  (produced by backfill_signals.py)

Outputs:
    backtest_results.csv     - one row per simulated trade
    backtest_metrics.json    - aggregated portfolio metrics

Methodology:
    For each ENTRY signal:
      1. Resolve TICKER -> TICKERUSDT on Binance spot
      2. Fetch 15-min klines from signal time forward (up to MAX_HOLD_DAYS)
      3. Entry: close of the first kline at/after the signal time
      4. Compute TP price (from signal value, or % of entry if pct targets)
      5. Walk forward kline-by-kline:
            - If low <= SL  -> exit at SL price (conservative: SL wins ties)
            - Else if high >= TP -> exit at TP price
      6. If neither fires in MAX_HOLD_DAYS, exit at last kline's close ("TIMEOUT")

Concurrency cap: respects MAX_CONCURRENT_POSITIONS from .env. If at cap when a
signal fires, the trade is skipped (mirrors live behavior).

Position size: respects POSITION_SIZE_USDT from .env. Each trade uses that fixed
USDT amount; PnL = (exit/entry - 1) * size.
"""
from __future__ import annotations

import argparse
import json
import logging
import math
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
import numpy as np
from binance.client import Client

from config import settings

PROJECT_ROOT = Path(__file__).resolve().parent
SIGNALS_PATH = PROJECT_ROOT / "backtest_signals.jsonl"
RESULTS_PATH = PROJECT_ROOT / "backtest_results.csv"
METRICS_PATH = PROJECT_ROOT / "backtest_metrics.json"
KLINE_CACHE = PROJECT_ROOT / "kline_cache"

MAX_HOLD_DAYS = 14
INIT_CASH = 100.0
RISK_FREE_RATE = 0.0


def fetch_klines(client: Client, symbol: str, start: datetime, end: datetime) -> pd.DataFrame:
    """15-min klines, cached on disk to avoid hammering Binance on re-runs."""
    KLINE_CACHE.mkdir(exist_ok=True)
    cache_file = KLINE_CACHE / f"{symbol}_{int(start.timestamp())}_{int(end.timestamp())}.parquet"
    if cache_file.exists():
        try:
            return pd.read_parquet(cache_file)
        except Exception:
            cache_file.unlink(missing_ok=True)

    start_ms = int(start.timestamp() * 1000)
    end_ms = int(end.timestamp() * 1000)
    try:
        raw = client.get_historical_klines(
            symbol, Client.KLINE_INTERVAL_15MINUTE,
            start_str=str(start_ms), end_str=str(end_ms),
        )
    except Exception as e:
        logging.getLogger("backtest").warning("Klines fetch failed for %s: %s", symbol, e)
        return pd.DataFrame()

    if not raw:
        return pd.DataFrame()

    df = pd.DataFrame(raw, columns=[
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_volume", "trades", "taker_buy_base",
        "taker_buy_quote", "ignore",
    ])
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    for col in ("open", "high", "low", "close"):
        df[col] = df[col].astype(float)
    df = df[["open_time", "open", "high", "low", "close"]].copy()
    try:
        df.to_parquet(cache_file, index=False)
    except Exception:
        pass
    return df


def resolve_symbol(client: Client, ticker: str, allowed_quotes: list[str]) -> Optional[str]:
    for q in allowed_quotes:
        sym = f"{ticker.upper()}{q}"
        try:
            info = client.get_symbol_info(sym)
        except Exception:
            continue
        if info and info.get("status") == "TRADING" and info.get("isSpotTradingAllowed", True):
            return sym
    return None


def simulate_one(klines: pd.DataFrame, entry_time: datetime, tp_value: float,
                 tp_is_pct: bool, sl_price: float, pos_size_usdt: float,
                 entry_price_hint: Optional[float]) -> Optional[dict]:
    """Return a dict describing the trade outcome, or None if not enough data."""
    after = klines[klines["open_time"] >= entry_time].reset_index(drop=True)
    if after.empty:
        return None

    entry_row = after.iloc[0]
    entry_price = float(entry_row["close"])
    if entry_price <= 0:
        return None

    if tp_is_pct:
        tp_price = entry_price * (1 + tp_value / 100.0)
    else:
        tp_price = tp_value

    if tp_price <= entry_price or sl_price >= entry_price:
        return {
            "outcome": "INVALID",
            "entry_time": entry_row["open_time"],
            "entry_price": entry_price,
            "exit_time": entry_row["open_time"],
            "exit_price": entry_price,
            "tp_price": tp_price,
            "sl_price": sl_price,
            "pnl_pct": 0.0,
            "pnl_usdt": 0.0,
            "duration_h": 0.0,
        }

    for _, row in after.iloc[1:].iterrows():
        low = float(row["low"])
        high = float(row["high"])
        if low <= sl_price:
            return _make_outcome(entry_row, row, entry_price, sl_price, tp_price, sl_price,
                                  "SL", pos_size_usdt)
        if high >= tp_price:
            return _make_outcome(entry_row, row, entry_price, tp_price, tp_price, sl_price,
                                  "TP", pos_size_usdt)

    last = after.iloc[-1]
    return _make_outcome(entry_row, last, entry_price, float(last["close"]),
                          tp_price, sl_price, "TIMEOUT", pos_size_usdt)


def _make_outcome(entry_row, exit_row, entry_price, exit_price, tp_price, sl_price,
                   outcome, pos_size) -> dict:
    pnl_pct = (exit_price - entry_price) / entry_price
    pnl_usdt = pnl_pct * pos_size
    dur_h = (exit_row["open_time"] - entry_row["open_time"]).total_seconds() / 3600.0
    return {
        "outcome": outcome,
        "entry_time": entry_row["open_time"],
        "entry_price": entry_price,
        "exit_time": exit_row["open_time"],
        "exit_price": exit_price,
        "tp_price": tp_price,
        "sl_price": sl_price,
        "pnl_pct": pnl_pct,
        "pnl_usdt": pnl_usdt,
        "duration_h": dur_h,
    }


def compute_metrics(trades: pd.DataFrame) -> dict:
    if trades.empty:
        return {"total_trades": 0, "note": "No trades simulated"}

    wins = trades[trades["pnl_usdt"] > 0]
    losses = trades[trades["pnl_usdt"] < 0]
    flat = trades[trades["pnl_usdt"] == 0]

    gross_profit = float(wins["pnl_usdt"].sum())
    gross_loss = float(-losses["pnl_usdt"].sum())
    net_pnl = float(trades["pnl_usdt"].sum())

    profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else float("inf")

    # Equity curve from trades sorted by exit time
    eq = trades.sort_values("exit_time").copy()
    eq["cum_pnl"] = eq["pnl_usdt"].cumsum()
    eq["equity"] = INIT_CASH + eq["cum_pnl"]

    running_max = eq["equity"].cummax()
    drawdown = (eq["equity"] - running_max) / running_max
    max_dd = float(drawdown.min()) if len(drawdown) else 0.0
    max_dd_abs = float((eq["equity"] - running_max).min()) if len(drawdown) else 0.0

    # Daily-PnL based Sharpe ratio (annualized, sqrt(365) for crypto)
    if not eq.empty:
        eq["exit_date"] = pd.to_datetime(eq["exit_time"]).dt.date
        by_day = eq.groupby("exit_date")["pnl_usdt"].sum()
        daily_returns = by_day / INIT_CASH
        if daily_returns.std() > 0:
            sharpe = float((daily_returns.mean() - RISK_FREE_RATE) /
                            daily_returns.std() * math.sqrt(365))
        else:
            sharpe = 0.0
    else:
        sharpe = 0.0

    avg_win = float(wins["pnl_usdt"].mean()) if not wins.empty else 0.0
    avg_loss = float(losses["pnl_usdt"].mean()) if not losses.empty else 0.0
    avg_duration_h = float(trades["duration_h"].mean()) if not trades.empty else 0.0

    return {
        "total_trades": int(len(trades)),
        "wins": int(len(wins)),
        "losses": int(len(losses)),
        "flat": int(len(flat)),
        "win_rate_pct": float(len(wins) / len(trades) * 100),
        "gross_profit_usdt": gross_profit,
        "gross_loss_usdt": gross_loss,
        "profit_factor": profit_factor if profit_factor != float("inf") else None,
        "net_pnl_usdt": net_pnl,
        "max_drawdown_pct": max_dd * 100,
        "max_drawdown_usdt": max_dd_abs,
        "sharpe_annualized": sharpe,
        "avg_win_usdt": avg_win,
        "avg_loss_usdt": avg_loss,
        "avg_duration_hours": avg_duration_h,
        "by_outcome": trades["outcome"].value_counts().to_dict(),
    }


def main(args: argparse.Namespace) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    log = logging.getLogger("backtest")

    if not SIGNALS_PATH.exists():
        log.error("No %s - run backfill_signals.py first.", SIGNALS_PATH.name)
        return 1

    pos_size = settings.position_size_usdt if settings.uses_fixed_usdt_sizing else 3.61
    max_concurrent = settings.max_concurrent_positions or 9999
    quote_assets = settings.allowed_quote_assets

    log.info("Backtest config: size=%.2f USDT, max_concurrent=%d, quotes=%s",
             pos_size, max_concurrent, quote_assets)

    # Load signals (sorted by time)
    raw_signals = []
    with SIGNALS_PATH.open() as f:
        for line in f:
            raw_signals.append(json.loads(line))
    entries = [
        r for r in raw_signals
        if r["kind"] == "SignalKind.ENTRY" and r.get("ticker") and r.get("take_profits")
        and r.get("stop_loss") is not None
    ]
    entries.sort(key=lambda r: r["time"])
    log.info("Loaded %d ENTRY signals (of %d total messages)",
             len(entries), len(raw_signals))

    if not entries:
        log.warning("No actionable ENTRY signals found - nothing to backtest.")
        return 0

    client = Client()  # public endpoints only
    symbol_cache: dict[str, Optional[str]] = {}
    open_positions: list[dict] = []  # active trades with exit_time
    trades_out: list[dict] = []

    for i, sig in enumerate(entries, 1):
        ticker = sig["ticker"]
        sig_time = datetime.fromisoformat(sig["time"])
        if sig_time.tzinfo is None:
            sig_time = sig_time.replace(tzinfo=timezone.utc)

        # Free up any positions that have closed before this signal
        open_positions = [p for p in open_positions if p["exit_time"] > sig_time]
        if len(open_positions) >= max_concurrent:
            log.info("[%d/%d] SKIP %s at %s: at concurrency cap (%d open)",
                     i, len(entries), ticker, sig_time.isoformat(), len(open_positions))
            continue

        # Resolve symbol
        if ticker not in symbol_cache:
            symbol_cache[ticker] = resolve_symbol(client, ticker, quote_assets)
            time.sleep(0.05)
        symbol = symbol_cache[ticker]
        if symbol is None:
            log.info("[%d/%d] SKIP %s: no spot pair on Binance in %s",
                     i, len(entries), ticker, quote_assets)
            continue

        end_time = sig_time + timedelta(days=MAX_HOLD_DAYS)
        klines = fetch_klines(client, symbol, sig_time, end_time)
        if klines.empty:
            log.info("[%d/%d] SKIP %s: no klines available", i, len(entries), ticker)
            continue

        tp_value = float(sig["take_profits"][0])
        result = simulate_one(
            klines, sig_time, tp_value,
            bool(sig["take_profits_are_pct"]),
            float(sig["stop_loss"]),
            pos_size,
            sig.get("entry_price"),
        )
        if result is None or result["outcome"] == "INVALID":
            log.info("[%d/%d] SKIP %s: invalid TP/SL vs entry", i, len(entries), ticker)
            continue

        row = {
            "signal_time": sig_time,
            "ticker": ticker,
            "symbol": symbol,
            "is_risky": sig.get("is_risky", False),
            **result,
        }
        trades_out.append(row)
        open_positions.append({"ticker": ticker, "exit_time": result["exit_time"]})
        log.info(
            "[%d/%d] %s %s entry=%.6f exit=%.6f outcome=%s pnl=%+0.4f USDT",
            i, len(entries), ticker, symbol,
            result["entry_price"], result["exit_price"], result["outcome"],
            result["pnl_usdt"],
        )

    trades_df = pd.DataFrame(trades_out)
    if not trades_df.empty:
        trades_df["entry_time"] = pd.to_datetime(trades_df["entry_time"])
        trades_df["exit_time"] = pd.to_datetime(trades_df["exit_time"])
        trades_df["signal_time"] = pd.to_datetime(trades_df["signal_time"])
        trades_df = trades_df.sort_values("entry_time").reset_index(drop=True)
        trades_df.to_csv(RESULTS_PATH, index=False)
        log.info("Wrote %s (%d rows)", RESULTS_PATH, len(trades_df))

    metrics = compute_metrics(trades_df)
    metrics["position_size_usdt"] = pos_size
    metrics["max_concurrent"] = settings.max_concurrent_positions
    metrics["init_cash"] = INIT_CASH
    metrics["lookback_days"] = args.days

    METRICS_PATH.write_text(json.dumps(metrics, indent=2, default=str))
    log.info("Wrote %s", METRICS_PATH)

    print("\n=== BACKTEST SUMMARY ===")
    for k, v in metrics.items():
        if isinstance(v, float):
            print(f"  {k:>22s} : {v:.4f}")
        else:
            print(f"  {k:>22s} : {v}")

    return 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=30, help="Days back used in backfill")
    sys.exit(main(ap.parse_args()))
