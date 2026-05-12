"""Backtest the channel's ENTRY signals against historical price data.

THREE STRATEGIES RUN ON THE SAME SIGNALS:

  A  SELL_ALL_TP1
     - Sell 100% of position when price reaches TP1.
     - Fixed SL until then. If SL hits first -> full loss.

  B  TIERED_FIXED_SL  (60/30/10 at TP1/TP2/TP3, original SL throughout)
     - Sell 60% at TP1, 30% at TP2, 10% at TP3.
     - If signal only has 2 TPs, splits become 60/40.
     - If only 1 TP, falls back to 100% at TP1.
     - The signal's original SL stays active for the *whole remainder* at all
       times. If SL hits, sell everything remaining at SL.

  C  TIERED_TRAIL_AFTER_TP1  (60/30/10 with 5% trailing after TP1)
     - Same splits as B.
     - BEFORE TP1 hits: original fixed SL.
     - AFTER TP1 hits: SL becomes a trailing 5% stop following the price peak.
       That means TP2 and TP3 can still fire normally, but if the price pulls
       back 5% from any new high in between, the trailing stop closes the rest.

DATA SOURCES (per ticker):
  1. Binance spot (preferred - 15-min OHLC klines)
  2. CoinGecko hourly prices (fallback for Alpha / non-spot tickers; OHLC
     approximated since we only have price points)

Fees: 0.2% round-trip per leg (BINANCE_FEE_BPS=20 default).
Concurrency cap: respects MAX_CONCURRENT_POSITIONS from .env.

Outputs:
  backtest_results.csv          One row per (signal, strategy)
  backtest_metrics.json         Aggregate metrics per strategy + comparison
"""
from __future__ import annotations
import os
import ccxt
import json
import logging
import math
import sys
import time
import threading
from tqdm import tqdm
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional
import concurrent.futures
import numpy as np
import pandas as pd
from binance.client import Client
from binance.exceptions import BinanceAPIException

from config import settings
from coingecko_data import CoinGeckoClient

PROJECT_ROOT = Path(__file__).resolve().parent
SIGNALS_PATH = PROJECT_ROOT / "backtest_signals.jsonl"
RESULTS_PATH = PROJECT_ROOT / "backtest_results.csv"
METRICS_PATH = PROJECT_ROOT / "backtest_metrics.json"
KLINE_CACHE = PROJECT_ROOT / "kline_cache"
SYMBOL_CACHE_PATH = PROJECT_ROOT / "symbol_cache.json"

MAX_HOLD_DAYS = 14
INIT_CASH = 100.0
BINANCE_TIMEOUT_S = 10
PRIORITY_EXCHANGES = ['mexc', 'okx', 'bybit', 'gateio', 'kucoin', 'kraken', 'bitget']

# ----------------------------------------------------------------------------
# Strategy definitions
# ----------------------------------------------------------------------------

@dataclass
class Strategy:
    name: str
    label: str
    splits: list[tuple[float, int]]  # [(qty_pct_of_full, tp_index)]
    trailing_after_tp: Optional[int] = None  # tp index that activates trailing
    trailing_pct: float = 5.0

STRATEGIES: list[Strategy] = [
    Strategy(
        name="A_SELL_ALL_TP1",
        label="Sell all at TP1",
        splits=[(1.0, 0)],
    ),
    Strategy(
        name="B_TIERED_FIXED_SL",
        label="60/30/10 at TP1/TP2/TP3, fixed SL",
        splits=[(0.6, 0), (0.3, 1), (0.1, 2)],
    ),
    Strategy(
        name="C_TIERED_TRAIL_AFTER_TP1",
        label="60/30/10 at TPs, trailing 5% after TP1",
        splits=[(0.6, 0), (0.3, 1), (0.1, 2)],
        trailing_after_tp=0,
        trailing_pct=5.0,
    ),
]


def adapt_splits(target: list[tuple[float, int]],
                  num_tps: int) -> list[tuple[float, int]]:
    """If the signal has fewer TPs than the strategy needs, redistribute the
    leftover qty to the last available TP. Always returns splits summing to 1.0."""
    if num_tps == 0:
        return []
    valid = [(pct, idx) for pct, idx in target if idx < num_tps]
    if not valid:
        return [(1.0, 0)]
    used = sum(pct for pct, _ in valid)
    if used < 1.0 - 1e-9:
        last_pct, last_idx = valid[-1]
        valid[-1] = (last_pct + (1.0 - used), last_idx)
    return valid


# ----------------------------------------------------------------------------
# Symbol resolution cache
# ----------------------------------------------------------------------------

def load_symbol_cache() -> dict:
    if SYMBOL_CACHE_PATH.exists():
        try:
            return json.loads(SYMBOL_CACHE_PATH.read_text())
        except Exception:
            return {}
    return {}


def save_symbol_cache(cache: dict) -> None:
    try:
        SYMBOL_CACHE_PATH.write_text(json.dumps(cache, indent=2))
    except Exception:
        pass


def resolve_binance_symbol(client: Client, ticker: str,
                            quotes: list[str], cache: dict) -> Optional[str]:
    key = f"{ticker.upper()}|{','.join(quotes)}"
    if key in cache:
        return cache[key]
    found: Optional[str] = None
    for q in quotes:
        sym = f"{ticker.upper()}{q}"
        try:
            info = client.get_symbol_info(sym)
        except BinanceAPIException:
            continue
        except Exception:
            continue
        if info and info.get("status") == "TRADING" \
                and info.get("isSpotTradingAllowed", True):
            found = sym
            break
    cache[key] = found
    return found


# ----------------------------------------------------------------------------
# Klines: Binance spot preferred, CoinGecko fallback
# ----------------------------------------------------------------------------

def fetch_binance_klines(client: Client, symbol: str,
                          start: datetime, end: datetime) -> pd.DataFrame:
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
        logging.getLogger("backtest").warning(
            "Binance klines fetch failed %s: %s", symbol, e
        )
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
def fetch_ccxt_ohlcv(exchange_id: str, ticker: str, start_dt: datetime, end_dt: datetime, symbol: str) -> pd.DataFrame:
    """Standardized OHLCV fetcher (Fetches ALL history from launch to NOW)."""
    # 1. Smart Daily Cache
    # We stamp the cache with today's date so it only downloads the full history once per day
    safe_symbol = symbol.replace("/", "")
    today_str = datetime.now(timezone.utc).strftime("%Y%m%d")
    cache_file = KLINE_CACHE / f"ccxt_{exchange_id}_{safe_symbol}_full_history_{today_str}.parquet"
    
    if cache_file.exists():
        try:
            return pd.read_parquet(cache_file)
        except Exception:
            pass

    try:
        ex_class = getattr(ccxt, exchange_id)
        ex = ex_class({'enableRateLimit': True})
        
        markets = ex.load_markets()
        if symbol not in markets:
            return pd.DataFrame()

        # 2. Set bounds: Start from beginning of time (0), go until NOW
        since = 0 
        end_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        
        all_ohlcv = []
        
        # Pagination loop
        while since < end_ms:
            ohlcv = ex.fetch_ohlcv(symbol, timeframe='1h', since=since, limit=1000)
            if not ohlcv:
                break
                
            all_ohlcv.extend(ohlcv)
            
            # Move the 'since' pointer to the last fetched candle + 1 millisecond
            since = ohlcv[-1][0] + 1 
            
            # If the exchange returned less than 1000 candles, we've reached the present
            if len(ohlcv) < 1000:
                break
                
            time.sleep(ex.rateLimit / 1000)

        if not all_ohlcv:
            return pd.DataFrame()

        df = pd.DataFrame(all_ohlcv, columns=['open_time', 'open', 'high', 'low', 'close', 'volume'])
        df['open_time'] = pd.to_datetime(df['open_time'], unit='ms', utc=True)
        
        # 3. We removed the time filter so it keeps EVERYTHING
        final_df = df[['open_time', 'open', 'high', 'low', 'close']]
        
        # Save to Cache
        try:
            final_df.to_parquet(cache_file, index=False)
        except Exception:
            pass
            
        return final_df
    except Exception as e:
        return pd.DataFrame()

def check_single_exchange(ex_id: str, ticker: str, start: datetime, end: datetime, quote_assets: list[str], stop_event: threading.Event) -> tuple[Optional[str], pd.DataFrame]:
    """Helper function to run in a separate thread with a kill switch."""
    log = logging.getLogger("backtest")
    
    if ex_id == 'binance': 
        return None, pd.DataFrame()
        
    for quote in quote_assets:
        # THE KILL SWITCH: If another thread succeeded, stop looping immediately
        if stop_event.is_set():
            return None, pd.DataFrame()

        symbol = f"{ticker.upper()}/{quote}"
        log.info(f"   -> [Thread] Searching {ex_id.upper()} for {symbol}...")
        
        df = fetch_ccxt_ohlcv(ex_id, ticker, start, end, symbol)
        
        if not df.empty:
            log.info(f"   => [SUCCESS] Found {symbol} data on {ex_id.upper()}!")
            return f"ccxt:{ex_id}:{symbol}", df
            
    return None, pd.DataFrame()

def find_on_any_exchange(ticker: str, start: datetime, end: datetime, quote_assets: list[str]) -> tuple[Optional[str], pd.DataFrame]:
    """Scans priority exchanges concurrently using multithreading."""
    log = logging.getLogger("backtest")
    log.info(f"Initiating rapid multi-thread scan for {ticker}...")
    
    all_exchanges = PRIORITY_EXCHANGES 
    
    # Create the shared kill switch
    stop_event = threading.Event()
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        # Notice we are now passing `stop_event` to the worker
        future_to_ex = {
            executor.submit(check_single_exchange, ex_id, ticker, start, end, quote_assets, stop_event): ex_id 
            for ex_id in all_exchanges
        }
        
        for future in concurrent.futures.as_completed(future_to_ex):
            try:
                source_label, df = future.result()
                if not df.empty:
                    # TRIGGER THE KILL SWITCH for all other running threads
                    stop_event.set()
                    executor.shutdown(wait=False, cancel_futures=True)
                    return source_label, df
            except Exception as e:
                continue
                
    log.info(f"   => [FAILED] {ticker} not found on any priority exchange.")
    return None, pd.DataFrame()

def get_klines(b_client: Client, cg_client: CoinGeckoClient,
                ticker: str, quotes: list[str],
                symbol_cache: dict,
                start: datetime, end: datetime) -> tuple[Optional[str], pd.DataFrame, str]:
    
    # 1. Binance Spot (Full Year+ History)
    sym = resolve_binance_symbol(b_client, ticker, quotes, symbol_cache)
    if sym:
        df = fetch_binance_klines(b_client, sym, start, end)
        if not df.empty:
            return sym, df, "binance"

    # 2. CoinGecko (Strictly capped at 89 days to prevent it from giving Daily candles)
    coin_id = cg_client.find_id(ticker)
    if coin_id:
        cg_start = max(start, end - timedelta(days=89))
        df = cg_client.fetch_klines(coin_id, int(cg_start.timestamp()), int(end.timestamp()))
        if not df.empty:
            return f"CG:{coin_id}", df, "coingecko"

    # 3. Global CCXT Scan (Already setup to fetch & cache Full History)
    source_label, df = find_on_any_exchange(ticker, start, end, quotes)
    if not df.empty:
        return source_label, df, "ccxt_global"

    return None, pd.DataFrame(), "none"
# ----------------------------------------------------------------------------
# Simulator (parameterised by Strategy)
# ----------------------------------------------------------------------------

def simulate_with_strategy(
    klines: pd.DataFrame,
    entry_time: datetime,
    tp_values: list[float],
    tp_is_pct: bool,
    sl_price: float,
    pos_size_usdt: float,
    fee_bps: float,
    strategy: Strategy,
) -> Optional[dict]:
    after = klines[klines["open_time"] >= entry_time].reset_index(drop=True)
    if after.empty:
        return None

    entry_row = after.iloc[0]
    entry_price = float(entry_row["close"])
    if entry_price <= 0:
        return None

    # Resolve TP prices
    tp_prices: list[float] = []
    for v in tp_values:
        p = entry_price * (1 + float(v) / 100.0) if tp_is_pct else float(v)
        tp_prices.append(p)
    # Filter TPs that are above entry; keep ordering
    tp_prices = [tp for tp in tp_prices if tp > entry_price]

    if not tp_prices or sl_price >= entry_price:
        return {
            "strategy": strategy.name,
            "outcome": "INVALID",
            "entry_time": entry_row["open_time"],
            "exit_time": entry_row["open_time"],
            "entry_price": entry_price,
            "first_tp_price": tp_prices[0] if tp_prices else None,
            "sl_price": sl_price,
            "tps_in_signal": len(tp_values),
            "tps_hit": 0,
            "duration_h": 0.0,
            "pnl_usdt": 0.0,
            "pnl_pct": 0.0,
            "fees_usdt": 0.0,
        }

    splits = adapt_splits(strategy.splits, len(tp_prices))
    full_qty = pos_size_usdt / entry_price
    fee_rate = fee_bps / 10_000.0
    entry_fee = pos_size_usdt * fee_rate

    remaining_qty = full_qty
    total_proceeds = 0.0
    fees_paid = entry_fee
    fills: list[dict] = []

    # Strategy state
    next_split = 0
    trailing_active = False
    high_water = entry_price
    last_event_time = entry_row["open_time"]

    for _, row in after.iloc[1:].iterrows():
        if remaining_qty <= 1e-12:
            break
        high = float(row["high"])
        low = float(row["low"])
        if trailing_active and high > high_water:
            high_water = high

        # Effective SL: fixed initially; trailing after the configured TP is hit
        if trailing_active:
            effective_sl = high_water * (1 - strategy.trailing_pct / 100.0)
        else:
            effective_sl = sl_price

        # 1) SL hit? Sell ALL remaining at effective_sl (conservative: SL wins ties)
        if low <= effective_sl:
            exit_price = effective_sl
            proceeds = remaining_qty * exit_price
            fees_paid += proceeds * fee_rate
            total_proceeds += proceeds
            fills.append({
                "qty": remaining_qty, "price": exit_price,
                "time": str(row["open_time"]),
                "reason": "TRAIL_STOP" if trailing_active else "SL",
            })
            remaining_qty = 0
            last_event_time = row["open_time"]
            break

        # 2) Process any TPs hit in this bar (in order)
        while next_split < len(splits) and remaining_qty > 1e-12:
            qty_pct, tp_idx = splits[next_split]
            tp_target = tp_prices[tp_idx]
            if high < tp_target:
                break
            qty_to_sell = min(full_qty * qty_pct, remaining_qty)
            proceeds = qty_to_sell * tp_target
            fees_paid += proceeds * fee_rate
            total_proceeds += proceeds
            fills.append({
                "qty": qty_to_sell, "price": tp_target,
                "time": str(row["open_time"]),
                "reason": f"TP{tp_idx + 1}",
            })
            remaining_qty -= qty_to_sell
            last_event_time = row["open_time"]
            # If this TP triggers trailing, activate it; seed high_water at TP price
            if strategy.trailing_after_tp == tp_idx:
                trailing_active = True
                high_water = max(high_water, tp_target)
            next_split += 1

    # 3) If we still have remainder at the end, exit at last close (timeout)
    if remaining_qty > 1e-12:
        last = after.iloc[-1]
        exit_price = float(last["close"])
        proceeds = remaining_qty * exit_price
        fees_paid += proceeds * fee_rate
        total_proceeds += proceeds
        fills.append({
            "qty": remaining_qty, "price": exit_price,
            "time": str(last["open_time"]),
            "reason": "TIMEOUT",
        })
        last_event_time = last["open_time"]
        remaining_qty = 0

    pnl_usdt = total_proceeds - pos_size_usdt - fees_paid
    pnl_pct = pnl_usdt / pos_size_usdt
    tps_hit = sum(1 for f in fills if str(f["reason"]).startswith("TP"))
    had_sl_or_trail = any(f["reason"] in ("SL", "TRAIL_STOP") for f in fills)
    had_timeout = any(f["reason"] == "TIMEOUT" for f in fills)

    if tps_hit == 0 and had_sl_or_trail:
        outcome = "FULL_SL"
    elif tps_hit == 0 and had_timeout:
        outcome = "FULL_TIMEOUT"
    elif tps_hit > 0 and had_sl_or_trail:
        outcome = "PARTIAL+TRAIL" if trailing_active else "PARTIAL+SL"
    elif tps_hit > 0 and had_timeout:
        outcome = "PARTIAL+TIMEOUT"
    elif tps_hit == len(splits) and not had_timeout and not had_sl_or_trail:
        outcome = "ALL_TPS_FILLED"
    else:
        outcome = "PARTIAL"

    return {
        "strategy": strategy.name,
        "outcome": outcome,
        "entry_time": entry_row["open_time"],
        "entry_price": entry_price,
        "first_tp_price": tp_prices[0],
        "sl_price": sl_price,
        "exit_time": last_event_time,
        "tps_in_signal": len(tp_prices),
        "tps_hit": tps_hit,
        "duration_h": (last_event_time - entry_row["open_time"]).total_seconds() / 3600.0,
        "pnl_usdt": pnl_usdt,
        "pnl_pct": pnl_pct,
        "fees_usdt": fees_paid,
        "fills_json": json.dumps(fills),
    }


# ----------------------------------------------------------------------------
# Metrics
# ----------------------------------------------------------------------------

def compute_metrics(trades: pd.DataFrame, init_cash: float) -> dict:
    if trades.empty:
        return {"total_trades": 0, "note": "No trades simulated"}
    wins = trades[trades["pnl_usdt"] > 0]
    losses = trades[trades["pnl_usdt"] < 0]
    flat = trades[trades["pnl_usdt"] == 0]
    gp = float(wins["pnl_usdt"].sum())
    gl = float(-losses["pnl_usdt"].sum())
    net = float(trades["pnl_usdt"].sum())
    fees = float(trades["fees_usdt"].sum()) if "fees_usdt" in trades else 0.0
    pf = (gp / gl) if gl > 0 else float("inf")
    eq = trades.sort_values("exit_time").copy()
    eq["cum_pnl"] = eq["pnl_usdt"].cumsum()
    eq["equity"] = init_cash + eq["cum_pnl"]
    running_max = eq["equity"].cummax()
    dd = (eq["equity"] - running_max) / running_max
    max_dd_pct = float(dd.min() * 100) if len(dd) else 0.0
    max_dd_abs = float((eq["equity"] - running_max).min()) if len(dd) else 0.0
    eq["exit_date"] = pd.to_datetime(eq["exit_time"]).dt.date
    by_day = eq.groupby("exit_date")["pnl_usdt"].sum()
    daily_ret = by_day / init_cash
    sharpe = float(daily_ret.mean() / daily_ret.std() * math.sqrt(365)) \
        if daily_ret.std() > 0 else 0.0
    return {
        "total_trades": int(len(trades)),
        "wins": int(len(wins)),
        "losses": int(len(losses)),
        "flat": int(len(flat)),
        "win_rate_pct": float(len(wins) / len(trades) * 100),
        "gross_profit_usdt": gp,
        "gross_loss_usdt": gl,
        "profit_factor": pf if pf != float("inf") else None,
        "net_pnl_usdt": net,
        "total_fees_usdt": fees,
        "max_drawdown_pct": max_dd_pct,
        "max_drawdown_usdt": max_dd_abs,
        "sharpe_annualized": sharpe,
        "avg_win_usdt": float(wins["pnl_usdt"].mean()) if not wins.empty else 0.0,
        "avg_loss_usdt": float(losses["pnl_usdt"].mean()) if not losses.empty else 0.0,
        "avg_duration_hours": float(trades["duration_h"].mean()),
        "avg_win_duration_hours": float(wins["duration_h"].mean()) if not wins.empty else 0.0,
        "avg_loss_duration_hours": float(losses["duration_h"].mean()) if not losses.empty else 0.0,
        "by_outcome": trades["outcome"].value_counts().to_dict(),
    }


def fetch_usdt_to_aud(client: Client) -> Optional[float]:
    try:
        t = client.get_symbol_ticker(symbol="AUDUSDT")
        v = float(t["price"])
        if v > 0:
            return 1.0 / v
    except Exception:
        pass
    return None


# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------

def main() -> int:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s",
                        stream=sys.stdout)
    log = logging.getLogger("backtest")

    if not SIGNALS_PATH.exists():
        log.error("No %s - run backfill_signals.py first.", SIGNALS_PATH.name)
        return 1

    pos_size = settings.position_size_usdt if settings.uses_fixed_usdt_sizing else 6.0
    max_concurrent = settings.max_concurrent_positions or 9999
    quote_assets = settings.allowed_quote_assets
    fee_bps = settings.fee_bps or 20.0

    log.info("Config: size=%.2f USDT, max_concurrent=%d, fees=%.1f bps round-trip",
             pos_size, max_concurrent, fee_bps)
    log.info("Strategies: %s", [s.name for s in STRATEGIES])

    raw_signals = []
    with SIGNALS_PATH.open() as f:
        for line in f:
            try:
                raw_signals.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    entries = [
        r for r in raw_signals
        if r.get("kind") == "SignalKind.ENTRY"
        and r.get("ticker") and r.get("take_profits")
        and r.get("stop_loss") is not None
    ]
    entries.sort(key=lambda r: r["time"])
    log.info("Loaded %d ENTRY signals (of %d total messages)",
             len(entries), len(raw_signals))
    if not entries:
        METRICS_PATH.write_text(json.dumps({"total_trades": 0}, indent=2))
        return 0

    b_client = Client(requests_params={"timeout": BINANCE_TIMEOUT_S})
    cg_client = CoinGeckoClient(timeout=BINANCE_TIMEOUT_S)
    symbol_cache = load_symbol_cache()
    usdt_to_aud = fetch_usdt_to_aud(b_client)
    if usdt_to_aud:
        log.info("USDT->AUD rate: 1 USDT = %.4f AUD", usdt_to_aud)

    # Pre-fetch klines once per signal (shared across strategies)
    # so we don't hit the API three times per signal.
    open_positions_by_strategy: dict[str, list[dict]] = {s.name: [] for s in STRATEGIES}
    all_rows: list[dict] = []

    for i, sig in enumerate(entries, 1):
        ticker = sig["ticker"]
        sig_time = datetime.fromisoformat(sig["time"])
        if sig_time.tzinfo is None:
            sig_time = sig_time.replace(tzinfo=timezone.utc)

        # --- SMART CACHING TIMESTAMP LOGIC ---
        # 1. Start from Jan 1st of the year prior to the signal (guarantees >= 1 year of data)
        # e.g., A signal in 2026 will fetch data starting from Jan 1, 2025.
        start_time = datetime(sig_time.year - 1, 1, 1, tzinfo=timezone.utc)
        
        # 2. End at exactly 23:59:59 of TODAY
        # By fixing this to midnight, the timestamps sent to the cache file are identical 
        # all day long, ensuring the cache file name never changes!
        end_time = datetime.now(timezone.utc).replace(hour=23, minute=59, second=59, microsecond=0)

        symbol_label, klines, source = get_klines(
            b_client, cg_client, ticker, quote_assets, symbol_cache,
            start_time, end_time,
        )

        if klines.empty or symbol_label is None:
            log.info("[%d/%d] SKIP %s: no data anywhere", i, len(entries), ticker)
            continue
        if source == "coingecko":
            log.info("[%d/%d] %s via CoinGecko (alpha/non-spot)",
                     i, len(entries), ticker)

        # Run all strategies on this signal
        for strat in STRATEGIES:
            # Concurrency cap (per-strategy)
            open_positions_by_strategy[strat.name] = [
                p for p in open_positions_by_strategy[strat.name]
                if p["exit_time"] > sig_time
            ]
            if len(open_positions_by_strategy[strat.name]) >= max_concurrent:
                continue

            result = simulate_with_strategy(
                klines, sig_time,
                tp_values=list(sig["take_profits"]),
                tp_is_pct=bool(sig.get("take_profits_are_pct", False)),
                sl_price=float(sig["stop_loss"]),
                pos_size_usdt=pos_size,
                fee_bps=fee_bps,
                strategy=strat,
            )
            if result is None or result["outcome"] == "INVALID":
                continue

            row = {
                "signal_time": sig_time,
                "ticker": ticker,
                "symbol": symbol_label,
                "data_source": source,
                "is_risky": bool(sig.get("is_risky", False)),
                "is_short": bool(sig.get("is_short", False)),
                **result,
            }
            all_rows.append(row)
            open_positions_by_strategy[strat.name].append({
                "ticker": ticker, "exit_time": result["exit_time"],
            })

        if i % 10 == 0:
            log.info("Progress: %d/%d signals processed", i, len(entries))

    save_symbol_cache(symbol_cache)

    df = pd.DataFrame(all_rows)
    if df.empty:
        log.warning("No trades produced - nothing to write.")
        METRICS_PATH.write_text(json.dumps({"total_trades": 0}, indent=2))
        return 0

    for col in ("entry_time", "exit_time", "signal_time"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col])
    df = df.sort_values(["strategy", "entry_time"]).reset_index(drop=True)
    df.to_csv(RESULTS_PATH, index=False)
    log.info("Wrote %s (%d rows)", RESULTS_PATH, len(df))

    # Per-strategy metrics + comparison
    per_strategy: dict[str, dict] = {}
    for strat in STRATEGIES:
        sub = df[df["strategy"] == strat.name]
        per_strategy[strat.name] = {
            "label": strat.label,
            **compute_metrics(sub, INIT_CASH),
        }

    out = {
        "strategies": per_strategy,
        "position_size_usdt": pos_size,
        "max_concurrent": settings.max_concurrent_positions,
        "fee_bps": fee_bps,
        "init_cash": INIT_CASH,
        "usdt_to_aud": usdt_to_aud,
        "max_hold_days": MAX_HOLD_DAYS,
        "generated_at": datetime.now(tz=timezone.utc).isoformat(),
        "trailing_stop_pct": settings.trailing_stop_pct,
        "data_source_counts": df["data_source"].value_counts().to_dict(),
    }

    # Rankings
    rank = {}
    for metric, lower_is_better in [
        ("net_pnl_usdt", False),
        ("profit_factor", False),
        ("win_rate_pct", False),
        ("sharpe_annualized", False),
        ("max_drawdown_pct", True),
    ]:
        items = [(name, m.get(metric))
                  for name, m in per_strategy.items()
                  if m.get(metric) is not None]
        if items:
            items.sort(key=lambda kv: kv[1], reverse=not lower_is_better)
            rank[metric] = items[0][0]
    out["best_by"] = rank

    METRICS_PATH.write_text(json.dumps(out, indent=2, default=str))
    log.info("Wrote %s", METRICS_PATH)

    # Console summary
    print()
    print("=" * 78)
    print(" STRATEGY COMPARISON")
    print("=" * 78)
    cols = [
        ("Strategy", lambda m: ""),  # placeholder
        ("Trades", "total_trades"),
        ("Win%", "win_rate_pct"),
        ("PF", "profit_factor"),
        ("Net USDT", "net_pnl_usdt"),
        ("Max DD%", "max_drawdown_pct"),
        ("Sharpe", "sharpe_annualized"),
    ]
    header = f"{'Strategy':24s} {'Trades':>7s} {'Win%':>7s} {'PF':>7s} {'NetUSDT':>10s} {'MaxDD%':>8s} {'Sharpe':>7s}"
    print(header)
    print("-" * 78)
    for name, m in per_strategy.items():
        pf_v = m.get("profit_factor")
        print(f"{name:24s} {m.get('total_trades',0):>7d} "
              f"{m.get('win_rate_pct',0):>6.1f}% "
              f"{(f'{pf_v:.2f}' if pf_v is not None else 'inf'):>7s} "
              f"{m.get('net_pnl_usdt',0):>10.2f} "
              f"{m.get('max_drawdown_pct',0):>7.1f}% "
              f"{m.get('sharpe_annualized',0):>7.2f}")
    print("-" * 78)
    print("Best by net PnL  :", rank.get("net_pnl_usdt"))
    print("Best by Profit F :", rank.get("profit_factor"))
    print("Best by Sharpe   :", rank.get("sharpe_annualized"))
    print("Smallest drawdown:", rank.get("max_drawdown_pct"))
    print()
    print("Data sources used :", out["data_source_counts"])
    if usdt_to_aud:
        print(f"1 USDT = {usdt_to_aud:.4f} AUD")
    print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
