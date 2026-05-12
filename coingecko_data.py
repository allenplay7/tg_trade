"""CoinGecko data fallback for tickers without a Binance spot pair.

Used for Binance Alpha tokens / pre-listing tokens. Free CoinGecko API has
rate limits (~30 req/min) which we throttle to. Historical data is price-only
(not OHLC) so we approximate by treating each price point as both high+low.

Cache:
  coingecko_coins.json - full list of coins (id, symbol, name), refreshed weekly
  cg_cache/<id>_<start>_<end>.parquet - cached price series per token+range

Usage:
    client = CoinGeckoClient()
    coin_id = client.find_id("BARD")  # search by ticker
    df = client.fetch_klines(coin_id, start_ts, end_ts)
"""
from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

log = logging.getLogger("coingecko")

CG_BASE = "https://api.coingecko.com/api/v3"
PROJECT_ROOT = Path(__file__).resolve().parent
COIN_LIST_CACHE = PROJECT_ROOT / "coingecko_coins.json"
CG_KLINE_CACHE = PROJECT_ROOT / "cg_cache"
LIST_CACHE_TTL_DAYS = 7
RATE_LIMIT_SLEEP_S = 2.5  # ~24 req/min, under the 30 req/min free limit


# Tokens we *know* aren't on CoinGecko or are noisy duplicates - skip lookup.
SKIP_TICKERS = {
    "USELESS", "WET", "TRIA", "COLLECT", "PLUM",
}

# Prefer these CG ids when there's symbol collision (multiple coins with same
# symbol). Add to this map as you discover collisions.
TICKER_OVERRIDE = {
    "BTC": "bitcoin",
    "ETH": "ethereum",
    "BNB": "binancecoin",
    "SOL": "solana",
    "ATH": "aethir",
    "AXS": "axie-infinity",
    "NEAR": "near",
    "ICP": "internet-computer",
    "PHA": "pha",
    "DASH": "dash",
    "BAT": "basic-attention-token",
    "ENJ": "enjincoin",
    "ZRX": "0x",
    "ETC": "ethereum-classic",
}


class CoinGeckoClient:
    def __init__(self, timeout: int = 10):
        self.timeout = timeout
        self._coins: Optional[list] = None
        self._last_req_ts = 0.0
        CG_KLINE_CACHE.mkdir(exist_ok=True)

    def _throttle(self) -> None:
        elapsed = time.time() - self._last_req_ts
        if elapsed < RATE_LIMIT_SLEEP_S:
            time.sleep(RATE_LIMIT_SLEEP_S - elapsed)
        self._last_req_ts = time.time()

    def _load_coin_list(self) -> list:
        if COIN_LIST_CACHE.exists():
            age_days = (time.time() - COIN_LIST_CACHE.stat().st_mtime) / 86400
            if age_days < LIST_CACHE_TTL_DAYS:
                try:
                    return json.loads(COIN_LIST_CACHE.read_text())
                except json.JSONDecodeError:
                    pass
        log.info("Fetching CoinGecko coin list...")
        self._throttle()
        try:
            resp = requests.get(f"{CG_BASE}/coins/list", timeout=self.timeout)
        except Exception as e:
            log.warning("CoinGecko coins/list failed: %s", e)
            return []
        if resp.status_code != 200:
            log.warning("CoinGecko coins/list returned %s", resp.status_code)
            return []
        coins = resp.json()
        try:
            COIN_LIST_CACHE.write_text(json.dumps(coins))
        except Exception:
            pass
        log.info("Cached %d CoinGecko coins", len(coins))
        return coins

    def find_id(self, ticker: str) -> Optional[str]:
        """Return CoinGecko id for ticker (e.g. 'BARD' -> 'bardcoin')."""
        t = ticker.upper()
        if t in SKIP_TICKERS:
            return None
        if t in TICKER_OVERRIDE:
            return TICKER_OVERRIDE[t]
        if self._coins is None:
            self._coins = self._load_coin_list()
        if not self._coins:
            return None
        # Find all coins whose symbol matches (case-insensitive).
        matches = [c for c in self._coins if c.get("symbol", "").upper() == t]
        if not matches:
            return None
        # Heuristic: prefer the shortest id (usually the canonical / most-popular
        # token; longer ids are typically forks like "wrapped-X", "bridged-X").
        matches.sort(key=lambda c: len(c.get("id", "")))
        return matches[0]["id"]

    def fetch_klines(self, coin_id: str, start_ts: int, end_ts: int) -> pd.DataFrame:
        """Return DataFrame with open_time, open, high, low, close columns.

        Free CoinGecko market_chart resolution:
            <1 day range  -> 5-min granularity
            1-90 days     -> hourly
            >90 days      -> daily
        We only have price (not OHLC), so we set open=high=low=close=price.
        """
        cache_file = CG_KLINE_CACHE / f"{coin_id}_{start_ts}_{end_ts}.parquet"
        if cache_file.exists():
            try:
                return pd.read_parquet(cache_file)
            except Exception:
                cache_file.unlink(missing_ok=True)

        self._throttle()
        url = f"{CG_BASE}/coins/{coin_id}/market_chart/range"
        params = {"vs_currency": "usd", "from": start_ts, "to": end_ts}
        try:
            resp = requests.get(url, params=params, timeout=self.timeout)
        except Exception as e:
            log.warning("CoinGecko market_chart failed for %s: %s", coin_id, e)
            return pd.DataFrame()
        if resp.status_code != 200:
            if resp.status_code == 429:
                log.warning("CoinGecko rate limit hit; backing off")
                time.sleep(10)
            log.info("CoinGecko %s returned %s", coin_id, resp.status_code)
            return pd.DataFrame()
        data = resp.json()
        prices = data.get("prices", [])
        if not prices:
            return pd.DataFrame()
        df = pd.DataFrame(prices, columns=["ts_ms", "price"])
        df["open_time"] = pd.to_datetime(df["ts_ms"], unit="ms", utc=True)
        df["open"] = df["price"].astype(float)
        df["high"] = df["price"].astype(float)
        df["low"] = df["price"].astype(float)
        df["close"] = df["price"].astype(float)
        df = df[["open_time", "open", "high", "low", "close"]].copy()
        try:
            df.to_parquet(cache_file, index=False)
        except Exception:
            pass
        return df
