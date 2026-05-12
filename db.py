"""Tiny SQLite-backed log of signals, orders, and open positions."""
from __future__ import annotations

import json
import sqlite3
import time
from contextlib import contextmanager
from dataclasses import asdict
from pathlib import Path
from typing import Iterator, Optional

SCHEMA = """
CREATE TABLE IF NOT EXISTS signals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts INTEGER NOT NULL,
    tg_message_id INTEGER,
    kind TEXT NOT NULL,
    ticker TEXT,
    raw_text TEXT,
    parsed_json TEXT
);

CREATE TABLE IF NOT EXISTS positions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    opened_ts INTEGER NOT NULL,
    closed_ts INTEGER,
    ticker TEXT NOT NULL,
    symbol TEXT NOT NULL,
    qty REAL NOT NULL,
    avg_entry REAL NOT NULL,
    tp_price REAL,
    sl_price REAL,
    oco_order_list_id INTEGER,
    entry_order_id INTEGER,
    status TEXT NOT NULL,           -- OPEN, CLOSED, ERROR
    pnl_quote REAL,
    notes TEXT
);

CREATE INDEX IF NOT EXISTS idx_positions_ticker_status
    ON positions(ticker, status);

CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts INTEGER NOT NULL,
    level TEXT NOT NULL,            -- INFO, WARN, ERROR
    msg TEXT NOT NULL,
    extra_json TEXT
);
"""


class DB:
    def __init__(self, path: Path):
        self.path = path
        with self._conn() as c:
            c.executescript(SCHEMA)

    @contextmanager
    def _conn(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.path, isolation_level=None)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()

    # ---- signals ----
    def log_signal(self, tg_message_id: Optional[int], parsed) -> int:
        with self._conn() as c:
            cur = c.execute(
                "INSERT INTO signals(ts, tg_message_id, kind, ticker, raw_text, parsed_json) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (
                    int(time.time()),
                    tg_message_id,
                    str(parsed.kind),
                    parsed.ticker,
                    parsed.raw_text[:4000],
                    json.dumps(_serialize(parsed)),
                ),
            )
            return cur.lastrowid

    # ---- positions ----
    def open_position(
        self,
        *,
        ticker: str,
        symbol: str,
        qty: float,
        avg_entry: float,
        tp_price: Optional[float],
        sl_price: Optional[float],
        oco_order_list_id: Optional[int],
        entry_order_id: Optional[int],
        notes: str = "",
    ) -> int:
        with self._conn() as c:
            cur = c.execute(
                "INSERT INTO positions(opened_ts, ticker, symbol, qty, avg_entry, tp_price, "
                "sl_price, oco_order_list_id, entry_order_id, status, notes) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'OPEN', ?)",
                (
                    int(time.time()),
                    ticker,
                    symbol,
                    qty,
                    avg_entry,
                    tp_price,
                    sl_price,
                    oco_order_list_id,
                    entry_order_id,
                    notes,
                ),
            )
            return cur.lastrowid

    def close_position(self, position_id: int, pnl_quote: float, notes: str = "") -> None:
        with self._conn() as c:
            c.execute(
                "UPDATE positions SET closed_ts=?, status='CLOSED', pnl_quote=?, "
                "notes=COALESCE(notes,'') || ? WHERE id=?",
                (int(time.time()), pnl_quote, ("\n" + notes) if notes else "", position_id),
            )

    def mark_position_error(self, position_id: int, msg: str) -> None:
        with self._conn() as c:
            c.execute(
                "UPDATE positions SET status='ERROR', notes=COALESCE(notes,'') || ? WHERE id=?",
                ("\nERROR: " + msg, position_id),
            )

    def open_positions(self, ticker: Optional[str] = None) -> list[sqlite3.Row]:
        with self._conn() as c:
            if ticker is not None:
                rows = c.execute(
                    "SELECT * FROM positions WHERE status='OPEN' AND ticker=? "
                    "ORDER BY opened_ts DESC",
                    (ticker,),
                ).fetchall()
            else:
                rows = c.execute(
                    "SELECT * FROM positions WHERE status='OPEN' ORDER BY opened_ts DESC"
                ).fetchall()
            return list(rows)

    def update_position_sl(self, position_id: int, new_sl: float,
                           new_oco_order_list_id: Optional[int]) -> None:
        with self._conn() as c:
            c.execute(
                "UPDATE positions SET sl_price=?, oco_order_list_id=? WHERE id=?",
                (new_sl, new_oco_order_list_id, position_id),
            )

    # ---- events ----
    def log_event(self, level: str, msg: str, extra: Optional[dict] = None) -> None:
        with self._conn() as c:
            c.execute(
                "INSERT INTO events(ts, level, msg, extra_json) VALUES (?, ?, ?, ?)",
                (int(time.time()), level, msg, json.dumps(extra) if extra else None),
            )

    # ---- analytics ----
    def daily_realized_pnl_quote(self, since_ts: int) -> float:
        with self._conn() as c:
            row = c.execute(
                "SELECT COALESCE(SUM(pnl_quote), 0) AS p FROM positions "
                "WHERE status='CLOSED' AND closed_ts >= ?",
                (since_ts,),
            ).fetchone()
            return float(row["p"] or 0.0)


def _serialize(parsed) -> dict:
    d = asdict(parsed)
    d["kind"] = str(parsed.kind)
    return d
