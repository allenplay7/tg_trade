"""Centralized config. All settings come from .env (see .env.example)."""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import List

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent
load_dotenv(PROJECT_ROOT / ".env")


def _get_bool(key: str, default: bool = False) -> bool:
    raw = os.getenv(key, str(default)).strip().lower()
    return raw in ("1", "true", "yes", "y", "on")


def _get_float(key: str, default: float) -> float:
    try:
        return float(os.getenv(key, str(default)))
    except (TypeError, ValueError):
        return default


def _get_int(key: str, default: int) -> int:
    try:
        return int(os.getenv(key, str(default)))
    except (TypeError, ValueError):
        return default


def _get_str(key: str, default: str = "") -> str:
    return (os.getenv(key, default) or "").strip()


def _get_list(key: str, default: List[str]) -> List[str]:
    raw = _get_str(key, "")
    if not raw:
        return list(default)
    return [x.strip().upper() for x in raw.split(",") if x.strip()]


@dataclass
class Settings:
    # Telegram
    tg_api_id: int = field(default_factory=lambda: _get_int("TELEGRAM_API_ID", 0))
    tg_api_hash: str = field(default_factory=lambda: _get_str("TELEGRAM_API_HASH"))
    tg_phone: str = field(default_factory=lambda: _get_str("TELEGRAM_PHONE"))
    tg_channel: str = field(default_factory=lambda: _get_str("TELEGRAM_CHANNEL"))
    notify_user_id: str = field(default_factory=lambda: _get_str("NOTIFY_USER_ID"))

    # Binance
    binance_key: str = field(default_factory=lambda: _get_str("BINANCE_API_KEY"))
    binance_secret: str = field(default_factory=lambda: _get_str("BINANCE_API_SECRET"))
    binance_testnet: bool = field(default_factory=lambda: _get_bool("BINANCE_TESTNET", True))

    # Behavior
    paper_trading: bool = field(default_factory=lambda: _get_bool("PAPER_TRADING", True))
    # Fixed-USDT sizing. If > 0, used in preference to the percent fields.
    position_size_usdt: float = field(default_factory=lambda: _get_float("POSITION_SIZE_USDT", 0))
    risky_position_size_usdt: float = field(default_factory=lambda: _get_float("RISKY_POSITION_SIZE_USDT", 0))
    # Percent-of-balance fallback when USDT fields are 0.
    position_size_pct: float = field(default_factory=lambda: _get_float("POSITION_SIZE_PCT", 3.0))
    risky_position_size_pct: float = field(default_factory=lambda: _get_float("RISKY_POSITION_SIZE_PCT", 1.0))
    # Ignore any channel message dated on/before this date (ISO YYYY-MM-DD).
    min_message_date: str = field(default_factory=lambda: _get_str("MIN_MESSAGE_DATE"))
    max_concurrent_positions: int = field(default_factory=lambda: _get_int("MAX_CONCURRENT_POSITIONS", 0))
    one_trade_per_ticker: bool = field(default_factory=lambda: _get_bool("ONE_TRADE_PER_TICKER", False))
    daily_loss_circuit_pct: float = field(default_factory=lambda: _get_float("DAILY_LOSS_CIRCUIT_PCT", 0))
    allowed_quote_assets: List[str] = field(default_factory=lambda: _get_list("ALLOWED_QUOTE_ASSETS", ["USDT"]))

    entry_order_type: str = field(default_factory=lambda: _get_str("ENTRY_ORDER_TYPE", "MARKET").upper())
    limit_entry_timeout_sec: int = field(default_factory=lambda: _get_int("LIMIT_ENTRY_TIMEOUT_SEC", 60))

    parse_mgmt_messages: bool = field(default_factory=lambda: _get_bool("PARSE_MGMT_MESSAGES", True))
    mgmt_message_window_hours: int = field(default_factory=lambda: _get_int("MGMT_MESSAGE_WINDOW_HOURS", 72))

    enable_image_ocr: bool = field(default_factory=lambda: _get_bool("ENABLE_IMAGE_OCR", False))
    tesseract_cmd: str = field(default_factory=lambda: _get_str("TESSERACT_CMD"))

    # Paths
    db_path: Path = field(default_factory=lambda: PROJECT_ROOT / "bot.sqlite3")
    session_path: Path = field(default_factory=lambda: PROJECT_ROOT / "telethon.session")
    log_path: Path = field(default_factory=lambda: PROJECT_ROOT / "bot.log")

    def validate(self, browser_mode: bool = False) -> list[str]:
        """Return list of human-readable issues with the configuration."""
        problems: list[str] = []
        if not self.tg_channel:
            problems.append("Missing TELEGRAM_CHANNEL (channel username or numeric ID)")
        if not browser_mode:
            if not self.tg_api_id or not self.tg_api_hash:
                problems.append("Missing TELEGRAM_API_ID / TELEGRAM_API_HASH (get from my.telegram.org)")
            if not self.tg_phone:
                problems.append("Missing TELEGRAM_PHONE (E.164 format, e.g. +14155551234)")
        if not self.paper_trading:
            if not self.binance_key or not self.binance_secret:
                problems.append("PAPER_TRADING is false but Binance keys are missing")
            if self.binance_key.startswith("REPLACE_") or self.binance_secret.startswith("REPLACE_"):
                problems.append("Binance keys are still REPLACE_WITH_YOUR_... placeholders")
        if self.position_size_usdt <= 0:
            if self.position_size_pct <= 0 or self.position_size_pct > 50:
                problems.append("POSITION_SIZE_PCT must be 0<x<=50 (or set POSITION_SIZE_USDT)")
        if self.entry_order_type not in ("MARKET", "LIMIT"):
            problems.append("ENTRY_ORDER_TYPE must be MARKET or LIMIT")
        return problems

    @property
    def uses_fixed_usdt_sizing(self) -> bool:
        return self.position_size_usdt > 0


settings = Settings()
