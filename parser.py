"""Parse Telegram messages from the signal channel into structured trade actions.

Kinds:
    ENTRY        - new spot trade: ticker + entry + at least one TP (SL optional, will auto-fill)
    CLOSE        - close an existing position (only if ticker explicit)
    UPDATE_SL    - move stop-loss for an existing position
    TP_HIT_INFO  - informational, notify only
    AMBIGUOUS    - looks tradable but cannot parse confidently
    IGNORE       - non-actionable commentary
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional


class SignalKind(str, Enum):
    ENTRY = "ENTRY"
    CLOSE = "CLOSE"
    UPDATE_SL = "UPDATE_SL"
    TP_HIT_INFO = "TP_HIT_INFO"
    AMBIGUOUS = "AMBIGUOUS"
    IGNORE = "IGNORE"


@dataclass
class ParsedSignal:
    kind: SignalKind = SignalKind.IGNORE
    ticker: Optional[str] = None
    entry_is_market: bool = False
    entry_price: Optional[float] = None
    take_profits: List[float] = field(default_factory=list)
    take_profits_are_pct: bool = False
    stop_loss: Optional[float] = None
    is_risky: bool = False
    is_short: bool = False
    raw_text: str = ""
    notes: List[str] = field(default_factory=list)

    @property
    def actionable(self) -> bool:
        return self.kind in (SignalKind.ENTRY, SignalKind.CLOSE, SignalKind.UPDATE_SL)


# Improved Regex: Now catches $TICKER or "BUY TICKER" or "TICKER/USDT"
_TICKER_RE = re.compile(r"(?:\$|(?:\b(?:BUY|LONG|TRADE)\s+))([A-Z0-9]{2,12})\b", re.IGNORECASE)
_NUMBER_RE = re.compile(r"(?<![\d.])(\d+(?:\.\d+)?)")
_ENTRY_KW_RE = re.compile(r"\b(?:entry|entries|buying|buy\s+at)\b", re.IGNORECASE)
_TP_KW_RE = re.compile(r"\b(?:tp|target|targets|take[\s-]?profit)\b", re.IGNORECASE)
_SL_KW_RE = re.compile(r"\b(?:sl|stop[\s-]?loss|stop)\b", re.IGNORECASE)
_CMP_RE = re.compile(r"\b(?:cmp|current\s+price|market\s+price|now)\b", re.IGNORECASE)

_RISK_RE = re.compile(
    r"\b("
    r"risky|low[\s-]?cap|hype|invest\s+little|invest\s+partial(?:ly)?|"
    r"low[\s-]?float|alpha|moonshot|high[\s-]?risk|small\s+amount|"
    r"lowest\s+amount|1[\s-]?2\s*%\s*of"
    r")\b",
    re.IGNORECASE,
)
_SHORT_RE = re.compile(r"\b(?:short|sell|future)\b", re.IGNORECASE)
_CLOSE_RE = re.compile(r"\b(?:close|exit|sold|sell\s+all)\b", re.IGNORECASE)
_TP_HIT_RE = re.compile(
    r"\b(?:first\s+target|tp\s*1|target\s+(?:hit|crushed|reached)|"
    r"first\s+tp|target\s+1\s+(?:hit|crushed))\b",
    re.IGNORECASE,
)
_SET_SL_RE = re.compile(
    r"\b(?:put|set|move)\b[^.]*\b(?:sl|stoploss|stop[\s-]?loss)\b",
    re.IGNORECASE,
)

DEFAULT_SL_PERCENT = 0.10  # 10% auto-stop loss if missing

def parse(text: str) -> ParsedSignal:
    if not text or not text.strip():
        return ParsedSignal(kind=SignalKind.IGNORE, raw_text=text or "")

    sig = ParsedSignal(raw_text=text)
    sig.is_risky = bool(_RISK_RE.search(text))
    sig.is_short = bool(_SHORT_RE.search(text))
    tickers = _extract_tickers(text)

    # 1. Informational TP Hits
    if _looks_like_tp_hit(text):
        sig.kind = SignalKind.TP_HIT_INFO
        sig.ticker = tickers[0] if tickers else None
        return sig

    # 2. Close Signal
    if _looks_like_close(text):
        if len(tickers) == 1:
            sig.kind = SignalKind.CLOSE
            sig.ticker = tickers[0]
        else:
            sig.kind = SignalKind.AMBIGUOUS
            sig.notes.append("Close message but ticker not explicit")
        return sig

    # 3. Stop Loss Update
    if _looks_like_set_sl(text):
        nums = _numbers(text)
        if len(tickers) == 1 and nums:
            sig.kind = SignalKind.UPDATE_SL
            sig.ticker = tickers[0]
            sig.stop_loss = nums[-1]
        else:
            sig.kind = SignalKind.AMBIGUOUS
            sig.notes.append("SL update but ticker or price not explicit")
        return sig

    # 4. Entry Detection
    entry_match = _ENTRY_KW_RE.search(text)
    tp_match = _TP_KW_RE.search(text)
    sl_match = _SL_KW_RE.search(text)

    # If it doesn't have at least Entry and TP, it's commentary
    if not (entry_match and tp_match):
        sig.kind = SignalKind.IGNORE
        return sig

    if sig.is_short:
        sig.kind = SignalKind.IGNORE
        sig.notes.append("SHORT - skip on spot")
        return sig

    if not tickers:
        sig.kind = SignalKind.AMBIGUOUS
        sig.notes.append("Entry/TP pattern present but no ticker found")
        return sig

    sig.ticker = tickers[0]
    if len(tickers) > 1:
        sig.notes.append(f"Multiple tickers found: {tickers}, using first.")

    # Sectional Extraction
    # Logic: Identify boundaries of Entry, TP, and SL sections
    found_segments = [("ENTRY", entry_match.start())]
    if tp_match: found_segments.append(("TP", tp_match.start()))
    if sl_match: found_segments.append(("SL", sl_match.start()))
    
    found_segments.sort(key=lambda x: x[1])
    
    sections = {}
    for i in range(len(found_segments)):
        label, start = found_segments[i]
        end = found_segments[i+1][1] if i+1 < len(found_segments) else len(text)
        sections[label] = text[start:end]

    # Parse Entry
    entry_text = sections.get("ENTRY", "")
    sig.entry_is_market = bool(_CMP_RE.search(entry_text))
    entry_nums = _numbers(entry_text)
    if entry_nums:
        sig.entry_price = entry_nums[0]

    # Parse TP
    tp_text = sections.get("TP", "")
    sig.take_profits = _numbers(tp_text)
    sig.take_profits_are_pct = "%" in tp_text

    # Parse SL
    sl_text = sections.get("SL", "")
    sl_nums = _numbers(sl_text)
    if sl_nums:
        sig.stop_loss = sl_nums[0]

    # Validation & Post-Processing
    if not sig.take_profits:
        sig.kind = SignalKind.AMBIGUOUS
        sig.notes.append("Could not extract any TP numbers")
        return sig

    if not sig.entry_is_market and sig.entry_price is None:
        sig.kind = SignalKind.AMBIGUOUS
        sig.notes.append("No entry price or CMP found")
        return sig

    # Handle Missing Stop Loss
    if sig.stop_loss is None:
        if sig.entry_price:
            # Fallback SL = Entry - 10%
            sig.stop_loss = round(sig.entry_price * (1 - DEFAULT_SL_PERCENT), 8)
            sig.notes.append(f"Missing SL: Auto-generated at {DEFAULT_SL_PERCENT*100}% below entry")
        else:
            # If it's CMP and no price listed, we can't calculate SL yet
            sig.notes.append("Missing SL: Could not auto-calc because entry price is CMP")

    sig.kind = SignalKind.ENTRY
    return sig


def _extract_tickers(text: str) -> List[str]:
    seen: List[str] = []
    # Check for $TICKER first
    for m in re.finditer(r"\$([A-Z0-9]{2,12})\b", text, re.I):
        t = m.group(1).upper()
        if t not in seen: seen.append(t)
    
    # Fallback to keyword-based ticker detection if none found with $
    if not seen:
        for m in _TICKER_RE.finditer(text):
            t = m.group(1).upper()
            if t not in seen: seen.append(t)
            
    return seen


def _numbers(text: str) -> List[float]:
    return [float(m.group(1)) for m in _NUMBER_RE.finditer(text)]


def _looks_like_close(text: str) -> bool:
    if not _CLOSE_RE.search(text):
        return False
    lowered = text.lower()
    if "don't close" in lowered or "do not close" in lowered:
        return False
    return True


def _looks_like_tp_hit(text: str) -> bool:
    return bool(_TP_HIT_RE.search(text))


def _looks_like_set_sl(text: str) -> bool:
    return bool(_SET_SL_RE.search(text))