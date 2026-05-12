"""Parse Telegram messages from the signal channel into structured trade actions.

Kinds:
    ENTRY        - new spot trade: ticker + entry + at least one TP + SL
    CLOSE        - close an existing position (only if ticker explicit)
    UPDATE_SL    - move stop-loss for an existing position
    TP_HIT_INFO  - informational, notify only
    AMBIGUOUS    - looks tradable but cannot parse confidently
    IGNORE       - non-actionable commentary

Rule: anything we cannot parse with high confidence becomes AMBIGUOUS or IGNORE.
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


_TICKER_RE = re.compile(r"(?:^|[^A-Za-z0-9])\$([A-Za-z0-9]{1,15})\b")
_NUMBER_RE = re.compile(r"(?<![\d.])(\d+(?:\.\d+)?)")
_ENTRY_KW_RE = re.compile(r"\bentry\b", re.IGNORECASE)
_TP_KW_RE = re.compile(r"\b(?:tp|target|targets)\b", re.IGNORECASE)
_SL_KW_RE = re.compile(r"\b(?:sl|stop[\s-]?loss)\b", re.IGNORECASE)
_CMP_RE = re.compile(r"\bcmp\b", re.IGNORECASE)

_RISK_RE = re.compile(
    r"\b("
    r"risky|low[\s-]?cap|hype|invest\s+little|invest\s+partial(?:ly)?|"
    r"low[\s-]?float|alpha|moonshot|high[\s-]?risk|small\s+amount|"
    r"lowest\s+amount|1[\s-]?2\s*%\s*of"
    r")\b",
    re.IGNORECASE,
)
_SHORT_RE = re.compile(r"\bshort\b", re.IGNORECASE)
_CLOSE_RE = re.compile(r"\bclose\b", re.IGNORECASE)
_TP_HIT_RE = re.compile(
    r"\b(?:first\s+target|tp\s*1|target\s+(?:hit|crushed|reached)|"
    r"first\s+tp|target\s+1\s+(?:hit|crushed))\b",
    re.IGNORECASE,
)
_SET_SL_RE = re.compile(
    r"\b(?:put|set|move)\b[^.]*\b(?:sl|stoploss|stop[\s-]?loss)\b",
    re.IGNORECASE,
)


def parse(text: str) -> ParsedSignal:
    if not text or not text.strip():
        return ParsedSignal(kind=SignalKind.IGNORE, raw_text=text or "")

    sig = ParsedSignal(raw_text=text)
    sig.is_risky = bool(_RISK_RE.search(text))
    sig.is_short = bool(_SHORT_RE.search(text))
    tickers = _extract_tickers(text)

    if _looks_like_tp_hit(text):
        sig.kind = SignalKind.TP_HIT_INFO
        sig.ticker = tickers[0] if tickers else None
        return sig

    if _looks_like_close(text):
        if len(tickers) == 1:
            sig.kind = SignalKind.CLOSE
            sig.ticker = tickers[0]
        else:
            sig.kind = SignalKind.AMBIGUOUS
            sig.notes.append("Close message but ticker not explicit")
        return sig

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

    entry_match = _ENTRY_KW_RE.search(text)
    tp_match = _TP_KW_RE.search(text)
    sl_match = _SL_KW_RE.search(text)

    if not (entry_match and tp_match and sl_match):
        sig.kind = SignalKind.IGNORE
        return sig

    if sig.is_short:
        sig.kind = SignalKind.IGNORE
        sig.notes.append("SHORT - skip on spot")
        return sig

    if not tickers:
        sig.kind = SignalKind.AMBIGUOUS
        sig.notes.append("Entry/TP/SL pattern present but no $TICKER")
        return sig

    if len(tickers) > 1:
        sig.kind = SignalKind.AMBIGUOUS
        sig.ticker = tickers[0]
        sig.notes.append(f"Multiple tickers found: {tickers}")
        return sig

    sig.ticker = tickers[0]

    spans = sorted(
        [
            ("ENTRY", entry_match.start(), entry_match.end()),
            ("TP", tp_match.start(), tp_match.end()),
            ("SL", sl_match.start(), sl_match.end()),
        ],
        key=lambda x: x[1],
    )
    sections: dict[str, str] = {}
    for i, (name, _s, end) in enumerate(spans):
        next_start = spans[i + 1][1] if i + 1 < len(spans) else len(text)
        sections[name] = text[end:next_start]

    entry_section = sections.get("ENTRY", "")
    sig.entry_is_market = bool(_CMP_RE.search(entry_section))
    entry_nums = _numbers(entry_section)
    if entry_nums:
        sig.entry_price = entry_nums[0]

    tp_section = sections.get("TP", "")
    if "%" in tp_section:
        sig.take_profits = _numbers(tp_section)
        sig.take_profits_are_pct = bool(sig.take_profits)
    else:
        sig.take_profits = _numbers(tp_section)

    sl_section = sections.get("SL", "")
    sl_nums = _numbers(sl_section)
    if sl_nums:
        sig.stop_loss = sl_nums[0]

    if not sig.take_profits or sig.stop_loss is None:
        sig.kind = SignalKind.AMBIGUOUS
        sig.notes.append("Could not extract TP or SL numbers")
        return sig

    if not sig.entry_is_market and sig.entry_price is None:
        sig.kind = SignalKind.AMBIGUOUS
        sig.notes.append("Entry section had neither CMP nor a numeric price")
        return sig

    sig.kind = SignalKind.ENTRY
    return sig


def _extract_tickers(text: str) -> List[str]:
    seen: List[str] = []
    for m in _TICKER_RE.finditer(text):
        t = m.group(1).upper()
        if t not in seen:
            seen.append(t)
    return seen


def _numbers(text: str) -> List[float]:
    return [float(m.group(1)) for m in _NUMBER_RE.finditer(text)]


def _looks_like_close(text: str) -> bool:
    if not _CLOSE_RE.search(text):
        return False
    lowered = text.lower()
    if "close 70" in lowered or "don't close" in lowered or "do not close" in lowered:
        return False
    return True


def _looks_like_tp_hit(text: str) -> bool:
    return bool(_TP_HIT_RE.search(text))


def _looks_like_set_sl(text: str) -> bool:
    return bool(_SET_SL_RE.search(text))
