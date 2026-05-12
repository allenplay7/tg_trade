"""Backfill historical signals from the Telegram channel for backtesting.

INTERACTIVE: prompts you for how far back to fetch when launched.

Reuses the existing browser_session/ (so no fresh login). Opens the channel,
scrolls up until it has captured the requested number of days of messages,
parses each through parser.py, and saves to backtest_signals.jsonl.

DATE EXTRACTION:
  Telegram Web K renders message-group date dividers (e.g. "May 11", "Yesterday",
  "Today") between bubbles in document order. We walk the DOM linearly, track
  the last-seen divider, and stamp each bubble with that date. This is more
  reliable than parsing per-bubble title attributes (which often hold time only).

SAFETY:
  - MAX_ROUNDS: hard stop after 300 scroll rounds.
  - MAX_CAPTURED: hard stop after 10000 messages.

IMPORTANT: stop main_browser.py before running this - the persistent context
locks browser_session/.
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from dateutil import parser as dt_parser
from playwright.async_api import async_playwright

from config import settings
from parser import parse, SignalKind

PROJECT_ROOT = Path(__file__).resolve().parent
SESSION_DIR = PROJECT_ROOT / "browser_session"
OUT_PATH = PROJECT_ROOT / "backtest_signals.jsonl"

MAX_ROUNDS = 300
MAX_CAPTURED = 10_000
SCROLL_DELAY_S = 2.5

_YEAR_RE = re.compile(r"\b(?:19|20)\d{2}\b")

_CAPTURE_JS = """
() => {
    const nodes = document.querySelectorAll(
        '.bubble[data-mid], .bubbles-date, .bubbles-date-group-name, '
        + '.bubbles-date-group .service, .is-date, .bubble.service.is-date, '
        + '.bubble-content .service'
    );
    const out = [];
    let currentDate = '';

    const isDateDivider = (el) => {
        if (!el || !el.classList) return false;
        const cls = el.classList;
        if (cls.contains('bubbles-date')) return true;
        if (cls.contains('bubbles-date-group-name')) return true;
        if (cls.contains('is-date')) return true;
        if (cls.contains('service') && !(el.dataset && el.dataset.mid)) {
            const txt = (el.innerText || '').trim();
            if (txt.length > 0 && txt.length <= 25
                && !txt.toLowerCase().includes('joined')
                && !txt.toLowerCase().includes('left')
                && !txt.toLowerCase().includes('pinned')) {
                return true;
            }
        }
        return false;
    };

    for (const el of nodes) {
        if (isDateDivider(el)) {
            const t = (el.innerText || '').trim().replace(/\s+/g, ' ');
            if (t) currentDate = t;
            continue;
        }
        const mid = el.dataset && el.dataset.mid;
        if (!mid) continue;

        const timeEl = el.querySelector('.bubble-time .time-inner')
                    || el.querySelector('.time-inner')
                    || el.querySelector('.bubble-time')
                    || el.querySelector('.time');
        const timeTitle = timeEl
            ? (timeEl.title || timeEl.getAttribute('title') || '')
            : '';
        const timeText = timeEl ? (timeEl.innerText || '').trim() : '';

        let dateFromGroup = '';
        const groupEl = el.closest('.bubbles-date-group')
                     || el.closest('section.bubbles-date-group');
        if (groupEl) {
            const h = groupEl.querySelector(
                '.bubbles-date, .bubbles-date-group-name, .is-date'
            );
            if (h) dateFromGroup = (h.innerText || '').trim().replace(/\s+/g, ' ');
        }

        const msgEl = el.querySelector('.message')
                   || el.querySelector('.message-text')
                   || el.querySelector('.translatable-message');
        const text = (msgEl ? msgEl.innerText : el.innerText || '').trim();
        const hasImage = !!el.querySelector('img, video, .media-photo, canvas');

        out.push({
            mid: mid,
            text,
            time_title: timeTitle,
            time_text: timeText,
            date_text: dateFromGroup || currentDate || '',
            has_image: hasImage,
        });
    }
    return out;
}
"""

_SCROLL_UP_JS = """
() => {
    const sels = ['.bubbles .scrollable-y', '.scrollable-y', '.bubbles-inner', '.bubbles'];
    for (const s of sels) {
        const el = document.querySelector(s);
        if (el) { el.scrollTop = 0; return true; }
    }
    return false;
}
"""


def parse_bubble_datetime(rec: dict, now: datetime) -> Optional[datetime]:
    today = now.date()
    yesterday = today - timedelta(days=1)
    date_text = (rec.get("date_text") or "").strip()
    time_text = (rec.get("time_text") or "").strip()
    time_title = (rec.get("time_title") or "").strip()

    if time_title and len(time_title) >= 8:
        try:
            dt = dt_parser.parse(
                time_title, fuzzy=True,
                default=datetime(today.year, today.month, today.day),
            )
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            if 2020 <= dt.year <= today.year + 1:
                return dt
        except (ValueError, dt_parser.ParserError, OverflowError):
            pass

    if not date_text:
        return None
    low = date_text.lower()
    if low == "today":
        d = today
    elif low == "yesterday":
        d = yesterday
    else:
        try:
            parsed = dt_parser.parse(
                date_text, fuzzy=True,
                default=datetime(today.year, 1, 1),
            )
            d = parsed.date()
        except (ValueError, dt_parser.ParserError, OverflowError):
            return None
        if d > today and not _YEAR_RE.search(date_text):
            try:
                d = d.replace(year=d.year - 1)
            except ValueError:
                pass

    if time_text:
        try:
            t = dt_parser.parse(
                time_text, fuzzy=True,
                default=datetime(today.year, today.month, today.day),
            ).time()
        except (ValueError, dt_parser.ParserError, OverflowError):
            t = datetime.min.time()
    else:
        t = datetime.min.time()

    return datetime(d.year, d.month, d.day, t.hour, t.minute, t.second,
                    tzinfo=timezone.utc)


def prompt_days() -> int:
    print()
    print("=" * 60)
    print(" Signal Channel Backfill")
    print("=" * 60)
    print()
    print("How many days of channel history should I fetch?")
    print("  Examples:  7 = last week, 30 = last month, 90 = 3 months.")
    print()
    while True:
        raw = input("Days back to fetch [default 30]: ").strip()
        if raw == "":
            return 30
        try:
            n = int(raw)
        except ValueError:
            print(f"  '{raw}' is not a number, try again.")
            continue
        if n < 1 or n > 365:
            print("  Please enter a number between 1 and 365.")
            continue
        return n


async def run(days: int) -> int:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")
    log = logging.getLogger("backfill")

    if not settings.tg_channel:
        log.error("TELEGRAM_CHANNEL not set in .env")
        return 1

    now = datetime.now(tz=timezone.utc)
    target_date = now - timedelta(days=days)
    log.info("Target cutoff: messages newer than %s (last %d days)",
             target_date.isoformat(), days)
    log.info("Hard safety stops: %d rounds, %d messages.",
             MAX_ROUNDS, MAX_CAPTURED)

    channel = settings.tg_channel.strip()
    if not channel.startswith("-") and not channel.startswith("@"):
        channel = "@" + channel
    tg_url = f"https://web.telegram.org/k/#{channel}"

    captured: dict[str, dict] = {}
    captured_dt: dict[str, Optional[datetime]] = {}
    rounds = 0
    rounds_without_new = 0

    async with async_playwright() as pw:
        log.info("Launching browser (headed)...")
        ctx = await pw.chromium.launch_persistent_context(
            str(SESSION_DIR),
            headless=False,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
            viewport={"width": 1280, "height": 900},
        )
        page = ctx.pages[0] if ctx.pages else await ctx.new_page()

        log.info("Navigating to %s", tg_url)
        await page.goto(tg_url, wait_until="domcontentloaded")
        try:
            await page.wait_for_selector(".bubbles, .chat-input", timeout=120_000)
        except Exception:
            log.error("Could not load Telegram Web - log in once first via main_browser.py")
            await ctx.close()
            return 1
        await asyncio.sleep(3)

        while rounds < MAX_ROUNDS and len(captured) < MAX_CAPTURED:
            rounds += 1
            bubbles = await page.evaluate(_CAPTURE_JS)
            new_in_round = 0
            for b in bubbles:
                mid = b["mid"]
                if mid in captured:
                    continue
                captured[mid] = b
                captured_dt[mid] = parse_bubble_datetime(b, now)
                new_in_round += 1

            dated = [dt for dt in captured_dt.values() if dt is not None]
            oldest = min(dated) if dated else None
            dated_count = len(dated)
            undated_count = len(captured) - dated_count

            log.info(
                "Round %d: +%d new (total %d, %d dated, %d undated). Oldest: %s",
                rounds, new_in_round, len(captured),
                dated_count, undated_count,
                oldest.isoformat() if oldest else "(none yet)",
            )

            if oldest is not None and oldest <= target_date:
                log.info("Reached target cutoff date - stopping.")
                break

            if new_in_round == 0:
                rounds_without_new += 1
                if rounds_without_new >= 5:
                    log.info("No new messages after 5 rounds - likely at channel start.")
                    break
            else:
                rounds_without_new = 0

            await page.evaluate(_SCROLL_UP_JS)
            await page.keyboard.press("Home")
            await asyncio.sleep(SCROLL_DELAY_S)

        if rounds >= MAX_ROUNDS:
            log.warning("Hit MAX_ROUNDS safety limit (%d)", MAX_ROUNDS)
        if len(captured) >= MAX_CAPTURED:
            log.warning("Hit MAX_CAPTURED safety limit (%d)", MAX_CAPTURED)

        await ctx.close()

    actionable = 0
    dropped_no_date = 0
    dropped_too_old = 0
    written = 0
    with OUT_PATH.open("w", encoding="utf-8") as f:
        for mid, b in sorted(captured.items(), key=lambda kv: int(kv[0])):
            dt = captured_dt.get(mid)
            if dt is None:
                dropped_no_date += 1
                continue
            if dt < target_date:
                dropped_too_old += 1
                continue
            parsed = parse(b["text"])
            record = {
                "mid": mid,
                "text": b["text"],
                "time": dt.isoformat(),
                "raw_date_text": b.get("date_text"),
                "raw_time_text": b.get("time_text"),
                "has_image": b["has_image"],
                "kind": str(parsed.kind),
                "ticker": parsed.ticker,
                "entry_is_market": parsed.entry_is_market,
                "entry_price": parsed.entry_price,
                "take_profits": parsed.take_profits,
                "take_profits_are_pct": parsed.take_profits_are_pct,
                "stop_loss": parsed.stop_loss,
                "is_risky": parsed.is_risky,
                "is_short": parsed.is_short,
            }
            f.write(json.dumps(record) + "\n")
            written += 1
            if parsed.kind == SignalKind.ENTRY:
                actionable += 1

    log.info("Saved %d messages to %s", written, OUT_PATH)
    log.info("  Actionable ENTRY signals: %d", actionable)
    log.info("  Dropped (could not parse date): %d", dropped_no_date)
    log.info("  Dropped (older than target): %d", dropped_too_old)
    print()
    print("Done. Next step: python backtest.py")
    return 0


def main() -> int:
    days = prompt_days()
    return asyncio.run(run(days))


if __name__ == "__main__":
    sys.exit(main())
