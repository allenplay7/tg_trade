"""Backfill historical signals from the Telegram channel for backtesting.

INTERACTIVE: prompts you for how far back to fetch when launched.

CACHE LAYER:
  signals_cache.jsonl is a persistent, append-only master cache of every
  message we've ever seen. backtest_signals.jsonl is the filtered view for
  the current run.
  - If the master cache already covers the requested date range, NO browser
    is opened - we just filter and write the output file.
  - If the cache is partial, we pre-populate the captured dict from it (so
    the observer dedupes) and scroll only enough to fill the gap.
  - The cache grows monotonically over runs.

DATE EXTRACTION:
  Walk DOM in document order, track .bubbles-date dividers, stamp each
  bubble with the most recent divider, then parse with python-dateutil.

SAFETY:
  - MAX_ROUNDS=300, MAX_CAPTURED=10000

IMPORTANT: stop main_browser.py before running this.
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
from parser import parse

PROJECT_ROOT = Path(__file__).resolve().parent
SESSION_DIR = PROJECT_ROOT / "browser_session"
CACHE_PATH = PROJECT_ROOT / "signals_cache.jsonl"
OUT_PATH = PROJECT_ROOT / "backtest_signals.jsonl"

MAX_ROUNDS = 300
MAX_CAPTURED = 10_000
SCROLL_DELAY_S = 1.5

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
        const timeTitle = timeEl ? (timeEl.title || timeEl.getAttribute('title') || '') : '';
        const timeText = timeEl ? (timeEl.innerText || '').trim() : '';
        let dateFromGroup = '';
        const groupEl = el.closest('.bubbles-date-group')
                     || el.closest('section.bubbles-date-group');
        if (groupEl) {
            const h = groupEl.querySelector('.bubbles-date, .bubbles-date-group-name, .is-date');
            if (h) dateFromGroup = (h.innerText || '').trim().replace(/\s+/g, ' ');
        }
        const msgEl = el.querySelector('.message')
                   || el.querySelector('.message-text')
                   || el.querySelector('.translatable-message');
        const text = (msgEl ? msgEl.innerText : el.innerText || '').trim();
        const hasImage = !!el.querySelector('img, video, .media-photo, canvas');
        out.push({
            mid: mid, text,
            time_title: timeTitle, time_text: timeText,
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
            dt = dt_parser.parse(time_title, fuzzy=True,
                                 default=datetime(today.year, today.month, today.day))
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
            parsed = dt_parser.parse(date_text, fuzzy=True,
                                     default=datetime(today.year, 1, 1))
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
            t = dt_parser.parse(time_text, fuzzy=True,
                                default=datetime(today.year, today.month, today.day)).time()
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
    print("  Cached data will be re-used; only the gap is fetched.")
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


def load_cache(path: Path) -> tuple[dict, dict]:
    cache, cache_dt = {}, {}
    source = path if path.exists() else OUT_PATH
    if not source.exists():
        return cache, cache_dt
    with source.open(encoding="utf-8") as f:
        for line in f:
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            mid = rec.get("mid")
            if not mid:
                continue
            cache[mid] = rec
            t = rec.get("time")
            if t:
                try:
                    dt = datetime.fromisoformat(t)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    cache_dt[mid] = dt
                except ValueError:
                    cache_dt[mid] = None
            else:
                cache_dt[mid] = None
    return cache, cache_dt


def make_record(mid: str, b: dict, dt: Optional[datetime]) -> dict:
    parsed = parse(b.get("text") or "")
    return {
        "mid": mid,
        "text": b.get("text", ""),
        "time": dt.isoformat() if dt else None,
        "raw_date_text": b.get("date_text"),
        "raw_time_text": b.get("time_text"),
        "has_image": bool(b.get("has_image")),
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


def write_jsonl(path: Path, records: list[dict]) -> None:
    with path.open("w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec) + "\n")


async def run(days: int) -> int:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")
    log = logging.getLogger("backfill")
    if not settings.tg_channel:
        log.error("TELEGRAM_CHANNEL not set in .env")
        return 1
    now = datetime.now(tz=timezone.utc)
    target_date = now - timedelta(days=days)
    log.info("Target: messages newer than %s (last %d days)",
             target_date.isoformat(), days)

    cache, cache_dt = load_cache(CACHE_PATH)
    log.info("Loaded %d cached messages", len(cache))
    cached_dated = [dt for dt in cache_dt.values() if dt is not None]
    oldest_cached = min(cached_dated) if cached_dated else None
    newest_cached = max(cached_dated) if cached_dated else None
    if oldest_cached:
        log.info("Cache covers: %s ... %s",
                 oldest_cached.isoformat(), newest_cached.isoformat())

    cache_covers = oldest_cached is not None and oldest_cached <= target_date
    captured = dict(cache)
    captured_dt = dict(cache_dt)

    if cache_covers:
        log.info("Cache already covers target range - NO BROWSER NEEDED.")
        log.info("(Delete signals_cache.jsonl if you want a fresh refresh.)")
    else:
        if oldest_cached:
            log.info("Cache partial; scrolling only until target reached.")
        else:
            log.info("No cache - scrolling from scratch.")

        async with async_playwright() as pw:
            log.info("Launching browser (headed)...")
            ctx = await pw.chromium.launch_persistent_context(
                str(SESSION_DIR),
                headless=False,
                args=["--no-sandbox", "--disable-dev-shm-usage"],
                viewport={"width": 1280, "height": 900},
            )
            page = ctx.pages[0] if ctx.pages else await ctx.new_page()
            channel = settings.tg_channel.strip()
            if not channel.startswith("-") and not channel.startswith("@"):
                channel = "@" + channel
            tg_url = f"https://web.telegram.org/k/#{channel}"
            log.info("Navigating to %s", tg_url)
            await page.goto(tg_url, wait_until="domcontentloaded")
            try:
                await page.wait_for_selector(".bubbles, .chat-input", timeout=120_000)
            except Exception:
                log.error("Could not load Telegram Web - log in once first.")
                await ctx.close()
                return 1
            await asyncio.sleep(3)
            rounds = 0
            rounds_without_new = 0
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
                log.info(
                    "Round %d: +%d new (total %d). Oldest: %s",
                    rounds, new_in_round, len(captured),
                    oldest.isoformat() if oldest else "(none)",
                )
                if oldest is not None and oldest <= target_date:
                    log.info("Reached target - stopping.")
                    break
                if new_in_round == 0:
                    rounds_without_new += 1
                    if rounds_without_new >= 20:
                        log.info("No new after 20 rounds - at channel start.")
                        break
                else:
                    rounds_without_new = 0
                await page.evaluate(_SCROLL_UP_JS)
                await page.keyboard.press("Home")
                await asyncio.sleep(SCROLL_DELAY_S)
            if rounds >= MAX_ROUNDS:
                log.warning("Hit MAX_ROUNDS safety limit")
            if len(captured) >= MAX_CAPTURED:
                log.warning("Hit MAX_CAPTURED safety limit")
            await ctx.close()

    all_records = []
    for mid in sorted(captured.keys(), key=lambda m: int(m)):
        b = captured[mid]
        dt = captured_dt.get(mid)
        if mid in cache and "kind" in cache[mid] and cache[mid].get("time"):
            all_records.append(cache[mid])
        else:
            all_records.append(make_record(mid, b, dt))

    write_jsonl(CACHE_PATH, all_records)
    log.info("Cache updated: %s (%d total messages)", CACHE_PATH.name, len(all_records))

    in_range = [r for r in all_records
                if r.get("time") and datetime.fromisoformat(r["time"]) >= target_date]
    actionable = sum(1 for r in in_range if r.get("kind") == "SignalKind.ENTRY")
    write_jsonl(OUT_PATH, in_range)
    log.info("Filtered for backtest: %s (%d msgs, %d actionable ENTRY)",
             OUT_PATH.name, len(in_range), actionable)

    print()
    print("Done. Next step: python backtest.py")
    return 0


def main() -> int:
    days = prompt_days()
    return asyncio.run(run(days))


if __name__ == "__main__":
    sys.exit(main())
