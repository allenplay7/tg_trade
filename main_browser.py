"""Entry point (browser mode) - keeps running 24/7.

Architecture:
    main_loop()    outer restart-on-failure loop with exponential backoff
        run_bot()  one Chromium session: launch -> listen -> dispatch -> close

If anything inside run_bot raises or the browser context closes, the outer loop
logs the reason and starts a fresh browser within a few seconds. The persistent
context (./browser_session/) keeps you logged in across restarts.

Recoverable failure modes handled:
- Chromium process crash       -> outer loop restarts
- Browser window closed        -> outer loop restarts
- Page navigated outside       -> recover_from_navigation() navigates back
- A different chat selected    -> recover_from_navigation() navigates back
- Observer JS lost on reload   -> watchdog reinstalls it
- Stale SingletonLock          -> cleaned up before each launch

NOT handled (need OS-level config):
- Computer sleep/hibernate     -> set Windows power plan to never sleep
- Windows update reboot        -> use Task Scheduler to auto-launch at login
"""
from __future__ import annotations

import asyncio
import json
import logging
import logging.handlers
import os
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Optional

import httpx
from playwright.async_api import async_playwright, BrowserContext, Page

from config import settings
from db import DB
from parser import parse
from binance_client import BinanceWrapper
from trade_manager import TradeManager

PROJECT_ROOT = Path(__file__).resolve().parent
SESSION_DIR = PROJECT_ROOT / "browser_session"

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
HEADLESS = os.getenv("BROWSER_HEADLESS", "false").strip().lower() in ("1", "true", "yes")

# JS observer - same logic as before: ignore MID <= maxAtInstall, ignore old dates.
_OBSERVER_JS = r"""
(callbackName, minDateIso) => {
    if (window.__tgBotInstalled) {
        return { installed: true, already: true, maxMid: window.__tgBotMaxMidAtInstall || 0 };
    }
    const minDate = minDateIso ? new Date(minDateIso) : null;
    const seen = new Set();
    let maxMidAtInstall = 0;

    document.querySelectorAll('.bubble[data-mid]').forEach(el => {
        const mid = parseInt(el.dataset.mid);
        if (!isNaN(mid) && mid > maxMidAtInstall) maxMidAtInstall = mid;
        seen.add(el.dataset.mid);
    });

    const bubbleDateOk = (node) => {
        if (!minDate) return true;
        const candidates = [
            node.querySelector('.bubble-time .time-inner'),
            node.querySelector('.time-inner'),
            node.querySelector('.bubble-time'),
            node.querySelector('.time'),
            node.querySelector('time'),
        ].filter(Boolean);
        for (const el of candidates) {
            const title = el.title || el.getAttribute('title')
                       || el.getAttribute('datetime') || '';
            if (!title) continue;
            const parsed = Date.parse(title);
            if (!isNaN(parsed)) {
                return new Date(parsed) >= minDate;
            }
        }
        return true;
    };

    const extract = (node) => {
        if (!node || node.nodeType !== 1) return;
        const midStr = node.dataset && node.dataset.mid;
        if (!midStr) return;
        const mid = parseInt(midStr);
        if (isNaN(mid)) return;
        if (mid <= maxMidAtInstall) return;
        if (seen.has(midStr)) return;
        seen.add(midStr);

        if (!bubbleDateOk(node)) return;

        const textEl = (
            node.querySelector('.message') ||
            node.querySelector('.message-text') ||
            node.querySelector('.translatable-message')
        );
        const text = (textEl ? textEl.innerText : (node.innerText || '')).trim();
        if (text) {
            window[callbackName](JSON.stringify({ mid: midStr, text }));
        }
    };

    const observer = new MutationObserver((mutations) => {
        for (const m of mutations) {
            for (const node of m.addedNodes) {
                if (node.nodeType !== 1) continue;
                if (node.classList && node.classList.contains('bubble')) {
                    extract(node);
                }
                if (node.querySelectorAll) {
                    node.querySelectorAll('.bubble[data-mid]').forEach(extract);
                }
            }
        }
    });

    observer.observe(document.body, { childList: true, subtree: true });
    window.__tgBotInstalled = true;
    window.__tgBotMaxMidAtInstall = maxMidAtInstall;
    window.__tgBotObserver = observer;
    return { installed: true, already: false, maxMid: maxMidAtInstall };
}
"""


def setup_logging() -> None:
    fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(logging.Formatter(fmt))
    fh = logging.handlers.RotatingFileHandler(
        settings.log_path, maxBytes=2_000_000, backupCount=3, encoding="utf-8"
    )
    fh.setFormatter(logging.Formatter(fmt))
    root.handlers.clear()
    root.addHandler(sh)
    root.addHandler(fh)


class BotNotifier:
    """Send alerts via a regular Telegram bot token (BotFather)."""

    def __init__(self, token: str, chat_id: str):
        self._token = token
        self._chat_id = chat_id
        self._log = logging.getLogger("notifier")

    async def send(self, text: str) -> None:
        if not self._token or not self._chat_id:
            self._log.info("NOTIFY (bot token or chat id missing): %s", text[:300])
            return
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.post(
                    f"https://api.telegram.org/bot{self._token}/sendMessage",
                    json={"chat_id": self._chat_id, "text": text[:4000]},
                )
                if resp.status_code != 200:
                    self._log.warning(
                        "Bot API %s: %s  (did you send /start to your bot?)",
                        resp.status_code, resp.text[:200],
                    )
        except Exception as e:
            self._log.warning("Notification failed: %s", e)


def _min_date_iso() -> Optional[str]:
    raw = (settings.min_message_date or "").strip()
    if not raw:
        return None
    try:
        d = date.fromisoformat(raw)
        return datetime(d.year, d.month, d.day, tzinfo=timezone.utc).isoformat()
    except ValueError:
        logging.getLogger("config").warning(
            "MIN_MESSAGE_DATE=%r is not YYYY-MM-DD, ignoring", raw
        )
        return None


def cleanup_session_locks() -> None:
    """Remove Chromium singleton lock files left by an unclean shutdown."""
    if not SESSION_DIR.exists():
        return
    for name in ("SingletonLock", "SingletonCookie", "SingletonSocket"):
        p = SESSION_DIR / name
        try:
            if p.is_symlink() or p.exists():
                p.unlink()
        except OSError:
            pass


async def _scroll_to_latest(page: Page) -> None:
    try:
        await page.evaluate("""
            () => {
                const sels = ['.bubbles .scrollable', '.bubbles-inner',
                              '.chat .bubbles', '.bubbles'];
                for (const s of sels) {
                    const el = document.querySelector(s);
                    if (el) { el.scrollTop = el.scrollHeight; }
                }
            }
        """)
        await page.keyboard.press("End")
    except Exception as e:
        logging.getLogger("main").warning("scroll-to-latest failed: %s", e)
    await asyncio.sleep(2.0)


async def _install_observer(page: Page, min_date_iso: Optional[str]) -> dict:
    js = "({JS})({CB}, {MD})".format(
        JS=_OBSERVER_JS,
        CB=json.dumps("__tgBotCallback__"),
        MD=json.dumps(min_date_iso),
    )
    return await page.evaluate(js)


async def run_bot(log: logging.Logger, manager: TradeManager,
                  notifier: BotNotifier) -> str:
    """One browser session. Returns a reason string when it ends."""
    SESSION_DIR.mkdir(exist_ok=True)
    cleanup_session_locks()

    message_queue: asyncio.Queue[str] = asyncio.Queue()
    loop = asyncio.get_event_loop()

    channel = settings.tg_channel.strip()
    if not channel.startswith("-") and not channel.startswith("@"):
        channel = "@" + channel
    tg_url = f"https://web.telegram.org/k/#{channel}"

    min_date_iso = _min_date_iso()
    if min_date_iso:
        log.info("Date filter active: %s", min_date_iso)

    async with async_playwright() as pw:
        log.info("Launching browser (headless=%s)...", HEADLESS)
        ctx: BrowserContext = await pw.chromium.launch_persistent_context(
            str(SESSION_DIR),
            headless=HEADLESS,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
            ],
            viewport={"width": 1280, "height": 900},
        )

        close_reason: dict = {"reason": ""}
        close_event = asyncio.Event()

        def on_ctx_close() -> None:
            close_reason["reason"] = close_reason["reason"] or "browser context closed"
            log.warning("Browser context closed - run_bot will return for restart")
            loop.call_soon_threadsafe(close_event.set)

        ctx.on("close", lambda *_: on_ctx_close())

        page = ctx.pages[0] if ctx.pages else await ctx.new_page()

        def on_page_close() -> None:
            close_reason["reason"] = close_reason["reason"] or "page closed"
            log.warning("Page closed - run_bot will return for restart")
            loop.call_soon_threadsafe(close_event.set)

        page.on("close", lambda *_: on_page_close())

        def on_crash(_=None) -> None:
            close_reason["reason"] = close_reason["reason"] or "page crashed"
            log.error("Page crashed - run_bot will return for restart")
            loop.call_soon_threadsafe(close_event.set)

        page.on("crash", on_crash)

        def _js_callback(payload: str) -> None:
            try:
                data = json.loads(payload)
                text = (data.get("text") or "").strip()
                if text:
                    loop.call_soon_threadsafe(message_queue.put_nowait, text)
            except Exception as e:
                log.warning("JS callback error: %s", e)

        await page.expose_function("__tgBotCallback__", _js_callback)

        log.info("Navigating to %s", tg_url)
        await page.goto(tg_url, wait_until="domcontentloaded")

        try:
            await page.wait_for_selector(".bubbles, .chat-input", timeout=120_000)
        except Exception:
            log.error("Timed out waiting for Telegram Web - did you log in?")
            try: await ctx.close()
            except Exception: pass
            return "telegram web load timeout"

        await asyncio.sleep(2)
        log.info("Scrolling channel to latest message...")
        await _scroll_to_latest(page)

        if not await page.evaluate("() => !!document.querySelector('.bubble[data-mid]')"):
            log.error("No .bubble[data-mid] elements - selectors stale?")
            try: await ctx.close()
            except Exception: pass
            return "stale selectors"

        log.info("Installing message observer...")
        install_result = await _install_observer(page, min_date_iso)
        log.info("Observer installed: max MID = %s", install_result.get("maxMid"))

        async def reinstall_if_needed() -> None:
            try:
                installed = await page.evaluate(
                    "() => window.__tgBotInstalled === true"
                )
                if not installed:
                    log.warning("Observer missing - reinstalling")
                    await _scroll_to_latest(page)
                    res = await _install_observer(page, min_date_iso)
                    log.info("Observer reinstalled: maxMid=%s", res.get("maxMid"))
            except Exception as e:
                log.warning("reinstall_if_needed: %s", e)

        async def recover_from_navigation(url: str) -> None:
            try:
                if not url.startswith("https://web.telegram.org/k/"):
                    log.warning("Page navigated outside web.telegram.org: %s",
                                url[:80])
                    await page.goto(tg_url, wait_until="domcontentloaded")
                    await page.wait_for_selector(".bubbles, .chat-input",
                                                  timeout=60_000)
                    await asyncio.sleep(2)
                    await _scroll_to_latest(page)
                    await _install_observer(page, min_date_iso)
                    log.info("Recovered: back on channel")
                    return
                # On the K client, the URL fragment includes the chat ID.
                channel_id_or_name = channel.lstrip("@").lstrip("-")
                if channel_id_or_name and channel_id_or_name not in url:
                    log.warning("Different chat selected (%s) - navigating back",
                                url[:80])
                    await page.goto(tg_url, wait_until="domcontentloaded")
                    await asyncio.sleep(2)
                    await _scroll_to_latest(page)
                    await _install_observer(page, min_date_iso)
                    return
                await reinstall_if_needed()
            except Exception as e:
                log.warning("recover_from_navigation: %s", e)

        async def watchdog() -> None:
            while True:
                await asyncio.sleep(20)
                await reinstall_if_needed()

        def on_frame_navigated(frame) -> None:
            if frame == page.main_frame:
                url = frame.url
                log.info("Frame navigated: %s", url[:100])
                asyncio.create_task(recover_from_navigation(url))

        page.on("framenavigated", on_frame_navigated)
        watchdog_task = asyncio.create_task(watchdog())

        sizing_desc = (
            f"{settings.position_size_usdt} USDT per trade"
            if settings.uses_fixed_usdt_sizing
            else f"{settings.position_size_pct}% per trade"
        )
        await notifier.send(
            "Bot started (24/7 auto-restart enabled).\n"
            f"Paper: {settings.paper_trading}  Testnet: {settings.binance_testnet}\n"
            f"Channel: {channel}\n"
            f"Sizing: {sizing_desc}  Max: {settings.max_concurrent_positions}\n"
            f"Min message date: {settings.min_message_date or '(none)'}"
        )
        log.info("Bot running. Browser stays alive until something closes it; "
                 "then we auto-restart.")

        async def process_messages() -> None:
            while True:
                text = await message_queue.get()
                log.info("MSG text=%r", text[:200])
                parsed = parse(text)
                log.info("PARSED kind=%s ticker=%s actionable=%s notes=%s",
                         parsed.kind, parsed.ticker, parsed.actionable,
                         parsed.notes)
                try:
                    await manager.handle(parsed, tg_message_id=0)
                except Exception as e:
                    log.exception("Handler crashed: %s", e)
                    try:
                        await notifier.send(
                            f"Handler crashed: {e}\nText: {text[:300]}"
                        )
                    except Exception:
                        pass

        processor_task = asyncio.create_task(process_messages())

        try:
            await close_event.wait()
        finally:
            for t in (watchdog_task, processor_task):
                t.cancel()
                try:
                    await t
                except (asyncio.CancelledError, Exception):
                    pass
            try:
                await ctx.close()
            except Exception:
                pass

        return close_reason["reason"] or "unknown"


async def main_loop() -> None:
    setup_logging()
    log = logging.getLogger("main")

    problems = settings.validate(browser_mode=True)
    if problems:
        log.error("Config problems:")
        for p in problems:
            log.error("  - %s", p)
        sys.exit(1)

    db = DB(settings.db_path)
    binance = BinanceWrapper(
        api_key=settings.binance_key,
        api_secret=settings.binance_secret,
        testnet=settings.binance_testnet,
        paper_trading=settings.paper_trading,
        allowed_quotes=settings.allowed_quote_assets,
    )
    notifier = BotNotifier(BOT_TOKEN, settings.notify_user_id)
    manager = TradeManager(settings, binance, db, notifier)

    backoff = 5
    while True:
        try:
            reason = await run_bot(log, manager, notifier)
            log.warning("Bot run ended (%s). Restarting in %ds.", reason, backoff)
            try:
                await notifier.send(
                    f"Bot run ended: {reason}. Auto-restarting in {backoff}s."
                )
            except Exception:
                pass
        except KeyboardInterrupt:
            log.info("KeyboardInterrupt - exiting cleanly")
            return
        except Exception as e:
            log.exception("Unexpected crash in run_bot: %s", e)
            try:
                await notifier.send(
                    f"Bot crashed ({type(e).__name__}: {e}). "
                    f"Auto-restarting in {backoff}s."
                )
            except Exception:
                pass
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, 60)


if __name__ == "__main__":
    try:
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        print("\nStopped.")
