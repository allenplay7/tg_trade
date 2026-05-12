"""Entry point: connect to Telegram, listen to the channel, dispatch to TradeManager."""
from __future__ import annotations

import asyncio
import logging
import logging.handlers
import sys
from typing import Optional

from telethon import TelegramClient, events

from config import settings
from db import DB
from parser import parse
from binance_client import BinanceWrapper
from notifier import Notifier
from trade_manager import TradeManager


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
    # Tame chatty libraries
    logging.getLogger("telethon").setLevel(logging.WARNING)


def maybe_ocr_text(message_text: str, image_path: Optional[str]) -> str:
    """Run OCR on an attached image only if the text is empty / non-actionable."""
    if not settings.enable_image_ocr or not image_path:
        return ""
    try:
        from PIL import Image
        import pytesseract
        if settings.tesseract_cmd:
            pytesseract.pytesseract.tesseract_cmd = settings.tesseract_cmd
        return pytesseract.image_to_string(Image.open(image_path)) or ""
    except Exception as e:  # noqa: BLE001
        logging.getLogger("ocr").warning("OCR failed: %s", e)
        return ""


async def main() -> None:
    setup_logging()
    log = logging.getLogger("main")
    
        problems = settings.validate()
    if problems:
        log.error("Invalid configuration:")
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

    client = TelegramClient(
        str(settings.session_path).replace(".session", ""),
        settings.tg_api_id,
        settings.tg_api_hash,
    )
    await client.start(phone=settings.tg_phone)
    log.info("Telegram client connected.")

    notifier = Notifier(client, settings.notify_user_id)
    manager = TradeManager(settings, binance, db, notifier)

    # Resolve the channel entity once so Telethon caches it
    try:
        channel = await client.get_entity(_chan_arg(settings.tg_channel))
        log.info("Listening to channel: %s (id=%s)", getattr(channel, "title", "?"), channel.id)
    except Exception as e:
        log.error("Could not resolve channel '%s': %s", settings.tg_channel, e)
        sys.exit(1)

    await notifier.send(
        f"Bot started.\n"
        f"Paper trading: {settings.paper_trading}\n"
        f"Channel: {getattr(channel, 'title', '?')}\n"
        f"Position size: {settings.position_size_pct}% "
        f"({settings.risky_position_size_pct}% on risky calls)\n"
        f"Entry mode: {settings.entry_order_type}\n"
        f"Mgmt msgs: {'ON' if settings.parse_mgmt_messages else 'OFF'}"
    )

    @client.on(events.NewMessage(chats=channel))
    async def on_message(event):
        text = (event.message.message or "").strip()
        image_path = None

        # If there's an image and no useful text, try OCR fallback.
        if event.message.media and (not text or "$" not in text):
            try:
                image_path = await event.message.download_media(file="bytes")
                if isinstance(image_path, (bytes, bytearray)):
                    # Telethon returns bytes when file='bytes'; save to a temp file
                    tmp = settings.db_path.parent / f".ocr_{event.message.id}.bin"
                    tmp.write_bytes(image_path)
                    image_path = str(tmp)
                ocr_text = maybe_ocr_text(text, image_path)
                if ocr_text:
                    text = (text + "\n" + ocr_text).strip()
            except Exception as e:  # noqa: BLE001
                log.warning("Image download/OCR failed: %s", e)

        log.info("MSG id=%s text=%r", event.message.id, text[:200])
        parsed = parse(text)
        log.info(
            "PARSED kind=%s ticker=%s actionable=%s notes=%s",
            parsed.kind, parsed.ticker, parsed.actionable, parsed.notes,
        )
        try:
            await manager.handle(parsed, tg_message_id=event.message.id)
        except Exception as e:  # noqa: BLE001
            log.exception("handler crashed: %s", e)
            try:
                await notifier.send(f"Handler crashed on signal: {e}\nText: {text[:300]}")
            except Exception:
                pass

    log.info("Bot running. Ctrl-C to stop.")
    await client.run_until_disconnected()


def _chan_arg(raw: str):
    raw = raw.strip()
    if raw.lstrip("-").isdigit():
        return int(raw)
    return raw  # username


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nStopped.")
