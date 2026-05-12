"""Send status / alert DMs back to the user via the same Telethon client.

We deliberately re-use the bot's own Telegram user session to message the user's
personal account. This avoids needing a separate Telegram bot token.
"""
from __future__ import annotations

import logging
from typing import Optional

log = logging.getLogger(__name__)


class Notifier:
    def __init__(self, telethon_client, recipient: str):
        """`recipient` should be a username (without @), phone, or numeric ID."""
        self.client = telethon_client
        self.recipient = recipient.strip() if recipient else ""

    async def send(self, text: str) -> None:
        if not self.recipient:
            log.debug("Notifier disabled (no recipient configured)")
            return
        try:
            await self.client.send_message(self._target(), text[:4000])
        except Exception as e:  # broad - we never want notifier errors to crash the bot
            log.warning("Failed to send notification: %s", e)

    def _target(self):
        # Telethon accepts int or str; coerce numeric strings to int.
        r = self.recipient
        if r.lstrip("-").isdigit():
            return int(r)
        return r
