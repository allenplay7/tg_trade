# Telegram Signal Bot → Binance Spot

Listens to a Telegram signal channel via a Telegram **user account** (Telethon),
parses each message into a structured trade, and executes spot orders on Binance.
Paper-trading mode is on by default — turn it off only after you've watched it
for a few days and trust the parser.

> **Risk warning.** This bot trades real money. Signal channels usually do worse
> than they advertise. Start in paper-trading mode (the default), use a small
> position size when you go live, and never trade money you can't afford to lose.
> Read every line of `parser.py` and `trade_manager.py` before flipping
> `PAPER_TRADING=false`.

---

## What it does

| Message kind                                          | Bot action                          |
|-------------------------------------------------------|-------------------------------------|
| `$BTC Entry - CMP - 65000 Target - 67000 SL - 64000`  | Market buy + OCO (TP1 + SL)         |
| `$BTC SHORT ...`                                      | Skipped (spot only)                 |
| `$BTC` alone, "Volume increasing"                     | Logged, ignored                     |
| `Close it` (no ticker)                                | Notified, no action                 |
| `close $ENJ`                                          | Cancels OCO + market-sells $ENJ     |
| `$SXT Put SL -0.01388`                                | Replaces OCO with new SL            |
| `first target crushed`                                | Notification only (your OCO handles)|
| `$A Next $B ENTRY - ...`                              | Ambiguous → notified, no action     |

---

## Setup (Windows)

### 1. Install Python 3.10+
Download from https://www.python.org/downloads/ and check "Add to PATH" during install.

### 2. Get Telegram API credentials
1. Go to https://my.telegram.org/apps and log in.
2. Create an app — any name/description is fine.
3. Note `api_id` and `api_hash`.

**Use a secondary Telegram account, not your main one.** Telegram can ban user
accounts for automation. Spin up a second account with a different phone number,
subscribe it to the signal channel, and use *that* account's credentials.

### 3. Get Binance API keys
1. https://www.binance.com/en/my/settings/api-management → Create API.
2. Permissions: **Enable Reading** + **Enable Spot & Margin Trading**.
   **Disable everything else** (no Futures, no Withdrawal, no Universal Transfer).
3. IP-whitelist your home IP for safety.

While you're testing, use Binance Spot Testnet keys instead:
https://testnet.binance.vision (free fake-money sandbox). Set
`BINANCE_TESTNET=true` in `.env`.

### 4. Configure
Copy `.env.example` to `.env` and fill in:

```env
TELEGRAM_API_ID=12345
TELEGRAM_API_HASH=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
TELEGRAM_PHONE=+11234567890
TELEGRAM_CHANNEL=high_table_channel    # channel @username, no @
NOTIFY_USER_ID=123456789                # your own Telegram user ID

BINANCE_API_KEY=...
BINANCE_API_SECRET=...
BINANCE_TESTNET=true

PAPER_TRADING=true
POSITION_SIZE_PCT=3.0
RISKY_POSITION_SIZE_PCT=1.0
ENTRY_ORDER_TYPE=MARKET
PARSE_MGMT_MESSAGES=true
```

Get your own Telegram user ID by messaging `@userinfobot` in Telegram.

### 5. Run
Double-click `run.bat`. The first launch will:
1. Create a `.venv` and install dependencies.
2. Ask for your Telegram phone code (one-time, then cached in `telethon.session`).
3. Start listening to the channel.

You should receive a "Bot started" DM from your own (second) account telling you
the bot is alive.

---

## Operating modes

### Paper-trading (default)
`PAPER_TRADING=true` — the bot runs the full pipeline (Telegram listen, parse,
size, place order) but never calls Binance. It assumes orders fill at the
current market price. Use this for at least a week.

### Live
`PAPER_TRADING=false` + valid Binance keys. **Test against testnet first.**

---

## Risk knobs (in `.env`)

| Knob                          | What it does                                                       |
|-------------------------------|--------------------------------------------------------------------|
| `POSITION_SIZE_PCT`           | % of free USDT used per trade. Start small.                         |
| `RISKY_POSITION_SIZE_PCT`     | Smaller % used when the message contains "risky", "low cap", "hype".|
| `MAX_CONCURRENT_POSITIONS`    | Cap on simultaneous open positions. **0 = unlimited.** Recommend ~5.|
| `ONE_TRADE_PER_TICKER`        | Skip duplicate signals for the same ticker.                         |
| `DAILY_LOSS_CIRCUIT_PCT`      | Stop trading after losing X% in a UTC day. **0 = disabled.**        |
| `ALLOWED_QUOTE_ASSETS`        | Only `USDT` by default. Add `USDC,BTC` etc. if you want.            |

Your sample channel dropped 7 signals in 6 minutes once. **Enable the
concurrent-position cap and the daily loss circuit-breaker** before going live —
they're the two settings that protect you from a bad-day cascade.

---

## What gets logged

- **`bot.log`** — rotating text log of everything the bot does/sees.
- **`bot.sqlite3`** — durable record of:
  - `signals` — every parsed message
  - `positions` — every position opened, with TP/SL/OCO order IDs and PnL
  - `events` — error/warn events
- **`telethon.session`** — your Telegram login token. **Treat this file like a
  password.** Anyone with it can log in as your bot account.

Inspect the SQLite database with any sqlite viewer (e.g. `sqlitebrowser`):
```sql
SELECT ticker, opened_ts, avg_entry, tp_price, sl_price, pnl_quote
FROM positions ORDER BY opened_ts DESC LIMIT 20;
```

---

## What can go wrong

- **Telegram bans the bot account.** Use a throwaway secondary account.
- **Channel changes its format.** Parser sends you AMBIGUOUS notifications —
  re-read `parser.py` and add a test case to `tests/test_parser.py` matching the
  new format.
- **Ticker not listed on Binance.** Many alt signals are Bybit/Bitget-only.
  Bot will skip and notify you.
- **OCO order rejected.** Usually because TP price is too close to current
  market or qty is below `MIN_NOTIONAL`. Check `bot.log` and Binance UI.
- **Computer sleeps.** Missed signals. Disable sleep, or move to a VPS.

---

## Project layout

```
tg_signal_bot/
├── main.py             # Entry point: Telethon listener
├── config.py           # .env loading + validation
├── parser.py           # Message -> ParsedSignal (the regex brain)
├── binance_client.py   # python-binance wrapper, paper-trading shim
├── trade_manager.py    # Orchestrates entry + OCO + management msgs
├── notifier.py         # DMs you status via the Telethon client
├── db.py               # SQLite log of signals, positions, events
├── tests/
│   └── test_parser.py  # Real sample signals from the channel
├── .env.example        # Copy to .env and edit
├── requirements.txt
└── run.bat             # Windows launcher
```

---

## Running the tests

```cmd
.venv\Scripts\activate
python tests\test_parser.py
```

If the channel changes its format, add a new test there first, then update
`parser.py` until it passes.
