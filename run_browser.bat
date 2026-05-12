@echo off
REM Launches the bot in BROWSER mode (safer — no selfbot / Telethon API).
REM First run: a Chromium window opens so you can log in to Telegram Web.
REM After logging in, the session is saved to browser_session\ automatically.
REM Subsequent runs re-use the saved session (no login needed).
cd /d "%~dp0"

if not exist .venv (
    echo Creating virtual environment with uv...
    uv venv .venv
)

if not exist .venv\.installed (
    echo Installing dependencies...
    uv pip install --python .venv -r requirements.txt
    .venv\Scripts\python.exe -m playwright install chromium
    echo done > .venv\.installed
)

if not exist .env (
    echo .env file not found. Copy .env.example to .env and fill it in first.
    pause
    exit /b 1
)

.venv\Scripts\python.exe main_browser.py
pause
