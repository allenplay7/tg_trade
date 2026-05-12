@echo off
REM Launches the Telegram -> Binance signal bot (selfbot / Telethon mode).
cd /d "%~dp0"

if not exist .venv (
    echo Creating virtual environment with uv...
    uv venv .venv
)

if not exist .venv\.installed (
    echo Installing dependencies...
    uv pip install --python .venv -r requirements.txt
    echo done > .venv\.installed
)

if not exist .env (
    echo .env file not found. Copy .env.example to .env and fill it in first.
    pause
    exit /b 1
)

.venv\Scripts\python.exe main.py
pause
