@echo off
REM Runs the full backtest pipeline:
REM   1. Backfill channel messages (prompts you for # of days)
REM   2. Simulate each signal against Binance klines
REM Requires: main_browser.py must NOT be running (locks browser_session)
cd /d "%~dp0"

if not exist .venv (
    echo No .venv found - run run_browser.bat first to install dependencies.
    pause
    exit /b 1
)

REM venv was created with uv, so install through uv. Falls back to pip if uv is gone.
echo Ensuring backtest dependencies are installed...
where uv >nul 2>&1
if %errorlevel%==0 (
    uv pip install --python .venv --quiet pandas numpy plotly streamlit pyarrow python-dateutil
) else (
    .venv\Scripts\python.exe -m pip install --quiet pandas numpy plotly streamlit pyarrow python-dateutil
)
if errorlevel 1 (
    echo Failed to install backtest dependencies. Run manually:
    echo   uv pip install --python .venv pandas numpy plotly streamlit pyarrow python-dateutil
    pause
    exit /b 1
)

echo.
echo === Step 1/2: backfilling channel messages ===
.venv\Scripts\python.exe backfill_signals.py
if errorlevel 1 (
    echo Backfill failed - see output above.
    pause
    exit /b 1
)

echo.
echo === Step 2/2: simulating trades ===
.venv\Scripts\python.exe backtest.py
if errorlevel 1 (
    echo Backtest failed - see output above.
    pause
    exit /b 1
)

echo.
echo Done. Launch run_dashboard.bat to view results.
pause
