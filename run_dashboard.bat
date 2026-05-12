@echo off
REM Launches the Streamlit dashboard (opens in default browser at http://localhost:8501)
cd /d "%~dp0"

if not exist .venv (
    echo No .venv found - run run_browser.bat first.
    pause
    exit /b 1
)

echo Ensuring dashboard dependencies are installed...
where uv >nul 2>&1
if %errorlevel%==0 (
    uv pip install --python .venv --quiet streamlit plotly pandas pyarrow python-dateutil
) else (
    .venv\Scripts\python.exe -m pip install --quiet streamlit plotly pandas pyarrow python-dateutil
)
if errorlevel 1 (
    echo Failed to install dashboard dependencies. Run manually:
    echo   uv pip install --python .venv streamlit plotly pandas pyarrow
    pause
    exit /b 1
)

.venv\Scripts\python.exe -m streamlit run dashboard.py
pause
