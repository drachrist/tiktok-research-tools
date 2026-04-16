@echo off
echo Starting TikTok Research App...
echo.

REM Try to install dependencies silently in case they're missing
pip install streamlit requests >nul 2>&1

REM Change to the folder this .bat file lives in
cd /d "%~dp0"

REM Launch the app
py -m streamlit run tiktok_research_app.py

pause
