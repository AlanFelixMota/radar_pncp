@echo off
setlocal
chcp 65001 >nul
cd /d "%~dp0"
if exist "%~dp0.venv\Scripts\python.exe" (
  "%~dp0.venv\Scripts\python.exe" -u "%~dp0criar_base_inicial.py"
) else (
  python -u "%~dp0criar_base_inicial.py"
)
set "resultado=%ERRORLEVEL%"
pause
exit /b %resultado%
