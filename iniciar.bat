@echo off
setlocal
chcp 65001 >nul
cd /d "%~dp0"
if exist "%~dp0.venv\Scripts\python.exe" (
  "%~dp0.venv\Scripts\python.exe" -u "%~dp0radar_pncp_iniciar.py"
) else if exist "%~dp0python\python.exe" (
  "%~dp0python\python.exe" -u "%~dp0radar_pncp_iniciar.py"
) else (
  python -u "%~dp0radar_pncp_iniciar.py"
)
set "resultado=%ERRORLEVEL%"
pause
exit /b %resultado%
