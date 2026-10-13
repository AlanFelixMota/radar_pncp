@echo off
setlocal
chcp 65001 >nul
cd /d "%~dp0"
where py >nul 2>nul
if %ERRORLEVEL% equ 0 (
  py -3.12 -m venv "%~dp0.venv"
) else (
  python -m venv "%~dp0.venv"
)
if errorlevel 1 goto erro
"%~dp0.venv\Scripts\python.exe" -c "import sys; assert sys.version_info >= (3,12), 'Use Python 3.12 ou superior'"
if errorlevel 1 goto erro
"%~dp0.venv\Scripts\python.exe" -m pip install -r "%~dp0requirements.txt"
if errorlevel 1 goto erro
echo.
echo Dependencias instaladas. Para uma base nova, execute CRIAR_BASE_INICIAL.bat.
echo Para usar dados existentes, siga docs\MIGRACAO.md antes de iniciar.
pause
exit /b 0
:erro
echo Falha na instalacao. Verifique o Python 3.12 e a conexao com a internet.
pause
exit /b 1
