@echo off
setlocal
cd /d "%~dp0"
if not exist "%~dp0douyu-danmaku.exe" (
  echo douyu-danmaku.exe is missing. Run build.cmd first.
  exit /b 1
)
if not defined DANMAKU_ADDR set "DANMAKU_ADDR=0.0.0.0:8787"
echo Open http://127.0.0.1:8787/ in your browser.
echo Phone: use the LAN URL printed below on the same Wi-Fi.
echo Press Ctrl+C here to exit.
if "%~1"=="" (
  "%~dp0douyu-danmaku.exe" -addr "%DANMAKU_ADDR%"
) else (
  "%~dp0douyu-danmaku.exe" -addr "%DANMAKU_ADDR%" -room "%~1"
)
