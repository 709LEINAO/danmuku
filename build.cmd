@echo off
setlocal
cd /d "%~dp0"
if not defined DANMAKU_PASSWORD (
  echo DANMAKU_PASSWORD is not set. The password is injected at build time and never kept in source.
  exit /b 1
)
go build -trimpath -ldflags="-s -w -X 'main.defaultPassword=%DANMAKU_PASSWORD%'" -o douyu-danmaku.exe .
if errorlevel 1 exit /b 1
echo Built: %~dp0douyu-danmaku.exe
