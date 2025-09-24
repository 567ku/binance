@echo off
echo === Starting Binance Grid Trading System ===
echo.

REM 检查配置文件
if not exist "config.json" (
    echo Error: config.json not found
    pause
    exit /b 1
)

REM 检查API密钥文件
if not exist "key.txt" (
    echo Error: key.txt not found
    pause
    exit /b 1
)

echo Configuration files found.
echo Starting trading system...
echo.
echo Press Ctrl+C to stop the system
echo.

go run main.go

pause