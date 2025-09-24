@echo off
echo === Binance Grid Trading System Build Script ===
echo.

REM 检查Go是否安装
where go >nul 2>nul
if %ERRORLEVEL% NEQ 0 (
    echo Error: Go is not installed or not in PATH
    echo Please install Go from https://golang.org/dl/
    pause
    exit /b 1
)

echo Go version:
go version
echo.

echo Downloading dependencies...
go mod tidy
if %ERRORLEVEL% NEQ 0 (
    echo Error: Failed to download dependencies
    pause
    exit /b 1
)
echo Dependencies downloaded successfully!
echo.

echo Building the project...
go build -o binance-grid-trader.exe main.go
if %ERRORLEVEL% NEQ 0 (
    echo Error: Build failed
    pause
    exit /b 1
)
echo Build completed successfully!
echo.

echo === Build Summary ===
echo ✓ Dependencies: OK
echo ✓ Build: OK
echo.
echo The executable 'binance-grid-trader.exe' has been created.
echo.
echo To run the trading system:
echo   1. Review your config.json settings
echo   2. Ensure your API keys are in key.txt
echo   3. Run: binance-grid-trader.exe
echo   4. Or run: go run main.go
echo.
pause