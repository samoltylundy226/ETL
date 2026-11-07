@echo off
title Week 4 - Banking Transaction Generator
cd /d "C:\Users\USER\Downloads\Telegram Desktop\Week 4 - Build ETL Pipeline\Week 4 - Build ETL Pipeline\2. Hands-on Lab\B). Banking Transaction Simulation\banking_transaction_simulation"

echo Installing Python requirements...
python -m pip install -r requirements-generator.txt

set MARIA_DB_USER=maria
set MARIA_DB_PASSWORD=maria#123!

echo.
echo Starting transaction generator (Ctrl+C to stop)...
python synthetic_banking_transaction_generator.py
pause
