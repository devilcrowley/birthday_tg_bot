#!/bin/bash

# Проверяем, запущен ли процесс
if ! pgrep -f "birthday-bot" > /dev/null; then
    echo "Bot is not running. Starting..."
    cd "$(dirname "$0")"
    nohup ./birthday-bot > bot.log 2>&1 &
    echo "Bot started with PID: $!"
else
    echo "Bot is already running"
fi