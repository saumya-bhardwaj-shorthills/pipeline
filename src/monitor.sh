#!/bin/bash

python3 main.py &
MAIN_PID=$!

echo "Started main.py with PID: $MAIN_PID"
echo "Monitoring memory usage..."

# Continuous monitoring
while true; do
    MEMORY_USAGE=$(free | awk '/Mem:/ {printf "%.0f", $3/$2 * 100}')

    echo "Current Memory Usage: ${MEMORY_USAGE}%"

    if [ "$MEMORY_USAGE" -ge 97 ]; then
        echo "Memory usage is above 97%! Terminating main.py..."
        kill -9 $MAIN_PID
        exit 1
    fi

    sleep 5
done
