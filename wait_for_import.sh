#!/bin/bash

while true; do
    # Extract the 1-minute load average using awk
    loadavg=$(uptime | awk -F'[a-z]:' '{ print $2}' | cut -d, -f1 | tr -d ' ')
    
    echo $loadavg
    awk -v loadavg="$loadavg" 'BEGIN { if (loadavg < 0.3) exit 0; else exit 1; }'
    
    if [ $? -eq 0 ]; then
        break
    fi

    sleep 60  # Check every 60 seconds
done

python3 add_index.py
