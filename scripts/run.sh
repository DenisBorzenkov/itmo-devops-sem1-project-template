#!/bin/bash
set -e

echo "Start the server"
go run cmd/app/main.go &

SERVER_PID=$!
echo "Server started with PID: $SERVER_PID"

echo "Waiting for the server to start"
MAX_RETRIES=10
RETRY_DELAY=5
RETRIES=0

while ! nc -z localhost 8080; do
    RETRIES=$((RETRIES + 1))
    
    if [ "$RETRIES" -ge "$MAX_RETRIES" ]; then
        echo "Server failed to start within the expected time."
        exit 1
    fi

    echo "Server not ready yet. Retrying in $RETRY_DELAY seconds..."
    sleep "$RETRY_DELAY"
done

echo "Server is up and running"
exit 0
