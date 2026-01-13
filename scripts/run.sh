#!/bin/bash
ls -la
echo "Building the application..."
cd ${GITHUB_WORKSPACE} 
ls -la
go build -o server ./cmd/api/main.go

echo "Starting the application in background."
nohup ./server > app.log 2>&1 &
echo "Application started successfully in background."
echo "PID: $!"
echo "Logs are being written to app.log"