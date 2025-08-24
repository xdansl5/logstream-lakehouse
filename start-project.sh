#!/bin/bash

# Iceberg Analytics Project Startup Script
# This script starts both the backend server and frontend

set -e

echo "🚀 Starting Iceberg Analytics Project..."

# Check if Node.js is installed
if ! command -v node &> /dev/null; then
    echo "❌ Node.js is not installed. Please install Node.js first."
    exit 1
fi

# Check if npm is installed
if ! command -v npm &> /dev/null; then
    echo "❌ npm is not installed. Please install npm first."
    exit 1
fi

# Function to cleanup background processes
cleanup() {
    echo "🛑 Shutting down services..."
    kill $BACKEND_PID $FRONTEND_PID 2>/dev/null || true
    exit 0
}

# Set trap to cleanup on script exit
trap cleanup SIGINT SIGTERM EXIT

# Start backend server
echo "🔧 Starting backend server..."
cd server
npm install
echo "📊 Backend dependencies installed"
npm start &
BACKEND_PID=$!
cd ..

# Wait for backend to start
echo "⏳ Waiting for backend to start..."
sleep 5

# Check if backend is running
if ! curl -s http://localhost:3001/health > /dev/null; then
    echo "❌ Backend failed to start. Check server logs."
    exit 1
fi

echo "✅ Backend server is running on http://localhost:3001"

# Start frontend
echo "🎨 Starting frontend..."
npm install
echo "🎯 Frontend dependencies installed"
npm run dev &
FRONTEND_PID=$!

# Wait for frontend to start
echo "⏳ Waiting for frontend to start..."
sleep 10

echo "✅ Frontend is running on http://localhost:5173"
echo ""
echo "🌐 Open your browser and navigate to:"
echo "   Frontend: http://localhost:5173"
echo "   Backend API: http://localhost:3001"
echo "   Health Check: http://localhost:3001/health"
echo ""
echo "📊 The system will automatically:"
echo "   - Generate realistic log data in the database"
echo "   - Provide interactive SQL query interface"
echo "   - Show real-time analytics and metrics"
echo ""
echo "🛑 Press Ctrl+C to stop all services"

# Keep script running
wait