#!/bin/bash

# Iceberg Analytics Project Startup Script
# This script starts both the backend server and frontend development server

set -e

echo "🚀 Starting Iceberg Analytics Project..."
echo "========================================"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if we're in the right directory
if [ ! -f "package.json" ]; then
    print_error "package.json not found. Please run this script from the project root directory."
    exit 1
fi

# Check if Node.js is installed
if ! command -v node &> /dev/null; then
    print_error "Node.js is not installed. Please install Node.js 18+ first."
    exit 1
fi

# Check Node.js version
NODE_VERSION=$(node -v | cut -d'v' -f2 | cut -d'.' -f1)
if [ "$NODE_VERSION" -lt 18 ]; then
    print_error "Node.js version 18+ is required. Current version: $(node -v)"
    exit 1
fi

print_success "Node.js version: $(node -v)"

# Install dependencies if node_modules doesn't exist
if [ ! -d "node_modules" ]; then
    print_status "Installing project dependencies..."
    npm install
    print_success "Dependencies installed successfully"
else
    print_status "Dependencies already installed"
fi

# Install server dependencies if needed
if [ ! -d "server/node_modules" ]; then
    print_status "Installing server dependencies..."
    cd server
    npm install
    cd ..
    print_success "Server dependencies installed successfully"
else
    print_status "Server dependencies already installed"
fi

# Create necessary directories
print_status "Creating necessary directories..."
mkdir -p server/logs
mkdir -p server/data/iceberg
print_success "Directories created"

# Function to cleanup background processes
cleanup() {
    print_status "Shutting down services..."
    if [ ! -z "$BACKEND_PID" ]; then
        kill $BACKEND_PID 2>/dev/null || true
        print_status "Backend server stopped"
    fi
    if [ ! -z "$FRONTEND_PID" ]; then
        kill $FRONTEND_PID 2>/dev/null || true
        print_status "Frontend server stopped"
    fi
    exit 0
}

# Set up signal handlers
trap cleanup SIGINT SIGTERM

# Start backend server
print_status "Starting backend server..."
cd server
npm start &
BACKEND_PID=$!
cd ..

# Wait a moment for backend to start
sleep 3

# Check if backend is running
if kill -0 $BACKEND_PID 2>/dev/null; then
    print_success "Backend server started (PID: $BACKEND_PID)"
    print_status "Backend URL: http://localhost:3001"
    print_status "Health check: http://localhost:3001/health"
else
    print_error "Failed to start backend server"
    exit 1
fi

# Start frontend development server
print_status "Starting frontend development server..."
npm run dev &
FRONTEND_PID=$!

# Wait a moment for frontend to start
sleep 3

# Check if frontend is running
if kill -0 $FRONTEND_PID 2>/dev/null; then
    print_success "Frontend server started (PID: $FRONTEND_PID)"
    print_status "Frontend URL: http://localhost:8080"
else
    print_error "Failed to start frontend server"
    kill $BACKEND_PID 2>/dev/null || true
    exit 1
fi

print_success "========================================"
print_success "🎉 Project started successfully!"
print_success "========================================"
print_status "Backend: http://localhost:3001"
print_status "Frontend: http://localhost:8080"
print_status "Health Check: http://localhost:3001/health"
print_status ""
print_status "Press Ctrl+C to stop all services"
print_status ""

# Wait for user to stop
wait