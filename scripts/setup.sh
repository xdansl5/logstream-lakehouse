#!/bin/bash

# Iceberg Analytics Platform Setup Script
echo "🚀 Setting up Iceberg Analytics Platform..."

# Check if Node.js is installed
if ! command -v node &> /dev/null; then
    echo "❌ Node.js is not installed. Please install Node.js 18+ first."
    echo "   Visit: https://nodejs.org/"
    exit 1
fi

# Check Node.js version
NODE_VERSION=$(node -v | cut -d'v' -f2 | cut -d'.' -f1)
if [ "$NODE_VERSION" -lt 18 ]; then
    echo "❌ Node.js version 18+ is required. Current version: $(node -v)"
    exit 1
fi

echo "✅ Node.js $(node -v) detected"

# Install frontend dependencies
echo "📦 Installing frontend dependencies..."
npm install

# Install server dependencies
echo "📦 Installing server dependencies..."
cd server
npm install
cd ..

# Create data directories
echo "📁 Creating data directories..."
mkdir -p data/iceberg
mkdir -p data/arrow
mkdir -p logs

# Create .env file if it doesn't exist
if [ ! -f .env ]; then
    echo "⚙️  Creating .env file..."
    cat > .env << EOF
# Server Configuration
VITE_SERVER_URL=http://localhost:3001
VITE_SSE_URL=http://localhost:3001/events

# Iceberg Configuration
ICEBERG_TABLE_PATH=./data/iceberg
ARROW_DATA_PATH=./data/arrow

# Kafka Configuration (optional)
KAFKA_BROKERS=localhost:9092
KAFKA_TOPIC=web-logs
KAFKA_GROUP_ID=ui-bridge-group
EOF
    echo "✅ Created .env file with default configuration"
else
    echo "✅ .env file already exists"
fi

echo ""
echo "🎉 Setup complete!"
echo ""
echo "To start the platform:"
echo "  1. Terminal 1: npm run server"
echo "  2. Terminal 2: npm run dev"
echo "  3. Open: http://localhost:5173"
echo ""
echo "📚 For more information, see README.md"
echo ""
echo "🧪 To test the server: npm run test:iceberg"