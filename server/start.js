#!/usr/bin/env node
'use strict';

import { spawn } from 'child_process';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

console.log('🚀 Starting Iceberg Analytics Server...');

// Check if we're in the right directory
const packageJsonPath = join(__dirname, 'package.json');
try {
  const packageJson = await import(packageJsonPath, { assert: { type: 'json' } });
  console.log(`📦 Package: ${packageJson.default.name} v${packageJson.default.version}`);
} catch (error) {
  console.error('❌ Error reading package.json:', error.message);
  process.exit(1);
}

// Start the server
const serverProcess = spawn('node', ['index.js'], {
  stdio: 'inherit',
  cwd: __dirname,
  env: {
    ...process.env,
    NODE_ENV: process.env.NODE_ENV || 'development'
  }
});

// Handle server process events
serverProcess.on('error', (error) => {
  console.error('❌ Failed to start server:', error.message);
  process.exit(1);
});

serverProcess.on('exit', (code, signal) => {
  if (code === 0) {
    console.log('✅ Server stopped gracefully');
  } else if (signal) {
    console.log(`⚠️  Server stopped due to signal: ${signal}`);
  } else {
    console.error(`❌ Server exited with code: ${code}`);
  }
  process.exit(code || 0);
});

// Handle process termination
process.on('SIGINT', () => {
  console.log('\n🛑 Received SIGINT, shutting down...');
  serverProcess.kill('SIGINT');
});

process.on('SIGTERM', () => {
  console.log('\n🛑 Received SIGTERM, shutting down...');
  serverProcess.kill('SIGTERM');
});

// Handle uncaught exceptions
process.on('uncaughtException', (error) => {
  console.error('❌ Uncaught Exception:', error);
  serverProcess.kill();
  process.exit(1);
});

process.on('unhandledRejection', (reason, promise) => {
  console.error('❌ Unhandled Rejection at:', promise, 'reason:', reason);
  serverProcess.kill();
  process.exit(1);
});