console.log('Starting test with error handling...');

import express from 'express';
console.log('Express imported');

import cors from 'cors';
console.log('CORS imported');

const app = express();
console.log('Express app created');

app.use(cors());
console.log('CORS middleware added');

app.use(express.json());
console.log('JSON middleware added');

app.get('/health', (req, res) => {
  console.log('Health check called');
  res.json({ status: 'healthy' });
});

console.log('Health endpoint added');

// Try to start server with error handling
const port = 3005;
console.log(`Attempting to start server on port ${port}...`);

try {
  const server = app.listen(port, () => {
    console.log(`Server started successfully on port ${port}`);
  });
  
  server.on('error', (error) => {
    console.error('Server error:', error);
  });
  
  // Close server after 5 seconds for testing
  setTimeout(() => {
    console.log('Closing server...');
    server.close(() => {
      console.log('Server closed successfully');
    });
  }, 5000);
  
} catch (error) {
  console.error('Error starting server:', error);
}

console.log('Test completed');