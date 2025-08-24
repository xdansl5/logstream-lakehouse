console.log('Starting test without listen...');

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

// Don't call app.listen
console.log('Test completed without starting server');