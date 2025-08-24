console.log('Starting minimal test...');

import express from 'express';
console.log('Express imported');

import cors from 'cors';
console.log('CORS imported');

import IcebergService from './icebergService.js';
console.log('IcebergService imported');

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

app.listen(3003, () => {
  console.log('Server listening on port 3003');
});

console.log('Test completed');