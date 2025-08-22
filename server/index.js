#!/usr/bin/env node
'use strict';

const express = require('express');
const cors = require('cors');
const { Kafka } = require('kafkajs');

const PORT = process.env.PORT ? parseInt(process.env.PORT, 10) : 4000;
const KAFKA_BROKERS = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');
const KAFKA_TOPIC = process.env.KAFKA_TOPIC || 'web-logs';
const KAFKA_GROUP_ID = process.env.KAFKA_GROUP_ID || 'ui-bridge-group';

const app = express();
app.use(cors({ origin: true, credentials: true }));

// In-memory list of connected SSE clients
const sseClients = new Set();

function sendSSE(res, data) {
	res.write(`data: ${data}\n\n`);
}

function broadcast(data) {
	for (const res of sseClients) {
		try { sendSSE(res, data); } catch (_) {}
	}
}

app.get('/health', (_req, res) => {
	res.json({ status: 'ok', topic: KAFKA_TOPIC, clients: sseClients.size });
});

app.get('/events', (req, res) => {
	res.setHeader('Content-Type', 'text/event-stream');
	res.setHeader('Cache-Control', 'no-cache');
	res.setHeader('Connection', 'keep-alive');
	res.flushHeaders && res.flushHeaders();

	// Initial hello event
	sendSSE(res, JSON.stringify({ type: 'hello', message: 'connected' }));

	sseClients.add(res);

	req.on('close', () => {
		sseClients.delete(res);
		try { res.end(); } catch (_) {}
	});
});

// Keep-alive pings to prevent proxies from closing the connection
setInterval(() => {
	for (const res of sseClients) {
		try { res.write(`: ping\n\n`); } catch (_) {}
	}
}, 25000);

function toUiLogEntry(kmsg) {
	// kmsg is parsed JSON from Kafka value
	const statusCode = kmsg.status_code ?? kmsg.status ?? 200;
	let level = 'INFO';
	if (typeof statusCode === 'number') {
		if (statusCode >= 500) level = 'ERROR';
		else if (statusCode >= 400) level = 'WARN';
	}

	const nowIso = new Date().toISOString();
	return {
		id: `${kmsg.session_id || ''}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
		timestamp: kmsg.timestamp || nowIso,
		level,
		source: 'kafka-consumer',
		message: `${kmsg.method || 'GET'} ${kmsg.endpoint || '/'} -> ${statusCode} ${kmsg.response_time ?? kmsg.responseTime ?? ''}ms`,
		ip: kmsg.ip,
		status: statusCode,
		responseTime: kmsg.response_time ?? kmsg.responseTime,
		endpoint: kmsg.endpoint,
		userId: kmsg.user_id || kmsg.userId,
		sessionId: kmsg.session_id || kmsg.sessionId,
	};
}

async function startKafka() {
	const kafka = new Kafka({ clientId: 'ui-bridge', brokers: KAFKA_BROKERS });
	const consumer = kafka.consumer({ groupId: KAFKA_GROUP_ID });

	await consumer.connect();
	await consumer.subscribe({ topic: KAFKA_TOPIC, fromBeginning: false });

	console.log(`✅ Kafka consumer connected. Topic: ${KAFKA_TOPIC}, Brokers: ${KAFKA_BROKERS.join(',')}`);

	await consumer.run({
		eachMessage: async ({ message, partition, topic }) => {
			try {
				const raw = message.value ? message.value.toString('utf8') : '';
				if (!raw) return;
				const parsed = JSON.parse(raw);
				const uiLog = toUiLogEntry(parsed);
				broadcast(JSON.stringify(uiLog));
			} catch (err) {
				console.error('Failed to process Kafka message:', err);
			}
		},
	});
}

startKafka().catch((err) => {
	console.error('Kafka startup error:', err);
	process.exitCode = 1;
});

app.listen(PORT, () => {
	console.log(`🟢 SSE server listening on http://localhost:${PORT}`);
	console.log(`➡️  Stream endpoint: http://localhost:${PORT}/events`);
});