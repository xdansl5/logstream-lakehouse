#!/usr/bin/env node

import fetch from 'node-fetch';

const SERVER_URL = 'http://localhost:3001';

async function testIcebergService() {
  console.log('🧪 Testing Iceberg Analytics Server...\n');

  try {
    // Test 1: Health Check
    console.log('1️⃣ Testing Health Check...');
    const healthResponse = await fetch(`${SERVER_URL}/health`);
    const healthData = await healthResponse.json();
    
    if (healthResponse.ok) {
      console.log('✅ Health check passed:', healthData);
    } else {
      console.log('❌ Health check failed:', healthData);
      return;
    }

    // Test 2: List Tables
    console.log('\n2️⃣ Testing Table Listing...');
    const tablesResponse = await fetch(`${SERVER_URL}/api/tables`);
    const tablesData = await tablesResponse.json();
    
    if (tablesResponse.ok) {
      console.log('✅ Tables listed successfully:', tablesData);
    } else {
      console.log('❌ Table listing failed:', tablesData);
    }

    // Test 3: Get Table Schema
    console.log('\n3️⃣ Testing Schema Retrieval...');
    const schemaResponse = await fetch(`${SERVER_URL}/api/tables/logs/schema`);
    const schemaData = await schemaResponse.json();
    
    if (schemaResponse.ok) {
      console.log('✅ Schema retrieved successfully:', schemaData);
    } else {
      console.log('❌ Schema retrieval failed:', schemaData);
    }

    // Test 4: Get Table Data
    console.log('\n4️⃣ Testing Data Retrieval...');
    const dataResponse = await fetch(`${SERVER_URL}/api/tables/logs/data?limit=5`);
    const dataData = await dataResponse.json();
    
    if (dataResponse.ok) {
      console.log('✅ Data retrieved successfully. Sample records:', dataData.data.slice(0, 2));
      console.log(`   Total records returned: ${dataData.data.length}`);
    } else {
      console.log('❌ Data retrieval failed:', dataData);
    }

    // Test 5: Get Table Statistics
    console.log('\n5️⃣ Testing Statistics Retrieval...');
    const statsResponse = await fetch(`${SERVER_URL}/api/tables/logs/stats`);
    const statsData = await statsResponse.json();
    
    if (statsResponse.ok) {
      console.log('✅ Statistics retrieved successfully:', statsData);
    } else {
      console.log('❌ Statistics retrieval failed:', statsData);
    }

    // Test 6: Execute SQL Queries
    console.log('\n6️⃣ Testing SQL Query Execution...');
    
    const testQueries = [
      'SELECT * FROM logs LIMIT 10',
      'SELECT endpoint, COUNT(*) as count FROM logs GROUP BY endpoint',
      'SELECT * FROM logs WHERE status >= 400 LIMIT 5',
      'SELECT source, AVG(response_time) as avg_time FROM logs GROUP BY source'
    ];

    for (let i = 0; i < testQueries.length; i++) {
      const query = testQueries[i];
      console.log(`   Testing query ${i + 1}: ${query}`);
      
      try {
        const queryResponse = await fetch(`${SERVER_URL}/api/query`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ query })
        });
        
        const queryData = await queryResponse.json();
        
        if (queryResponse.ok) {
          console.log(`   ✅ Query ${i + 1} executed successfully in ${queryData.executionTime}ms`);
          console.log(`      Rows returned: ${queryData.rowCount}`);
        } else {
          console.log(`   ❌ Query ${i + 1} failed:`, queryData);
        }
      } catch (error) {
        console.log(`   ❌ Query ${i + 1} error:`, error.message);
      }
    }

    // Test 7: Data Ingestion
    console.log('\n7️⃣ Testing Data Ingestion...');
    const sampleData = [
      {
        id: `test_${Date.now()}`,
        timestamp: new Date().toISOString(),
        level: 'INFO',
        source: 'test-script',
        message: 'Test log entry from test script',
        ip: '127.0.0.1',
        status: 200,
        response_time: 150,
        endpoint: '/test',
        user_id: 'test_user',
        session_id: 'test_session'
      }
    ];

    try {
      const ingestResponse = await fetch(`${SERVER_URL}/api/ingest`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ data: sampleData })
      });
      
      const ingestData = await ingestResponse.json();
      
      if (ingestResponse.ok) {
        console.log('✅ Data ingestion successful:', ingestData);
      } else {
        console.log('❌ Data ingestion failed:', ingestData);
      }
    } catch (error) {
      console.log('❌ Data ingestion error:', error.message);
    }

    // Test 8: Server-Sent Events
    console.log('\n8️⃣ Testing Server-Sent Events...');
    try {
      const eventSource = new EventSource(`${SERVER_URL}/events`);
      
      eventSource.onopen = () => {
        console.log('✅ SSE connection opened');
      };
      
      eventSource.onmessage = (event) => {
        const data = JSON.parse(event.data);
        console.log('📡 SSE message received:', data);
        
        if (data.type === 'connection') {
          console.log('✅ SSE connection established successfully');
          eventSource.close();
        }
      };
      
      eventSource.onerror = (error) => {
        console.log('❌ SSE error:', error);
        eventSource.close();
      };
      
      // Close after 5 seconds if no connection message
      setTimeout(() => {
        eventSource.close();
        console.log('⏰ SSE test timeout');
      }, 5000);
      
    } catch (error) {
      console.log('❌ SSE test error:', error.message);
    }

    console.log('\n🎉 All tests completed!');
    console.log('\n📊 Server Status:');
    console.log(`   URL: ${SERVER_URL}`);
    console.log(`   Health: ${healthData.status}`);
    console.log(`   Version: ${healthData.version}`);
    console.log(`   Service: ${healthData.service}`);

  } catch (error) {
    console.error('❌ Test execution failed:', error.message);
    console.error('Make sure the server is running on port 3001');
    console.error('Run: npm run server');
  }
}

// Run tests
testIcebergService();