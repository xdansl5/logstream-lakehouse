#!/usr/bin/env node

const fetch = require('node-fetch');

const SERVER_URL = process.env.SERVER_URL || 'http://localhost:4000';

async function testDeltaLakeIntegration() {
  console.log('🧪 Testing Delta Lake Integration...\n');

  try {
    // Test 1: List tables
    console.log('1. Testing table listing...');
    const tablesResponse = await fetch(`${SERVER_URL}/api/tables`);
    if (!tablesResponse.ok) {
      throw new Error(`Failed to list tables: ${tablesResponse.status}`);
    }
    const tables = await tablesResponse.json();
    console.log(`✅ Found ${tables.tables.length} tables`);
    tables.tables.forEach(table => {
      console.log(`   - ${table.name} (${table.schema.fields.length} columns)`);
    });

    // Test 2: Get table schema
    console.log('\n2. Testing schema retrieval...');
    const schemaResponse = await fetch(`${SERVER_URL}/api/tables/logs/schema`);
    if (!schemaResponse.ok) {
      throw new Error(`Failed to get schema: ${schemaResponse.status}`);
    }
    const schema = await schemaResponse.json();
    console.log(`✅ Retrieved schema for 'logs' table`);
    console.log(`   Columns: ${schema.schema.fields.map(f => f.name).join(', ')}`);

    // Test 3: Execute a simple query
    console.log('\n3. Testing query execution...');
    const queryResponse = await fetch(`${SERVER_URL}/api/query`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        query: 'SELECT count(*) as total_records FROM delta_lake.logs'
      }),
    });
    
    if (!queryResponse.ok) {
      throw new Error(`Failed to execute query: ${queryResponse.status}`);
    }
    const queryResult = await queryResponse.json();
    console.log(`✅ Query executed successfully in ${queryResult.executionTime}`);
    console.log(`   Results: ${queryResult.rowCount} rows returned`);

    // Test 4: Execute a more complex query
    console.log('\n4. Testing complex query...');
    const complexQueryResponse = await fetch(`${SERVER_URL}/api/query`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        query: `SELECT 
  level,
  count(*) as count,
  avg(response_time) as avg_response_time
FROM delta_lake.logs 
WHERE timestamp >= current_timestamp() - interval 1 day
GROUP BY level
ORDER BY count DESC`
      }),
    });
    
    if (!complexQueryResponse.ok) {
      throw new Error(`Failed to execute complex query: ${complexQueryResponse.status}`);
    }
    const complexResult = await complexQueryResponse.json();
    console.log(`✅ Complex query executed successfully in ${complexResult.executionTime}`);
    console.log(`   Results: ${complexResult.rowCount} rows returned`);
    
    if (complexResult.results.length > 0) {
      console.log('   Sample results:');
      complexResult.results.slice(0, 3).forEach(row => {
        console.log(`     - ${row.level}: ${row.count} records, avg ${row.avg_response_time}ms`);
      });
    }

    // Test 5: Health check
    console.log('\n5. Testing health endpoint...');
    const healthResponse = await fetch(`${SERVER_URL}/health`);
    if (!healthResponse.ok) {
      throw new Error(`Health check failed: ${healthResponse.status}`);
    }
    const health = await healthResponse.json();
    console.log(`✅ Health check passed: ${health.status}`);
    console.log(`   SSE clients: ${health.clients}`);

    console.log('\n🎉 All tests passed! Delta Lake integration is working correctly.');
    console.log('\n📊 You can now:');
    console.log('   - Open http://localhost:5173 to access the web interface');
    console.log('   - Execute queries in the SQL interface');
    console.log('   - Explore table schemas in the Delta Lake Explorer');
    console.log('   - View real-time streaming data');

  } catch (error) {
    console.error('\n❌ Test failed:', error.message);
    console.log('\n🔧 Troubleshooting:');
    console.log('   1. Make sure the server is running: npm run server');
    console.log('   2. Check if port 4000 is available');
    console.log('   3. Verify the server logs for any errors');
    process.exit(1);
  }
}

// Run the tests
testDeltaLakeIntegration();