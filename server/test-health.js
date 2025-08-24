import IcebergService from './icebergService.js';

async function testHealthCheck() {
  try {
    console.log('Testing IcebergService health check...');
    
    const service = new IcebergService();
    console.log('Service created');
    
    await service.initialize();
    console.log('Service initialized');
    
    // Test health check query
    const healthResult = await service.executeQuery('SELECT 1 as health_check');
    console.log('Health check result:', healthResult);
    
    await service.close();
    console.log('Service closed');
    
  } catch (error) {
    console.error('Test failed:', error);
  }
}

testHealthCheck();