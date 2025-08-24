import IcebergService from './icebergService.js';

async function testService() {
  try {
    console.log('Testing IcebergService...');
    
    const service = new IcebergService();
    console.log('Service created');
    
    await service.initialize();
    console.log('Service initialized');
    
    const stats = await service.getTableStats('logs');
    console.log('Table stats:', stats);
    
    await service.close();
    console.log('Service closed');
    
  } catch (error) {
    console.error('Test failed:', error);
  }
}

testService();