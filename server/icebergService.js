import { readFileSync, writeFileSync, existsSync, mkdirSync } from 'fs';
import { join } from 'path';
import { fileURLToPath } from 'url';
import { dirname } from 'path';
import pkg from 'duckdb';
const { Database } = pkg;
import { Table, Vector, Int32, Utf8, Timestamp, Bool, Float64 } from 'apache-arrow';
import winston from 'winston';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// Configure modern logging
const logger = winston.createLogger({
  level: 'info',
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.errors({ stack: true }),
    winston.format.json()
  ),
  transports: [
    new winston.transports.Console({
      format: winston.format.combine(
        winston.format.colorize(),
        winston.format.simple()
      )
    }),
    new winston.transports.File({ filename: 'logs/iceberg-service.log' })
  ]
});

class IcebergService {
  constructor() {
    this.tablePath = process.env.ICEBERG_TABLE_PATH || join(__dirname, '../data/iceberg');
    this.duckdb = null;
    this.conn = null;
    this.ensureDataDirectory();
    this.initialized = false;
  }

  async initialize() {
    if (this.initialized) return;
    
    try {
      logger.info('Starting DuckDB initialization...');
      await this.initializeDuckDB();
      await this.initializeSampleData();
      this.initialized = true;
      logger.info('IcebergService initialized successfully');
    } catch (error) {
      logger.error('Failed to initialize IcebergService:', error);
      throw error;
    }
  }

  ensureDataDirectory() {
    if (!existsSync(this.tablePath)) {
      mkdirSync(this.tablePath, { recursive: true });
    }
    if (!existsSync(join(__dirname, '../logs'))) {
      mkdirSync(join(__dirname, '../logs'), { recursive: true });
    }
  }

  async initializeDuckDB() {
    return new Promise((resolve, reject) => {
      try {
        logger.info('Creating DuckDB instance...');
        
        // Create DuckDB instance with in-memory database
        this.duckdb = new Database(':memory:');
        logger.info('DuckDB instance created');
        
        // Create connection
        this.conn = this.duckdb.connect();
        logger.info('DuckDB connection established');
        
        // Initialize tables
        this.createTables()
          .then(() => {
            logger.info('DuckDB tables initialized successfully');
            resolve();
          })
          .catch(reject);
        
      } catch (error) {
        logger.error('Error initializing DuckDB:', error);
        reject(error);
      }
    });
  }

  async createTables() {
    return new Promise((resolve, reject) => {
      try {
        // Create logs table
        this.conn.exec(`
          CREATE TABLE IF NOT EXISTS logs (
            id VARCHAR,
            timestamp TIMESTAMP,
            level VARCHAR,
            source VARCHAR,
            message VARCHAR,
            ip VARCHAR,
            status INTEGER,
            response_time INTEGER,
            endpoint VARCHAR,
            user_id VARCHAR,
            session_id VARCHAR
          )
        `, (err) => {
          if (err) {
            logger.error('Error creating logs table:', err);
            reject(err);
            return;
          }
          logger.info('Logs table created/verified');

          // Create iceberg_metadata table
          this.conn.exec(`
            CREATE TABLE IF NOT EXISTS iceberg_metadata (
              table_name VARCHAR,
              schema_version INTEGER,
              last_updated TIMESTAMP,
              record_count INTEGER,
              metadata VARCHAR
            )
          `, (err2) => {
            if (err2) {
              logger.error('Error creating iceberg_metadata table:', err2);
              reject(err2);
              return;
            }
            logger.info('Iceberg metadata table created/verified');
            resolve();
          });
        });
        
      } catch (error) {
        logger.error('Error creating tables:', error);
        reject(error);
      }
    });
  }

  async tableExists(tableName) {
    return new Promise((resolve, reject) => {
      this.conn.all(`
        SELECT name FROM sqlite_master 
        WHERE type='table' AND name='${tableName}'
      `, (err, rows) => {
        if (err) {
          logger.error(`Error checking if table ${tableName} exists:`, err);
          reject(err);
        } else {
          resolve(rows.length > 0);
        }
      });
    });
  }

  async createSampleLogsTable() {
    try {
      const sampleLogs = this.generateSampleLogs();
      logger.info(`Generated ${sampleLogs.length} sample logs`);
      
      // Clear existing data
      await this.clearTable('logs');
      logger.info('Cleared existing logs table');
      
      // Insert sample data in batches
      const batchSize = 1000;
      let successCount = 0;
      let errorCount = 0;
      
      for (let i = 0; i < sampleLogs.length; i += batchSize) {
        const batch = sampleLogs.slice(i, i + batchSize);
        
        try {
          // Create batch insert query
          const values = batch.map(log => 
            `('${log.id}', '${log.timestamp}', '${log.level}', '${log.source}', '${log.message}', '${log.ip}', ${log.status}, ${log.response_time}, '${log.endpoint}', '${log.user_id}', '${log.session_id}')`
          ).join(',');
          
          const insertQuery = `
            INSERT INTO logs (id, timestamp, level, source, message, ip, status, response_time, endpoint, user_id, session_id) 
            VALUES ${values}
          `;
          
          await this.executeQuery(insertQuery);
          successCount += batch.length;
          
          if (i % 5000 === 0) {
            logger.info(`Inserted ${successCount} logs so far...`);
          }
          
        } catch (error) {
          logger.warn(`Failed to insert batch starting at index ${i}:`, error.message);
          errorCount += batch.length;
        }
      }

      logger.info(`Inserted ${successCount} logs successfully, ${errorCount} failed`);

      // Update metadata
      await this.updateTableMetadata('logs', successCount);

      // Export to Parquet file for Iceberg compatibility
      await this.exportToParquet('logs');

      logger.info(`Sample Iceberg table created with ${successCount} records`);
      return successCount > 0;
    } catch (error) {
      logger.error('Error creating sample table:', error);
      return false;
    }
  }

  async exportToParquet(tableName) {
    return new Promise((resolve, reject) => {
      try {
        const parquetPath = join(this.tablePath, `${tableName}.parquet`);
        
        this.conn.exec(`
          COPY (SELECT * FROM ${tableName}) TO '${parquetPath}' (FORMAT PARQUET)
        `, (err) => {
          if (err) {
            logger.error('Error exporting to Parquet:', err);
            reject(err);
          } else {
            logger.info(`Table ${tableName} exported to Parquet successfully at ${parquetPath}`);
            resolve();
          }
        });
      } catch (error) {
        logger.error('Error exporting to Parquet:', error);
        reject(error);
      }
    });
  }

  async updateTableMetadata(tableName, recordCount) {
    return new Promise((resolve, reject) => {
      try {
        const metadata = {
          format: 'iceberg',
          version: '1.0.0',
          compression: 'snappy',
          created_at: new Date().toISOString()
        };

        // First clear existing metadata for this table
        this.conn.run(`DELETE FROM iceberg_metadata WHERE table_name = ?`, [tableName], (err) => {
          if (err) {
            logger.error('Error clearing existing metadata:', err);
            reject(err);
            return;
          }
          
          // Then insert new metadata using string interpolation for simplicity
          const insertQuery = `
            INSERT INTO iceberg_metadata VALUES ('${tableName}', 1, '${new Date().toISOString()}', ${recordCount}, '${JSON.stringify(metadata)}')
          `;
          
          this.conn.run(insertQuery, (err2) => {
            if (err2) {
              logger.error('Error inserting table metadata:', err2);
              reject(err2);
            } else {
              logger.info(`Metadata updated for table ${tableName}`);
              resolve();
            }
          });
        });
        
      } catch (error) {
        logger.error('Error updating table metadata:', error);
        reject(error);
      }
    });
  }

  generateSampleLogs() {
    const logs = [];
    const sources = ['spark-streaming', 'kafka-consumer', 'iceberg-writer', 'web-server', 'api-gateway'];
    const endpoints = ['/api/users', '/api/orders', '/api/products', '/health', '/metrics'];
    const levels = ['INFO', 'WARN', 'ERROR', 'DEBUG'];
    
    const now = new Date();
    const sevenDaysAgo = new Date(now.getTime() - (7 * 24 * 60 * 60 * 1000));
    
    for (let i = 0; i < 10000; i++) {
      const timestamp = new Date(sevenDaysAgo.getTime() + Math.random() * (now.getTime() - sevenDaysAgo.getTime()));
      const source = sources[Math.floor(Math.random() * sources.length)];
      const endpoint = endpoints[Math.floor(Math.random() * endpoints.length)];
      const level = levels[Math.floor(Math.random() * levels.length)];
      const status = level === 'ERROR' ? 
        (Math.random() > 0.5 ? 500 : 404) : 
        (Math.random() > 0.1 ? 200 : 400);
      const responseTime = Math.floor(Math.random() * 2000) + 50;
      
      logs.push({
        id: `log_${i}_${Date.now()}`,
        timestamp: timestamp.toISOString(),
        level,
        source,
        message: `${source} processing ${endpoint} - ${level.toLowerCase()} level event`,
        ip: `192.168.${Math.floor(Math.random() * 255)}.${Math.floor(Math.random() * 255)}`,
        status,
        response_time: responseTime,
        endpoint,
        user_id: `user_${Math.floor(Math.random() * 1000)}`,
        session_id: `sess_${Math.random().toString(36).substr(2, 8)}`
      });
    }
    
    return logs;
  }

  async executeQuery(query) {
    return new Promise((resolve, reject) => {
      try {
        const startTime = Date.now();
        
        this.conn.all(query, (err, rows) => {
          const executionTime = Date.now() - startTime;
          
          if (err) {
            logger.error('Query execution error:', err);
            reject(err);
          } else {
            // Convert BigInt values to regular numbers for JSON serialization
            const processedRows = rows.map(row => {
              const processedRow = {};
              Object.keys(row).forEach(key => {
                if (typeof row[key] === 'bigint') {
                  processedRow[key] = Number(row[key]);
                } else {
                  processedRow[key] = row[key];
                }
              });
              return processedRow;
            });
            
            logger.info(`Query executed successfully in ${executionTime}ms, returned ${processedRows.length} rows`);
            resolve({
              data: processedRows,
              executionTime,
              rowCount: processedRows.length,
              query: query
            });
          }
        });
      } catch (error) {
        logger.error('Query execution error:', error);
        reject(error);
      }
    });
  }

  async getTableSchema(tableName) {
    return new Promise((resolve, reject) => {
      this.conn.all(`PRAGMA table_info(${tableName})`, (err, rows) => {
        if (err) {
          logger.error(`Error getting table schema for ${tableName}:`, err);
          reject(err);
        } else {
          resolve(rows);
        }
      });
    });
  }

  async getTableData(tableName, limit = 100) {
    return new Promise((resolve, reject) => {
      this.conn.all(`SELECT * FROM ${tableName} LIMIT ${limit}`, (err, rows) => {
        if (err) {
          logger.error(`Error getting table data for ${tableName}:`, err);
          reject(err);
        } else {
          resolve(rows);
        }
      });
    });
  }

  async getTableStats(tableName) {
    return new Promise((resolve, reject) => {
      this.conn.all(`
        SELECT 
          COUNT(*) as total_records,
          COUNT(DISTINCT level) as unique_levels,
          COUNT(DISTINCT source) as unique_sources,
          COUNT(DISTINCT endpoint) as unique_endpoints,
          AVG(response_time) as avg_response_time,
          MIN(timestamp) as earliest_record,
          MAX(timestamp) as latest_record
        FROM ${tableName}
      `, (err, rows) => {
        if (err) {
          logger.error(`Error getting table stats for ${tableName}:`, err);
          reject(err);
        } else {
          // Convert BigInt values to regular numbers for JSON serialization
          const stats = rows[0];
          if (stats) {
            Object.keys(stats).forEach(key => {
              if (typeof stats[key] === 'bigint') {
                stats[key] = Number(stats[key]);
              }
            });
          }
          resolve(stats);
        }
      });
    });
  }

  async close() {
    return new Promise((resolve) => {
      try {
        if (this.conn) {
          this.conn.close();
          logger.info('DuckDB connection closed');
        }
        if (this.duckdb) {
          this.duckdb.close();
          logger.info('DuckDB instance closed');
        }
        resolve();
      } catch (error) {
        logger.error('Error closing DuckDB:', error);
        resolve();
      }
    });
  }

  async initializeSampleData() {
    try {
      logger.info('Starting sample data initialization...');
      const tableExists = await this.tableExists('logs');
      logger.info(`Table 'logs' exists: ${tableExists}`);
      
      logger.info('Creating sample logs table...');
      await this.createSampleLogsTable();
      logger.info('Sample logs table created successfully');
    } catch (error) {
      logger.error('Error initializing sample data:', error);
      throw error;
    }
  }

  async clearTable(tableName) {
    return new Promise((resolve, reject) => {
      this.conn.run(`DELETE FROM ${tableName}`, (err) => {
        if (err) {
          logger.error(`Error clearing table ${tableName}:`, err);
          reject(err);
        } else {
          logger.info(`Table ${tableName} cleared successfully`);
          resolve();
        }
      });
    });
  }
}

export default IcebergService;