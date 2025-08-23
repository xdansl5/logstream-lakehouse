import { readFileSync, writeFileSync, existsSync, mkdirSync } from 'fs';
import { join } from 'url';
import { fileURLToPath } from 'url';
import { dirname } from 'url';
import { Table, Vector, Int32, Utf8, Timestamp, Bool, Float64 } from 'apache-arrow';
import winston from 'winston';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// Configure logging
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
    new winston.transports.File({ filename: 'logs/arrow-service.log' })
  ]
});

class ArrowService {
  constructor() {
    this.dataPath = process.env.ARROW_DATA_PATH || join(__dirname, '../data/arrow');
    this.ensureDataDirectory();
  }

  ensureDataDirectory() {
    if (!existsSync(this.dataPath)) {
      mkdirSync(this.dataPath, { recursive: true });
    }
  }

  // Convert data to Apache Arrow format
  createArrowTable(data) {
    try {
      if (!Array.isArray(data) || data.length === 0) {
        throw new Error('Data must be a non-empty array');
      }

      const sample = data[0];
      const columns = Object.keys(sample);
      
      // Create vectors for each column
      const vectors = {};
      
      columns.forEach(column => {
        const values = data.map(row => row[column]);
        const firstValue = values[0];
        
        if (typeof firstValue === 'number') {
          if (Number.isInteger(firstValue)) {
            vectors[column] = new Int32(new Int32Array(values));
          } else {
            vectors[column] = new Float64(new Float64Array(values));
          }
        } else if (firstValue instanceof Date || typeof firstValue === 'string') {
          // Handle timestamps and strings
          if (firstValue instanceof Date || (typeof firstValue === 'string' && firstValue.match(/^\d{4}-\d{2}-\d{2}/))) {
            const timestamps = values.map(v => new Date(v).getTime());
            vectors[column] = new Timestamp(new Int32Array(timestamps));
          } else {
            vectors[column] = new Utf8(values);
          }
        } else if (typeof firstValue === 'boolean') {
          vectors[column] = new Bool(new Uint8Array(values.map(v => v ? 1 : 0)));
        } else {
          // Default to string for unknown types
          vectors[column] = new Utf8(values.map(v => String(v)));
        }
      });

      return new Table(vectors);
    } catch (error) {
      logger.error('Error creating Arrow table:', error);
      throw error;
    }
  }

  // Save data as Parquet file
  async saveAsParquet(data, filename) {
    try {
      const table = this.createArrowTable(data);
      const parquetPath = join(this.dataPath, `${filename}.parquet`);
      
      // In a real implementation, you'd use a Parquet writer
      // For now, we'll save the Arrow table as JSON for demonstration
      const jsonPath = join(this.dataPath, `${filename}.arrow.json`);
      
      const tableData = {
        schema: table.schema.toJSON(),
        data: table.toArray().map(row => {
          const obj = {};
          table.schema.fields.forEach((field, i) => {
            obj[field.name] = row[i];
          });
          return obj;
        })
      };
      
      writeFileSync(jsonPath, JSON.stringify(tableData, null, 2));
      
      logger.info(`Arrow table saved to ${jsonPath}`);
      return jsonPath;
    } catch (error) {
      logger.error('Error saving Arrow table:', error);
      throw error;
    }
  }

  // Load data from Parquet/Arrow file
  async loadFromFile(filename) {
    try {
      const jsonPath = join(this.dataPath, `${filename}.arrow.json`);
      
      if (!existsSync(jsonPath)) {
        throw new Error(`File not found: ${jsonPath}`);
      }
      
      const fileContent = readFileSync(jsonPath, 'utf8');
      const tableData = JSON.parse(fileContent);
      
      logger.info(`Arrow table loaded from ${jsonPath}`);
      return tableData;
    } catch (error) {
      logger.error('Error loading Arrow table:', error);
      throw error;
    }
  }

  // Execute queries on Arrow data
  async executeQuery(data, query) {
    try {
      // Simple query execution for demonstration
      // In a real implementation, you'd use Arrow's compute functions
      
      if (query.toLowerCase().includes('where')) {
        // Basic WHERE clause parsing
        return this.executeWhereClause(data, query);
      } else if (query.toLowerCase().includes('group by')) {
        // Basic GROUP BY parsing
        return this.executeGroupByClause(data, query);
      } else {
        // Simple SELECT
        return this.executeSelectClause(data, query);
      }
    } catch (error) {
      logger.error('Error executing Arrow query:', error);
      throw error;
    }
  }

  executeWhereClause(data, query) {
    // Simple WHERE clause implementation
    let filteredData = [...data];
    
    if (query.toLowerCase().includes('status >= 400')) {
      filteredData = filteredData.filter(row => row.status >= 400);
    }
    
    if (query.toLowerCase().includes('level = \'error\'')) {
      filteredData = filteredData.filter(row => row.level.toLowerCase() === 'error');
    }
    
    if (query.toLowerCase().includes('response_time > 1000')) {
      filteredData = filteredData.filter(row => row.response_time > 1000);
    }
    
    return filteredData;
  }

  executeGroupByClause(data, query) {
    // Simple GROUP BY implementation
    const groups = {};
    
    data.forEach(row => {
      let key = 'all';
      
      if (query.toLowerCase().includes('group by endpoint')) {
        key = row.endpoint;
      } else if (query.toLowerCase().includes('group by source')) {
        key = row.source;
      } else if (query.toLowerCase().includes('group by level')) {
        key = row.level;
      }
      
      if (!groups[key]) {
        groups[key] = {
          count: 0,
          total_response_time: 0,
          errors: 0
        };
      }
      
      groups[key].count++;
      groups[key].total_response_time += row.response_time;
      if (row.status >= 400) {
        groups[key].errors++;
      }
    });
    
    // Calculate averages
    Object.keys(groups).forEach(key => {
      groups[key].avg_response_time = Math.round(groups[key].total_response_time / groups[key].count);
      delete groups[key].total_response_time;
    });
    
    return Object.entries(groups).map(([key, values]) => ({
      group: key,
      ...values
    }));
  }

  executeSelectClause(data, query) {
    // Simple SELECT implementation
    if (query.toLowerCase().includes('count(*)')) {
      return [{ count: data.length }];
    }
    
    if (query.toLowerCase().includes('limit')) {
      const limitMatch = query.match(/limit\s+(\d+)/i);
      const limit = limitMatch ? parseInt(limitMatch[1]) : 100;
      return data.slice(0, limit);
    }
    
    return data;
  }

  // Get table statistics
  getTableStats(data) {
    if (!Array.isArray(data) || data.length === 0) {
      return null;
    }
    
    const stats = {
      total_records: data.length,
      unique_levels: new Set(data.map(row => row.level)).size,
      unique_sources: new Set(data.map(row => row.source)).size,
      unique_endpoints: new Set(data.map(row => row.endpoint)).size,
      avg_response_time: 0,
      min_timestamp: null,
      max_timestamp: null
    };
    
    let totalResponseTime = 0;
    let minTime = Infinity;
    let maxTime = -Infinity;
    
    data.forEach(row => {
      totalResponseTime += row.response_time;
      
      const timestamp = new Date(row.timestamp).getTime();
      if (timestamp < minTime) minTime = timestamp;
      if (timestamp > maxTime) maxTime = timestamp;
    });
    
    stats.avg_response_time = Math.round(totalResponseTime / data.length);
    stats.min_timestamp = new Date(minTime).toISOString();
    stats.max_timestamp = new Date(maxTime).toISOString();
    
    return stats;
  }

  // Convert Arrow table to different formats
  convertToCSV(data) {
    if (!Array.isArray(data) || data.length === 0) {
      return '';
    }
    
    const headers = Object.keys(data[0]);
    const csvRows = [headers.join(',')];
    
    data.forEach(row => {
      const values = headers.map(header => {
        const value = row[header];
        // Escape commas and quotes in CSV
        if (typeof value === 'string' && (value.includes(',') || value.includes('"'))) {
          return `"${value.replace(/"/g, '""')}"`;
        }
        return value;
      });
      csvRows.push(values.join(','));
    });
    
    return csvRows.join('\n');
  }

  convertToJSON(data, pretty = false) {
    if (pretty) {
      return JSON.stringify(data, null, 2);
    }
    return JSON.stringify(data);
  }
}

export default ArrowService;