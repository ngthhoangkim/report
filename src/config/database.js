let sql = require('mssql');
const os = require('os');
require('dotenv').config();
const logger = require('../utils/logger');

function parseBool(value, defaultValue) {
  if (value === undefined) return defaultValue;
  return String(value).toLowerCase() === 'true';
}

function buildSqlConfig() {
  const isWindows = os.platform() === 'win32';
  const fallbackServer = '172.16.2.240';
  const fallbackDatabase = 'Hospital_NM';
  const connectTimeout = parseInt(process.env.DB_CONNECT_TIMEOUT_MS, 10) || 15000;
  const requestTimeout = parseInt(process.env.DB_REQUEST_TIMEOUT_MS, 10) || 120000;

  const authMode = String(process.env.DB_AUTH_MODE || '').toLowerCase();
  const envHasSqlLogin = Boolean(process.env.DB_USER && process.env.DB_PASSWORD);
  const hasSqlLogin = envHasSqlLogin;
  const useWindowsAuth = authMode === 'windows' || (!hasSqlLogin && authMode !== 'sql');

  if (useWindowsAuth) {
    try {
      sql = require('mssql/msnodesqlv8');
    } catch (error) {
      throw new Error(
        'Windows Authentication requires package "msnodesqlv8". Please run: npm install msnodesqlv8'
      );
    }

    const server = process.env.DB_SERVER || (isWindows ? fallbackServer : 'localhost');
    const database = process.env.DB_DATABASE || fallbackDatabase;
    const encrypt = parseBool(process.env.DB_ENCRYPT, true);
    const trustServerCertificate = parseBool(process.env.DB_TRUST_SERVER_CERTIFICATE, true);
    const connectionString = process.env.DB_CONN_STR || (
      `Driver={ODBC Driver 17 for SQL Server};` +
      `Server=${server};` +
      `Database=${database};` +
      'Trusted_Connection=Yes;' +
      `Encrypt=${encrypt ? 'Yes' : 'No'};` +
      `TrustServerCertificate=${trustServerCertificate ? 'Yes' : 'No'};`
    );

    return {
      server,
      database,
      driver: 'msnodesqlv8',
      connectionString,
      options: {
        trustedConnection: true,
        encrypt,
        trustServerCertificate,
        connectTimeout,
        requestTimeout,
      },
    };
  }

  // SQL Authentication (username/password)
  return {
    server: process.env.DB_SERVER || (isWindows ? fallbackServer : 'localhost'),
    database: process.env.DB_DATABASE || fallbackDatabase,
    authentication: {
      type: 'default',
      options: {
        userName: process.env.DB_USER || (isWindows ? 'sa' : 'sa'),
        password: process.env.DB_PASSWORD || '',
      },
    },
    options: {
      encrypt: parseBool(process.env.DB_ENCRYPT, false),
      trustServerCertificate: parseBool(process.env.DB_TRUST_SERVER_CERTIFICATE, true),
      connectTimeout,
      requestTimeout,
    },
    port: parseInt(process.env.DB_PORT, 10) || 1433,
  };
}

const sqlConfig = buildSqlConfig();

let pool = null;

/**
 * Khởi tạo connection pool
 */
async function initializePool() {
  try {
    pool = new sql.ConnectionPool(sqlConfig);
    await pool.connect();
    logger.info('✓ Database connected successfully');
    return pool;
  } catch (error) {
    logger.error('✗ Failed to connect to database:', error?.message || error);
    if (error?.originalError) {
      logger.error('Original DB error:', JSON.stringify(error.originalError));
    }
    throw error;
  }
}

/**
 * Lấy pool hiện tại hoặc khởi tạo mới
 */
async function getPool() {
  if (!pool) {
    await initializePool();
  }
  return pool;
}

/**
 * Đóng kết nối pool
 */
async function closePool() {
  if (pool) {
    await pool.close();
    pool = null;
    logger.info('Database connection closed');
  }
}

/**
 * Thực hiện truy vấn SQL
 * @param {string} query - SQL query string
 * @param {object} params - Parameter cho query
 * @returns {array} Mảng kết quả
 */
async function executeQuery(query, params = {}) {
  try {
    const pool = await getPool();
    const request = pool.request();

    // Thêm parameters
    for (const [key, value] of Object.entries(params)) {
      request.input(key, value);
    }

    const result = await request.query(query);
    return result.recordset || [];
  } catch (error) {
    logger.error('Query execution error:', error.message);
    throw error;
  }
}

/**
 * Thực hiện stored procedure
 * @param {string} procedureName - Tên stored procedure
 * @param {object} params - Parameters
 * @returns {array} Kết quả
 */
async function executeStoredProcedure(procedureName, params = {}) {
  try {
    const pool = await getPool();
    const request = pool.request();

    for (const [key, value] of Object.entries(params)) {
      request.input(key, value);
    }

    const result = await request.execute(procedureName);
    return result.recordset || [];
  } catch (error) {
    logger.error(`Stored procedure error (${procedureName}):`, error.message);
    throw error;
  }
}

module.exports = {
  initializePool,
  getPool,
  closePool,
  executeQuery,
  executeStoredProcedure,
  sql,
};
