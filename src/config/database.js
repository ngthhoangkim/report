let sql = require('mssql');
const os = require('os');
require('dotenv').config();
const logger = require('../utils/logger');

function envString(key, fallback = '') {
  const raw = process.env[key];
  if (raw == null) return fallback;
  const s = String(raw).trim();
  if (
    (s.startsWith('"') && s.endsWith('"') && s.length >= 2) ||
    (s.startsWith("'") && s.endsWith("'") && s.length >= 2)
  ) {
    return s.slice(1, -1);
  }
  return s;
}

function parseBool(value, defaultValue) {
  if (value === undefined) return defaultValue;
  return String(value).toLowerCase() === 'true';
}

function buildSqlConfig() {
  const isWindows = os.platform() === 'win32';
  const fallbackServer = '172.16.2.240';
  const fallbackDatabase = 'Hospital_NM';
  const connectTimeout = parseInt(envString('DB_CONNECT_TIMEOUT_MS', ''), 10) || 15000;
  const requestTimeout = parseInt(envString('DB_REQUEST_TIMEOUT_MS', ''), 10) || 120000;

  const authMode = envString('DB_AUTH_MODE', '').toLowerCase();
  const envHasSqlLogin = Boolean(envString('DB_USER', '') && envString('DB_PASSWORD', ''));
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

    const server = envString('DB_SERVER', '') || (isWindows ? fallbackServer : 'localhost');
    const database = envString('DB_DATABASE', '') || fallbackDatabase;
    const encrypt = parseBool(envString('DB_ENCRYPT', undefined), true);
    const trustServerCertificate = parseBool(envString('DB_TRUST_SERVER_CERTIFICATE', undefined), true);
    const connectionString = envString('DB_CONN_STR', '') || (
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
  const server = envString('DB_SERVER', '') || (isWindows ? fallbackServer : 'localhost');
  const database = envString('DB_DATABASE', '') || fallbackDatabase;
  const userName = envString('DB_USER', '') || (isWindows ? 'sa' : 'sa');
  const password = envString('DB_PASSWORD', '') || '';
  const encrypt = parseBool(envString('DB_ENCRYPT', undefined), false);
  const trustServerCertificate = parseBool(envString('DB_TRUST_SERVER_CERTIFICATE', undefined), true);
  const port = parseInt(envString('DB_PORT', ''), 10) || 1433;

  logger.info('DB config (safe)', {
    authMode: authMode || '(empty)',
    driver: 'tedious',
    server,
    database,
    port,
    encrypt,
    trustServerCertificate,
    user: userName ? '(set)' : '(empty)',
    connectTimeout,
    requestTimeout,
  });

  return {
    server,
    database,
    authentication: {
      type: 'default',
      options: {
        userName,
        password,
      },
    },
    options: {
      encrypt,
      trustServerCertificate,
      connectTimeout,
      requestTimeout,
    },
    port,
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
 * @param {{ silent?: boolean }} [options] - silent: true — không log lỗi (dùng cho truy vấn thử schema)
 * @returns {array} Mảng kết quả
 */
async function executeQuery(query, params = {}, options = {}) {
  const silent = options.silent === true;
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
    if (!silent) logger.error('Query execution error:', error.message);
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
