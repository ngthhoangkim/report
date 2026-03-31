/**
 * Test SQL Server connection using the same config as the app.
 *
 * Usage (Windows):
 *   node scripts/testDbConnection.js
 *
 * Requires .env to be set (DB_* variables).
 */
require('dotenv').config();

const os = require('os');
const db = require('../src/config/database');
const logger = require('../src/utils/logger');

function safeConfigSummary() {
  return {
    authMode: process.env.DB_AUTH_MODE || 'sql',
    server: process.env.DB_SERVER || '(empty)',
    database: process.env.DB_DATABASE || '(empty)',
    port: process.env.DB_PORT || '(default)',
    encrypt: process.env.DB_ENCRYPT,
    trustServerCertificate: process.env.DB_TRUST_SERVER_CERTIFICATE,
    user: process.env.DB_USER ? '(set)' : '(empty)',
    password: process.env.DB_PASSWORD ? '(set)' : '(empty)',
    platform: os.platform(),
  };
}

async function main() {
  const startedAt = new Date();
  logger.info('DB test starting', { startedAt: startedAt.toISOString(), config: safeConfigSummary() });

  try {
    await db.initializePool();

    // Basic connectivity test
    const ping = await db.executeQuery('SELECT 1 AS ok');
    logger.info('SELECT 1 result', { ping });

    // Validate key objects exist
    const objects = await db.executeQuery(`
      SELECT
        SUM(CASE WHEN o.type_desc = 'USER_TABLE' AND o.name = 'CN_ImagingResult' THEN 1 ELSE 0 END) AS Has_CN_ImagingResult,
        SUM(CASE WHEN o.type_desc = 'USER_TABLE' AND o.name = 'CN_ImagingResultData' THEN 1 ELSE 0 END) AS Has_CN_ImagingResultData,
        SUM(CASE WHEN o.type_desc = 'USER_TABLE' AND o.name = 'CN_PathologyImage' THEN 1 ELSE 0 END) AS Has_CN_PathologyImage,
        SUM(CASE WHEN o.type_desc = 'VIEW' AND o.name = 'ViewImagingResult' THEN 1 ELSE 0 END) AS Has_ViewImagingResult
      FROM sys.objects o
    `);
    logger.info('Schema presence check', { objects: objects[0] || objects });

    // Sample query (non-sensitive)
    const sample = await db.executeQuery(`
      SELECT TOP 3
        v.FileNum,
        v.SessionId,
        r.Id AS ImagingResultId,
        r.PathologyType,
        r.TemplateFile,
        r.FileName,
        r.CreatedDate
      FROM dbo.CN_ImagingResult r
      INNER JOIN dbo.ViewImagingResult v ON v.Id = r.Id
      WHERE r.DeletedDate IS NULL
      ORDER BY r.CreatedDate DESC
    `);
    logger.info('Sample imaging rows (TOP 3)', { count: sample.length, sample });

    logger.info('DB test OK', { durationMs: Date.now() - startedAt.getTime() });
    process.exitCode = 0;
  } catch (e) {
    logger.error('DB test FAILED', {
      durationMs: Date.now() - startedAt.getTime(),
      errorMessage: e?.message || String(e),
      errorStack: e?.stack || null,
      reason: e?.code || null,
    });
    process.exitCode = 1;
  } finally {
    try {
      await db.closePool();
    } catch (_) {
      // ignore
    }
  }
}

main();

