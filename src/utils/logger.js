const winston = require('winston');
const DailyRotateFile = require('winston-daily-rotate-file');
const fs = require('fs');
const path = require('path');

function ensureDir(dir) {
  try {
    if (dir && !fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  } catch (_) {
    // ignore
  }
}

const logsDir = process.env.LOGS_FOLDER
  ? path.resolve(process.cwd(), process.env.LOGS_FOLDER)
  : path.resolve(process.cwd(), 'logs');
ensureDir(logsDir);

const logMaxFiles = process.env.LOG_MAX_FILES || '30d';
const logMaxSize = process.env.LOG_MAX_SIZE || '20m';
const logZipArchive = String(process.env.LOG_ZIP_OLD_LOGS || 'true').toLowerCase() !== 'false';

/** Một dòng JSON — dễ grep; meta không pretty-print. */
const fileLineFormat = winston.format.printf(({ timestamp, level, message, ...meta }) => {
  const rest = { ...meta };
  delete rest.splat;
  const metaStr = Object.keys(rest).length ? ` ${JSON.stringify(rest)}` : '';
  return `${timestamp} [${level.toUpperCase()}]: ${message}${metaStr}`;
});

const fileFormat = winston.format.combine(winston.format.timestamp(), fileLineFormat);

const dailyApp = new DailyRotateFile({
  dirname: logsDir,
  filename: 'automation-%DATE%.log',
  datePattern: 'YYYY-MM-DD',
  zippedArchive: logZipArchive,
  maxSize: logMaxSize,
  maxFiles: logMaxFiles,
  level: 'info',
  format: fileFormat,
});

const dailyError = new DailyRotateFile({
  dirname: logsDir,
  filename: 'automation-error-%DATE%.log',
  datePattern: 'YYYY-MM-DD',
  zippedArchive: logZipArchive,
  maxSize: logMaxSize,
  maxFiles: logMaxFiles,
  level: 'warn',
  format: fileFormat,
});

const logger = winston.createLogger({
  level: 'info',
  format: fileFormat,
  transports: [
    new winston.transports.Console({
      format: winston.format.combine(
        winston.format.colorize(),
        winston.format.timestamp(),
        winston.format.printf(({ timestamp, level, message, ...meta }) => {
          const rest = { ...meta };
          delete rest.splat;
          const metaStr = Object.keys(rest).length ? ` ${JSON.stringify(rest, null, 2)}` : '';
          return `${timestamp} [${level}]: ${message}${metaStr}`;
        }),
      ),
    }),
    dailyApp,
    dailyError,
  ],
});

/**
 * Một dòng JSON / phiên automation — mở đúng file theo ngày, lướt nhanh OK/fail.
 * Tắt: LOG_AUTOMATION_SUMMARY=false
 */
function appendAutomationSummary(entry) {
  if (String(process.env.LOG_AUTOMATION_SUMMARY || 'true').toLowerCase() === 'false') {
    return;
  }
  ensureDir(logsDir);
  const d = new Date();
  const ymd = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
  const filePath = path.join(logsDir, `automation-summary-${ymd}.log`);
  const line = `${JSON.stringify({ ts: d.toISOString(), ...entry })}\n`;
  try {
    fs.appendFileSync(filePath, line, 'utf8');
  } catch (_) {
    // ignore disk errors — không làm hỏng luồng chính
  }
}

module.exports = logger;
module.exports.appendAutomationSummary = appendAutomationSummary;
