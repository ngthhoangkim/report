const winston = require('winston');
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

const logger = winston.createLogger({
  level: 'info',
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.printf(({ timestamp, level, message, ...meta }) => {
      return `${timestamp} [${level.toUpperCase()}]: ${message} ${Object.keys(meta).length ? JSON.stringify(meta, null, 2) : ''}`;
    })
  ),
  transports: [
    new winston.transports.Console({
      format: winston.format.combine(
        winston.format.colorize(),
        winston.format.printf(({ timestamp, level, message, ...meta }) => {
            return `${timestamp} [${level}]: ${message} ${Object.keys(meta).length ? JSON.stringify(meta, null, 2) : ''}`;
        })
      ),
    }),
    new winston.transports.File({
      filename: path.join(logsDir, 'automation.log'),
      level: 'info',
    }),
    new winston.transports.File({
      filename: path.join(logsDir, 'automation.error.log'),
      level: 'warn',
    }),
  ],
});

module.exports = logger;
