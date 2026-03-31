const fs = require('fs');
const path = require('path');
const logger = require('../utils/logger');

function resolveCheckpointPath() {
  const p = process.env.CHECKPOINT_FILE || './checkpoint.json';
  return path.isAbsolute(p) ? p : path.resolve(process.cwd(), p);
}

function defaultCheckpoint() {
  return {
    lastUpdatedAt: '1970-01-01T00:00:00.000Z',
    lastImagingResultId: 0,
  };
}

function readCheckpoint() {
  const filePath = resolveCheckpointPath();
  try {
    if (!fs.existsSync(filePath)) return { filePath, checkpoint: defaultCheckpoint() };
    const raw = fs.readFileSync(filePath, 'utf8');
    const parsed = JSON.parse(raw);
    const cp = {
      lastUpdatedAt: parsed.lastUpdatedAt || defaultCheckpoint().lastUpdatedAt,
      lastImagingResultId: Number(parsed.lastImagingResultId || 0),
    };
    return { filePath, checkpoint: cp };
  } catch (e) {
    logger.warn(`Checkpoint read failed, using default. reason=${e.message}`, { filePath });
    return { filePath, checkpoint: defaultCheckpoint() };
  }
}

function writeCheckpoint(checkpoint) {
  const filePath = resolveCheckpointPath();
  const dir = path.dirname(filePath);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(
    filePath,
    JSON.stringify(
      {
        lastUpdatedAt: checkpoint.lastUpdatedAt,
        lastImagingResultId: Number(checkpoint.lastImagingResultId || 0),
      },
      null,
      2,
    ),
  );
  return filePath;
}

module.exports = { readCheckpoint, writeCheckpoint, resolveCheckpointPath };

