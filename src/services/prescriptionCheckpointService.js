const fs = require('fs');
const path = require('path');
const logger = require('../utils/logger');
const { prescriptionCheckpointPathAbs } = require('../config/prescriptionConstants');

function defaultCheckpoint() {
  return {
    lastUpdatedAt: '1970-01-01T00:00:00.000Z',
    lastPrescriptionRowId: 0,
  };
}

function readPrescriptionCheckpoint() {
  const filePath = prescriptionCheckpointPathAbs();
  try {
    if (!fs.existsSync(filePath)) return { filePath, checkpoint: defaultCheckpoint() };
    const raw = fs.readFileSync(filePath, 'utf8');
    const parsed = JSON.parse(raw);
    const cp = {
      lastUpdatedAt: parsed.lastUpdatedAt || defaultCheckpoint().lastUpdatedAt,
      lastPrescriptionRowId: Number(parsed.lastPrescriptionRowId || 0),
    };
    return { filePath, checkpoint: cp };
  } catch (e) {
    logger.warn(`Prescription checkpoint read failed, using default. reason=${e.message}`, {
      filePath,
    });
    return { filePath, checkpoint: defaultCheckpoint() };
  }
}

function writePrescriptionCheckpoint(checkpoint) {
  const filePath = prescriptionCheckpointPathAbs();
  const dir = path.dirname(filePath);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(
    filePath,
    JSON.stringify(
      {
        lastUpdatedAt: checkpoint.lastUpdatedAt,
        lastPrescriptionRowId: Number(checkpoint.lastPrescriptionRowId || 0),
      },
      null,
      2,
    ),
  );
  return filePath;
}

module.exports = {
  readPrescriptionCheckpoint,
  writePrescriptionCheckpoint,
  resolvePath: prescriptionCheckpointPathAbs,
};
