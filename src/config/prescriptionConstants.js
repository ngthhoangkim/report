const path = require('path');

/**
 * Worker toa thuốc: chỉnh trực tiếp tại đây (không cần dàn PRESCRIPTION_* trong .env).
 * .env vẫn cần DB_* và, nếu upload, S3_UPLOAD_API_BASE.
 */
const PRESCRIPTION_CONFIG = {
  ENABLED_DEFAULT: true,
  // Backfill/poll bắt đầu từ mốc này (inclusive) nếu checkpoint còn mặc định.
  // Đặt theo ISO để ổn định theo ngày, không phụ thuộc "hôm nay - N năm".
  HISTORY_START_ISO: '2023-01-01T00:00:00.000Z',
  // Fallback nếu HISTORY_START_ISO không hợp lệ.
  HISTORY_YEARS: 3,
  POLL_INTERVAL_SECONDS: 30,
  POLL_BATCH_SIZE: 200,
  S3_PREFIX: 'khambenh/toathuoc/',
  MIN_PDF_BYTES: 500,
  CHECKPOINT_FILENAME: 'checkpoint-prescription.json',
  OUTPUT_DIR_SEGMENTS: ['output', 'prescription-auto'],
};

function prescriptionCheckpointPathAbs() {
  return path.resolve(process.cwd(), PRESCRIPTION_CONFIG.CHECKPOINT_FILENAME);
}

function prescriptionOutputDirAbs() {
  return path.resolve(process.cwd(), ...PRESCRIPTION_CONFIG.OUTPUT_DIR_SEGMENTS);
}

function normalizePrescriptionS3Prefix() {
  const p = String(PRESCRIPTION_CONFIG.S3_PREFIX).trim();
  return p.endsWith('/') ? p : `${p}/`;
}

module.exports = {
  PRESCRIPTION_CONFIG,
  prescriptionCheckpointPathAbs,
  prescriptionOutputDirAbs,
  normalizePrescriptionS3Prefix,
};
