const path = require('path');
const os = require('os');
const crypto = require('crypto');
const { decompressToString } = require('./gzipHelper');
const { rtfToPlainText } = require('./rtfToPlain');

function normalizeDiagnosisPlain(s) {
  return String(s || '')
    .replace(/\r\n/g, '\n')
    .replace(/[ \t]+\n/g, '\n')
    .replace(/\n[ \t]+/g, '\n')
    .replace(/\n{5,}/g, '\n\n\n\n')
    .trim();
}

/**
 * Giống báo cáo CDHA (buildRenderPayload Diagnosis = record.conclusion):
 * ưu tiên cột Conclusion; rỗng thì gunzip + RTF → plain từ ConclusionData.
 */
function plainDiagnosisFromImagingRow(row) {
  const col = row.Conclusion != null ? String(row.Conclusion).trim() : '';
  if (col) return normalizeDiagnosisPlain(col);
  const raw = row.ConclusionData;
  if (raw == null) return '';
  const buf = Buffer.isBuffer(raw) ? raw : Buffer.from(raw);
  const rtf = decompressToString(buf);
  if (!rtf || !String(rtf).trim()) return '';
  const tmpPrefix = path.join(os.tmpdir(), `presc_dx_${crypto.randomBytes(8).toString('hex')}`);
  return normalizeDiagnosisPlain(rtfToPlainText(rtf, tmpPrefix));
}

/** Nối các kết luận phiên (bỏ trùng nội dung, giữ thứ tự). */
function mergeSessionImagingDiagnoses(rows) {
  const parts = [];
  const seen = new Set();
  for (const row of rows || []) {
    const p = plainDiagnosisFromImagingRow(row);
    if (!p) continue;
    if (seen.has(p)) continue;
    seen.add(p);
    parts.push(p);
  }
  return parts.join('; ');
}

module.exports = {
  plainDiagnosisFromImagingRow,
  mergeSessionImagingDiagnoses,
};
