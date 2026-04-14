const path = require('path');
const fs = require('fs');
const { Blob } = require('buffer');

/**
 * Upload một file PDF (buffer) — cùng API với backfill báo cáo CDHA.
 * POST {base}/api/v1/s3/upload-multiple — FormData: prefix, files
 */
async function uploadPdfMultipartBuffer(buf, fileName, baseUrl, prefix) {
  const base = String(baseUrl || '').replace(/\/$/, '');
  if (!base) throw new Error('S3_UPLOAD_API_BASE is empty');
  const url = `${base}/api/v1/s3/upload-multiple`;
  const name = path.basename(fileName || 'report.pdf');
  const body = new FormData();
  body.append('prefix', prefix || 'khambenh/');
  body.append('files', new Blob([buf], { type: 'application/pdf' }), name);

  const res = await fetch(url, { method: 'POST', body });
  const text = await res.text();
  if (!res.ok) {
    throw new Error(`Upload HTTP ${res.status}: ${text.slice(0, 500)}`);
  }
  try {
    return JSON.parse(text);
  } catch {
    return { raw: text };
  }
}

async function uploadPdfMultipartFile(filePath, baseUrl, prefix) {
  const buf = fs.readFileSync(filePath);
  return uploadPdfMultipartBuffer(buf, path.basename(filePath), baseUrl, prefix);
}

module.exports = {
  uploadPdfMultipartBuffer,
  uploadPdfMultipartFile,
};
