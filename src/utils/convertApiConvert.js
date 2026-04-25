const fs = require('fs');
const path = require('path');

function isConvertApiEnabled() {
  return String(process.env.CONVERTAPI_ENABLED || '').toLowerCase().trim() === 'true';
}

function getConvertApiSecretOrNull() {
  const s = process.env.CONVERTAPI_SECRET || process.env.CONVERT_API_SECRET || '';
  const v = String(s).trim();
  return v ? v : null;
}

function baseUrl() {
  return String(process.env.CONVERTAPI_BASE_URL || 'https://v2.convertapi.com')
    .trim()
    .replace(/\/+$/, '');
}

function timeoutMs() {
  const raw = parseInt(process.env.CONVERTAPI_TIMEOUT_MS || '180000', 10);
  if (!Number.isFinite(raw) || raw <= 0) return 180000;
  return Math.max(10_000, Math.min(900_000, raw));
}

function extNoDot(p) {
  const e = path.extname(String(p || '')).toLowerCase();
  return e.startsWith('.') ? e.slice(1) : e;
}

async function fetchJsonWithTimeout(url, init) {
  const ctl = new AbortController();
  const t = setTimeout(() => ctl.abort(), timeoutMs());
  try {
    const r = await fetch(url, { ...init, signal: ctl.signal });
    const text = await r.text();
    if (!r.ok) throw new Error(`ConvertAPI HTTP ${r.status}: ${text.slice(0, 600)}`);
    try {
      return JSON.parse(text);
    } catch {
      throw new Error(`ConvertAPI invalid JSON: ${text.slice(0, 600)}`);
    }
  } finally {
    clearTimeout(t);
  }
}

async function uploadFileToConvertApi(absInputPath, secret) {
  const url = `${baseUrl()}/upload?Secret=${encodeURIComponent(secret)}`;
  const bytes = fs.readFileSync(absInputPath);
  const filename = path.basename(absInputPath);
  const r = await fetchJsonWithTimeout(url, {
    method: 'POST',
    headers: {
      'Content-Disposition': `inline; filename="${filename.replace(/"/g, '')}"`,
      'Content-Type': 'application/octet-stream',
    },
    body: bytes,
  });
  const fileId = r && (r.FileId || r.fileId);
  if (!fileId) throw new Error(`ConvertAPI upload failed: missing FileId`);
  return String(fileId);
}

async function deleteUploadedFile(fileId, secret) {
  if (!fileId) return;
  const url = `${baseUrl()}/d/${encodeURIComponent(fileId)}?Secret=${encodeURIComponent(secret)}`;
  try {
    const ctl = new AbortController();
    const t = setTimeout(() => ctl.abort(), Math.min(30_000, timeoutMs()));
    try {
      await fetch(url, { method: 'DELETE', signal: ctl.signal });
    } finally {
      clearTimeout(t);
    }
  } catch {
    // best-effort
  }
}

async function convertUploadedFile(fileId, fromExt, toExt, secret) {
  const url =
    `${baseUrl()}/convert/${encodeURIComponent(fromExt)}/to/${encodeURIComponent(toExt)}` +
    `?Secret=${encodeURIComponent(secret)}` +
    `&File=${encodeURIComponent(fileId)}` +
    `&StoreFile=false`;

  const r = await fetchJsonWithTimeout(url, { method: 'GET' });
  const files = r && r.Files;
  if (!Array.isArray(files) || files.length === 0) throw new Error('ConvertAPI convert failed: missing Files[]');
  const fileData = files[0] && files[0].FileData;
  if (!fileData) throw new Error('ConvertAPI convert failed: missing FileData');
  return Buffer.from(String(fileData), 'base64');
}

async function convertWithConvertApi(mode, inputPath, outDir) {
  const secret = getConvertApiSecretOrNull();
  if (!isConvertApiEnabled() || !secret) throw new Error('ConvertAPI disabled or missing CONVERTAPI_SECRET');

  const m = String(mode).toLowerCase().trim();
  if (!['pdf', 'docx'].includes(m)) throw new Error(`ConvertAPI: unsupported mode: ${mode}`);

  const absIn = path.resolve(inputPath);
  const fromExt = extNoDot(absIn);
  if (!fromExt) throw new Error(`ConvertAPI: cannot infer input extension: ${absIn}`);

  if (!fs.existsSync(absIn)) throw new Error(`ConvertAPI: input not found: ${absIn}`);
  const st = fs.statSync(absIn);
  if (!st.isFile() || st.size <= 0) throw new Error(`ConvertAPI: input empty/invalid: ${absIn} size=${st.size}`);

  fs.mkdirSync(outDir, { recursive: true });
  const base = path.parse(absIn).name;
  const outPath = path.join(path.resolve(outDir), `${base}.${m}`);

  if (fromExt === m) {
    fs.copyFileSync(absIn, outPath);
    return;
  }

  let fileId = null;
  try {
    fileId = await uploadFileToConvertApi(absIn, secret);
    const outBuf = await convertUploadedFile(fileId, fromExt, m, secret);
    fs.writeFileSync(outPath, outBuf);
  } finally {
    await deleteUploadedFile(fileId, secret);
  }
}

module.exports = { isConvertApiEnabled, getConvertApiSecretOrNull, convertWithConvertApi };

