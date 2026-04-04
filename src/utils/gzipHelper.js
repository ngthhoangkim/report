const zlib = require('zlib');
const iconv = require('iconv-lite');

/** RTF \ansicpgN → iconv-lite encoding name */
const ANSI_CP_TO_ICONV = {
  1258: 'win1258',
  1252: 'win1252',
  1250: 'win1250',
  1251: 'win1251',
  936: 'cp936',
  950: 'cp950',
  437: 'cp437',
};

/**
 * Giải nén gzip (hoặc buffer thô) rồi decode chuỗi RTF đúng bảng mã.
 * Nhiều RTF từ hệ VN dùng Windows-1258; gunzip + toString('utf8') sẽ làm hỏng tiếng Việt
 * và Word/LibreOffice xuất PDF ra ô / ?.
 */
function decodeRtfPayloadBytes(raw) {
  if (!raw || !raw.length) return '';

  if (String(process.env.RTF_FORCE_UTF8 || '').toLowerCase() === 'true') {
    return raw.toString('utf8');
  }

  const headLen = Math.min(32768, raw.length);
  const headLatin1 = raw.slice(0, headLen).toString('latin1');
  const cpMatch = /\\ansicpg(\d+)/i.exec(headLatin1);
  if (cpMatch) {
    const enc = ANSI_CP_TO_ICONV[Number(cpMatch[1])];
    if (enc && iconv.encodingExists(enc)) {
      const s = iconv.decode(raw, enc);
      if (/\{\\rtf/i.test(s)) {
        return s;
      }
    }
  }

  let strictUtf8 = null;
  try {
    strictUtf8 = new TextDecoder('utf-8', { fatal: true }).decode(raw);
  } catch {
    strictUtf8 = null;
  }
  if (strictUtf8 != null && /\{\\rtf/i.test(strictUtf8)) {
    return strictUtf8;
  }

  if (String(process.env.RTF_TRY_WIN1258_FALLBACK || 'true').toLowerCase() !== 'false') {
    if (iconv.encodingExists('win1258')) {
      const v = iconv.decode(raw, 'win1258');
      if (/\{\\rtf/i.test(v)) {
        return v;
      }
    }
  }

  return raw.toString('utf8');
}

/**
 * Decompress gzip buffer to RTF string (encoding-aware).
 * Same entry as trước; C# tương đương GzipHelper.DecompressToRtf.
 */
function decompressToString(buffer) {
  if (!buffer) return null;
  const buf = Buffer.isBuffer(buffer) ? buffer : Buffer.from(buffer);
  if (!buf.length) return null;
  try {
    const raw = zlib.gunzipSync(buf);
    return decodeRtfPayloadBytes(raw);
  } catch (_) {
    return decodeRtfPayloadBytes(buf);
  }
}

module.exports = { decompressToString, decodeRtfPayloadBytes };
