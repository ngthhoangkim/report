const zlib = require('zlib');
const iconv = require('iconv-lite');

/** RTF \\ansicpgN → iconv-lite */
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
 * Chuỗi RTF sau gunzip: C# GzipHelper chỉ UTF-8, nhưng Aspose.Words parse RTF và áp code page
 * (\\ansicpg1258, \\'hh…). Nếu buffer thực tế là CP1258 / ANSI, utf8.toString sẽ ra U+FFFD / ?
 * (đúng như PDF bạn gửi).
 *
 * Thứ tự:
 * 1) UTF-8 strict + có {\\rtf → dùng (RTF UTF-8 dù header ghi nhầm 1258)
 * 2) Theo \\ansicpg trong đầu file (đọc header qua latin1 để khớp byte)
 * 3) Fallback win1258 nếu RTF_TRY_WIN1258_FALLBACK không tắt
 * 4) utf8 lỏng (cuối cùng)
 *
 * RTF_FORCE_UTF8=true → chỉ bước 4 (giống string hóa C# thuần).
 */
function decodeRtfPayloadBytes(raw) {
  if (!raw || !raw.length) return '';

  if (String(process.env.RTF_FORCE_UTF8 || '').toLowerCase() === 'true') {
    return raw.toString('utf8');
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

  const headLen = Math.min(32768, raw.length);
  const headLatin1 = raw.slice(0, headLen).toString('latin1');
  const cpMatch = /\\ansicpg(\d+)/i.exec(headLatin1);
  if (cpMatch) {
    const cp = Number(cpMatch[1]);
    if (cp === 65001) {
      const s = raw.toString('utf8');
      if (/\{\\rtf/i.test(s)) return s;
    }
    const enc = ANSI_CP_TO_ICONV[cp];
    if (enc && iconv.encodingExists(enc)) {
      const s = iconv.decode(raw, enc);
      if (/\{\\rtf/i.test(s)) return s;
    }
  }

  if (String(process.env.RTF_TRY_WIN1258_FALLBACK || 'true').toLowerCase() !== 'false') {
    if (iconv.encodingExists('win1258')) {
      const v = iconv.decode(raw, 'win1258');
      if (/\{\\rtf/i.test(v)) return v;
    }
  }

  return raw.toString('utf8');
}

/**
 * gunzip rồi decode RTF; nếu không phải gzip thì decode buffer như payload thô.
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
