const zlib = require('zlib');

/**
 * Giống MedicalReportServer.Infrastructure.Helpers.GzipHelper.DecompressToRtf:
 * sau khi giải nén GZip, dùng UTF-8 (C#: Encoding.UTF8.GetString).
 */
function decodeRtfPayloadBytes(raw) {
  if (!raw || !raw.length) return '';
  return raw.toString('utf8');
}

/**
 * Decompress gzip buffer to string (UTF-8), hoặc coi buffer là payload thô nếu không phải gzip.
 * Entry tương đương GzipHelper.DecompressToRtf trong MedicalReportServer.
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
