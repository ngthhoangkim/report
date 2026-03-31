const zlib = require('zlib');

/**
 * Decompress gzip buffer to UTF-8 string (RTF). Same idea as C# GzipHelper.DecompressToRtf.
 */
function decompressToString(buffer) {
  if (!buffer) return null;
  const buf = Buffer.isBuffer(buffer) ? buffer : Buffer.from(buffer);
  if (!buf.length) return null;
  try {
    return zlib.gunzipSync(buf).toString('utf8');
  } catch (_) {
    return buf.toString('utf8');
  }
}

module.exports = { decompressToString };
