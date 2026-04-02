const logger = require('./logger');

function isPrivateOrLocalHostname(hostname) {
  const h = String(hostname).toLowerCase();
  if (h === 'localhost' || h.endsWith('.localhost')) return true;
  if (h === '0.0.0.0') return true;
  const m = /^(\d+)\.(\d+)\.(\d+)\.(\d+)$/.exec(h);
  if (m) {
    const a = Number(m[1]);
    const b = Number(m[2]);
    if (a === 10) return true;
    if (a === 127) return true;
    if (a === 169 && b === 254) return true;
    if (a === 172 && b >= 16 && b <= 31) return true;
    if (a === 192 && b === 168) return true;
  }
  return false;
}

/**
 * Tải URL về Buffer, chỉ http(s). Có timeout.
 * @param {string} url
 * @param {{ timeoutMs?: number, maxBytes?: number }} [opts]
 */
async function fetchUrlBuffer(url, opts = {}) {
  const timeoutMs = opts.timeoutMs ?? 45000;
  const maxBytes = opts.maxBytes ?? 80 * 1024 * 1024;

  let parsed;
  try {
    parsed = new URL(String(url).trim());
  } catch {
    throw new Error('Invalid URL');
  }
  if (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') {
    throw new Error(`Unsupported URL scheme: ${parsed.protocol}`);
  }

  if (process.env.PACS_BLOCK_PRIVATE_URL === 'true' && isPrivateOrLocalHostname(parsed.hostname)) {
    throw new Error('URL host blocked by PACS_BLOCK_PRIVATE_URL');
  }

  const ac = new AbortController();
  const t = setTimeout(() => ac.abort(), timeoutMs);
  try {
    const res = await fetch(parsed.toString(), {
      signal: ac.signal,
      redirect: 'follow',
      headers: { Accept: 'application/pdf,*/*' },
    });
    if (!res.ok) {
      throw new Error(`HTTP ${res.status}`);
    }
    const lenHeader = res.headers.get('content-length');
    if (lenHeader && Number(lenHeader) > maxBytes) {
      throw new Error(`Response too large (${lenHeader} bytes)`);
    }
    const buf = Buffer.from(await res.arrayBuffer());
    if (buf.length > maxBytes) {
      throw new Error(`Response too large (${buf.length} bytes)`);
    }
    return buf;
  } finally {
    clearTimeout(t);
  }
}

function looksLikePdf(buf) {
  if (!buf || buf.length < 5) return false;
  return buf[0] === 0x25 && buf[1] === 0x50 && buf[2] === 0x44 && buf[3] === 0x46;
}

/**
 * @param {string} url
 * @param {{ timeoutMs?: number }} [opts]
 * @returns {Promise<Buffer>}
 */
async function fetchPdfBuffer(url, opts = {}) {
  const buf = await fetchUrlBuffer(url, opts);
  if (!looksLikePdf(buf)) {
    logger.warn('PACS URL did not return PDF magic bytes', { url: String(url).slice(0, 120) });
    throw new Error('Not a PDF response');
  }
  return buf;
}

module.exports = { fetchUrlBuffer, fetchPdfBuffer, looksLikePdf };
