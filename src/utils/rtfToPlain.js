const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { execFileSync } = require('child_process');
const { getLibreOfficeBinary } = require('./libreOffice');

function findBalancedBraceEnd(s, openIdx) {
  if (!s || openIdx < 0 || s[openIdx] !== '{') return -1;
  let depth = 0;
  for (let i = openIdx; i < s.length; i += 1) {
    const c = s[i];
    if (c === '{') depth += 1;
    else if (c === '}') {
      depth -= 1;
      if (depth === 0) return i;
    }
  }
  return -1;
}

/**
 * Bỏ các nhóm RTF hay chứa font table / stylesheet (gây lộ chuỗi Calibri;Times... khi strip kém).
 */
function stripRtfHeaderGroups(rtf) {
  let s = rtf;
  const triggers = [
    '\\fonttbl',
    '\\colortbl',
    '\\stylesheet',
    '\\filetbl',
    '\\listtable',
    '\\listoverridetable',
    '\\rsidtbl',
    '\\generator',
    '\\info',
    '\\datastore',
  ];
  for (let guard = 0; guard < 200; guard += 1) {
    let bestPos = -1;
    let bestStart = -1;
    for (const kw of triggers) {
      const p = s.indexOf(kw);
      if (p < 0) continue;
      let start = p;
      while (start > 0 && s[start] !== '{') start -= 1;
      if (s[start] !== '{') continue;
      if (bestPos < 0 || p < bestPos) {
        bestPos = p;
        bestStart = start;
      }
    }
    if (bestStart < 0) break;
    const end = findBalancedBraceEnd(s, bestStart);
    if (end < 0) break;
    s = `${s.slice(0, bestStart)} ${s.slice(end + 1)}`;
  }
  return s;
}

/**
 * RTF \'hh = 1 byte (thường ANSI 1258/1252 trong hệ thống VN) — tốt hơn là xóa byte.
 */
function decodeRtfHexEscapes(rtf) {
  return String(rtf).replace(/\\'([0-9a-fA-F]{2})/g, (_, h) =>
    String.fromCharCode(parseInt(h, 16)),
  );
}

function fallbackRtfToPlainText(rtfText) {
  let s = stripRtfHeaderGroups(String(rtfText || ''));
  s = decodeRtfHexEscapes(s);
  s = s
    .replace(/\r\n/g, '\n')
    .replace(/\\par[d]?\s*/gi, '\n')
    .replace(/\\line\s*/gi, '\n')
    .replace(/\\tab/g, ' ')
    .replace(/\\u(-?\d+)\s*\?/g, (_, n) => {
      const code = Number(n);
      if (!Number.isFinite(code)) return '';
      try {
        return String.fromCodePoint(code < 0 ? 65536 + code : code);
      } catch {
        return '';
      }
    })
    .replace(/\\[a-zA-Z]+-?\d*\s?/g, '')
    .replace(/[{}]/g, '')
    .replace(/[ \t]+\n/g, '\n')
    .replace(/\n{4,}/g, '\n\n\n')
    .trim();
  return s;
}

function rtfToPlainTextTextutil(rtfText, tmpPrefix) {
  if (process.platform !== 'darwin') return null;
  const tmpRtfPath = `${tmpPrefix}.tmp.rtf`;
  fs.writeFileSync(tmpRtfPath, rtfText, 'utf8');
  try {
    const txt = execFileSync('textutil', ['-convert', 'txt', '-stdout', tmpRtfPath], {
      encoding: 'utf8',
      maxBuffer: 50 * 1024 * 1024,
    });
    return (txt || '').trim();
  } catch {
    return null;
  } finally {
    try {
      if (fs.existsSync(tmpRtfPath)) fs.unlinkSync(tmpRtfPath);
    } catch {
      /* ignore */
    }
  }
}

function rtfToPlainTextLibreOffice(rtfText, tmpPrefix) {
  if (String(process.env.RTF_PLAIN_USE_LIBREOFFICE || 'true').toLowerCase() === 'false') {
    return null;
  }
  const parent = path.dirname(tmpPrefix);
  const dir = path.join(parent, `_rtf_txt_${crypto.randomBytes(6).toString('hex')}`);
  fs.mkdirSync(dir, { recursive: true });
  const base = 'rtfcontent';
  const rtfPath = path.join(dir, `${base}.rtf`);
  try {
    fs.writeFileSync(rtfPath, rtfText, 'utf8');
    const bin = getLibreOfficeBinary();
    execFileSync(bin, ['--headless', '--convert-to', 'txt:Text', '--outdir', dir, rtfPath], {
      maxBuffer: 50 * 1024 * 1024,
    });
    const txtPath = path.join(dir, `${base}.txt`);
    if (fs.existsSync(txtPath)) {
      const t = fs.readFileSync(txtPath, 'utf8');
      return t.trim();
    }
  } catch {
    return null;
  } finally {
    try {
      fs.rmSync(dir, { recursive: true, force: true });
    } catch {
      /* ignore */
    }
  }
  return null;
}

/**
 * RTF → plain (UTF-8) cho fallback khi inject RTF vào docx thất bại.
 * Thứ tự: macOS textutil → LibreOffice txt → stripper cải tiến (Windows/Linux không có textutil).
 */
function rtfToPlainText(rtfText, tmpPrefix) {
  if (!rtfText) return '';

  const tu = rtfToPlainTextTextutil(rtfText, tmpPrefix);
  if (tu != null && tu.length > 0) return tu;

  const lo = rtfToPlainTextLibreOffice(rtfText, tmpPrefix);
  if (lo != null && lo.length > 0) return lo;

  return fallbackRtfToPlainText(rtfText);
}

module.exports = { rtfToPlainText, fallbackRtfToPlainText, stripRtfHeaderGroups };
