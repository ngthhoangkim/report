const fs = require('fs');
const { execFileSync } = require('child_process');

function fallbackRtfToPlainText(rtfText) {
  return (rtfText || '')
    .replace(/\\par[d]?/g, '\n')
    .replace(/\\tab/g, ' ')
    .replace(/\\'[0-9a-fA-F]{2}/g, '')
    .replace(/\\[a-zA-Z]+-?\d* ?/g, '')
    .replace(/[{}]/g, '')
    .replace(/\n{3,}/g, '\n\n')
    .trim();
}

/**
 * RTF → plain text (macOS textutil if available; else regex fallback).
 */
function rtfToPlainText(rtfText, tmpPrefix) {
  if (!rtfText) return '';
  const tmpRtfPath = `${tmpPrefix}.tmp.rtf`;
  fs.writeFileSync(tmpRtfPath, rtfText, 'utf8');
  try {
    const txt = execFileSync('textutil', ['-convert', 'txt', '-stdout', tmpRtfPath], {
      encoding: 'utf8',
    });
    return (txt || '').trim();
  } catch (_) {
    return fallbackRtfToPlainText(rtfText);
  } finally {
    if (fs.existsSync(tmpRtfPath)) {
      try {
        fs.unlinkSync(tmpRtfPath);
      } catch (_) {
        /* ignore */
      }
    }
  }
}

module.exports = { rtfToPlainText };
