const rtfToHTML = require('@iarna/rtf-to-html');

function nowMs() {
  return Number(process.hrtime.bigint() / 1000000n);
}

function timingEnabled() {
  return String(process.env.REPORT_TIMING || '').toLowerCase().trim() === 'true';
}

/**
 * Convert RTF string to HTML locally (no Office, no API).
 * Note: conversion fidelity depends on RTF features used.
 *
 * @param {string} rtf
 * @returns {Promise<{ html: string, durationMs?: number }>}
 */
function rtfToHtmlLocal(rtf) {
  const t0 = timingEnabled() ? nowMs() : 0;
  return new Promise((resolve, reject) => {
    rtfToHTML.fromString(String(rtf || ''), (err, html) => {
      if (err) return reject(err);
      if (!t0) return resolve({ html: String(html || '') });
      return resolve({ html: String(html || ''), durationMs: nowMs() - t0 });
    });
  });
}

/**
 * Very small HTML -> text helper (keeps paragraph-ish breaks).
 * Good enough for "test flow" without Docxtemplater HTML module.
 */
function htmlToTextLoose(html) {
  let s = String(html || '');
  if (!s) return '';
  s = s
    .replace(/\r\n/g, '\n')
    .replace(/<\s*br\s*\/?>/gi, '\n')
    .replace(/<\/\s*p\s*>/gi, '\n')
    .replace(/<\/\s*div\s*>/gi, '\n')
    .replace(/<\/\s*li\s*>/gi, '\n')
    .replace(/<\s*li\b[^>]*>/gi, '• ')
    .replace(/<\s*\/?\s*(p|div|ul|ol|span|b|strong|i|em|u)\b[^>]*>/gi, '')
    .replace(/&nbsp;/gi, ' ')
    .replace(/&amp;/gi, '&')
    .replace(/&lt;/gi, '<')
    .replace(/&gt;/gi, '>')
    .replace(/&quot;/gi, '"')
    .replace(/&#39;/gi, "'")
    .replace(/[ \t]+\n/g, '\n')
    .replace(/\n{4,}/g, '\n\n\n')
    .trim();
  return s;
}

module.exports = { rtfToHtmlLocal, htmlToTextLoose };

