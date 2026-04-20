const fs = require('fs');
const path = require('path');
const logger = require('./logger');

let _aw = null;
let _awLoadError = null;
let _licenseApplied = false;

/**
 * @returns {import('@aspose/words') | null}
 */
function getAsposeWordsOrNull() {
  if (_aw !== null) return _aw;
  if (_awLoadError) return null;
  try {
    // Chỉ có bản Windows x64 trên npm; macOS/Linux sẽ fail require.
    // eslint-disable-next-line import/no-extraneous-dependencies, global-require
    _aw = require('@aspose/words');
    return _aw;
  } catch (e) {
    _awLoadError = e;
    return null;
  }
}

function isAsposeWordsAvailable() {
  return getAsposeWordsOrNull() != null;
}

function isAsposeWordsEnabled() {
  const raw =
    process.env.ASPOSE_WORDS_ENABLED || process.env.USE_ASPOSE_WORDS || '';
  return String(raw).toLowerCase().trim() === 'true';
}

function applyLicenseOnce(aw) {
  if (_licenseApplied) return;
  const candidates = [
    process.env.ASPOSE_WORDS_LICENSE_PATH,
    process.env.ASPOSE_LICENSE_PATH,
  ].filter(Boolean);

  const lic = new aw.License();
  for (const p of candidates) {
    const abs = path.resolve(p);
    if (!fs.existsSync(abs)) continue;
    try {
      lic.setLicense(abs);
      _licenseApplied = true;
      logger.info('Aspose.Words license applied', { path: abs });
      return;
    } catch (e) {
      logger.warn(`Aspose.Words setLicense failed for ${abs}: ${e.message}`);
    }
  }

  _licenseApplied = true;
  logger.warn(
    'Aspose.Words: no valid license file (ASPOSE_WORDS_LICENSE_PATH / ASPOSE_LICENSE_PATH); evaluation limits may apply',
  );
}

function getAsposeWordsWithLicenseOrThrow() {
  const aw = getAsposeWordsOrNull();
  if (!aw) {
    throw new Error(
      'Không load được @aspose/words (package chỉ hỗ trợ Windows x64). Cài trên server Windows hoặc tắt ASPOSE_WORDS_ENABLED.',
    );
  }
  applyLicenseOnce(aw);
  return aw;
}

function ensureDir(dir) {
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
}

/**
 * Chuyển file Word (doc/docx/dot/rtf/...) sang .docx hoặc .pdf bằng Aspose.Words.
 * Dùng cùng quy ước tên output với LibreOffice: `<basename của input>.<ext>` trong outDir.
 *
 * @param {'docx'|'pdf'} mode
 * @param {string} inputPath
 * @param {string} outDir
 */
function convertWithAsposeWords(mode, inputPath, outDir) {
  const aw = getAsposeWordsWithLicenseOrThrow();

  const m = String(mode).toLowerCase().trim();
  if (!['pdf', 'docx'].includes(m)) {
    throw new Error(`Aspose.Words: unsupported mode: ${mode}`);
  }

  ensureDir(outDir);
  const absIn = path.resolve(inputPath);
  if (!fs.existsSync(absIn)) {
    throw new Error(`Aspose.Words: input not found: ${absIn}`);
  }
  const st = fs.statSync(absIn);
  if (!st.isFile() || st.size <= 0) {
    throw new Error(`Aspose.Words: input empty/invalid: ${absIn}`);
  }

  const base = path.parse(absIn).name;
  const outPath = path.join(path.resolve(outDir), `${base}.${m}`);
  const saveFormat = m === 'pdf' ? aw.SaveFormat.Pdf : aw.SaveFormat.Docx;

  const doc = new aw.Document(absIn);
  doc.save(outPath, saveFormat);

  if (!fs.existsSync(outPath)) {
    throw new Error(`Aspose.Words produced no output: ${outPath}`);
  }
}

/**
 * Mail merge / chèn dữ liệu: Aspose hỗ trợ MERGEFIELD trong template Word.
 * Luồng hiện tại của repo dùng docxtemplater với <<Field>> — không trùng MERGEFIELD.
 * Nếu sau này chuyển template sang MERGEFIELD, có thể dùng Document.mailMerge ở đây.
 * Tham khảo: https://docs.aspose.com/words/nodejs-net/mail-merge-and-reporting/
 */

module.exports = {
  getAsposeWordsOrNull,
  getAsposeWordsWithLicenseOrThrow,
  isAsposeWordsAvailable,
  isAsposeWordsEnabled,
  convertWithAsposeWords,
};
