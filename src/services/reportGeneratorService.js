const fs = require('fs');
const path = require('path');
const os = require('os');
const crypto = require('crypto');
const logger = require('../utils/logger');
const { getPaths } = require('../config/paths');
const { getReportDataListByFileNumAndSessionId } = require('../repositories/reportRepository');
const { FileCopyHelper } = require('./fileCopyHelper');
const { TemplateSelector } = require('./templateSelector');
const { renderRecordToPdf } = require('./reportDocumentService');
const { mergePdfBuffers } = require('../utils/pdfMerge');
const { ensureDirectoryExists, cleanupDirectory } = require('../utils/fileHelper');

/**
 * Tên file PDF (không .pdf): ưu tiên request; không thì CN_ImagingResult.FileName
 * của dòng mới nhất trong phiên (sau ORDER BY CreatedDate — thường là file kết quả cuối).
 */
function sanitizePdfBase(name) {
  let s = String(name).replace(/[/\\?%*:|"<>]/g, '_').trim();
  if (/\.pdf$/i.test(s)) s = s.slice(0, -4);
  return s || 'report';
}

function resolveOutputPdfBaseName(records, fileNum, sessionId, explicitOverride) {
  if (explicitOverride != null && String(explicitOverride).trim() !== '') {
    return sanitizePdfBase(String(explicitOverride).trim());
  }
  for (let i = records.length - 1; i >= 0; i--) {
    const fn = records[i].fileName;
    if (fn && String(fn).trim()) {
      return sanitizePdfBase(String(fn).trim());
    }
  }
  return sanitizePdfBase(`${fileNum}_${sessionId}`);
}

const jobChains = new Map();

function jobKey(fileNum, sessionId) {
  return `${String(fileNum)}::${Number(sessionId)}`;
}

async function withJobLock(key, fn) {
  const prev = jobChains.get(key) || Promise.resolve();
  const next = prev.then(() => fn());
  jobChains.set(key, next.catch(() => {}));
  return next;
}

/**
 * @param {object} [options]
 * @param {string} [options.resultFileName] — tên nền file kết quả (không .pdf), override tự suy từ DB
 */
async function generatePdfByFileNumAndSessionId(fileNum, sessionId, options = {}) {
  const key = jobKey(fileNum, sessionId);
  return withJobLock(key, async () => {
    const paths = getPaths();
    const records = await getReportDataListByFileNumAndSessionId(fileNum, sessionId);

    if (!records.length) {
      const err = new Error(
        `No imaging records for FileNum=${fileNum}, SessionId=${sessionId}`,
      );
      err.code = 'NO_RECORDS';
      throw err;
    }

    const pdfBase = resolveOutputPdfBaseName(
      records,
      fileNum,
      sessionId,
      options.resultFileName,
    );
    const finalName = `${pdfBase}.pdf`;

    // Reuse helpers to avoid re-creating local cache directories every run.
    const fileCopyHelper = getOrCreateFileCopyHelper(paths);
    const templateSelector = getOrCreateTemplateSelector(paths);

    const tempRoot = path.join(
      os.tmpdir(),
      `report_${sanitizePdfBase(fileNum)}_${sessionId}_${crypto.randomBytes(6).toString('hex')}`,
    );
    ensureDirectoryExists(tempRoot);

    const ctx = {
      fileCopyHelper,
      templateSelector,
      templatesDir: paths.templates,
    };

    const segmentBuffers = [];
    let skipped = 0;

    try {
      for (let idx = 0; idx < records.length; idx++) {
        const record = records[idx];
        try {
          const pdfBuf = await renderRecordToPdf(record, idx, tempRoot, ctx);
          if (!pdfBuf) {
            skipped += 1;
            continue;
          }
          segmentBuffers.push(pdfBuf);
        } catch (e) {
          logger.error(
            `Failed segment FileNum=${fileNum} SessionId=${sessionId} index=${idx} ImagingResultId=${record.imagingResultId}: ${e.message}`,
          );
          throw e;
        }
      }

      if (!segmentBuffers.length) {
        const err = new Error(
          `No documents generated for FileNum=${fileNum}, SessionId=${sessionId} (skipped=${skipped}).`,
        );
        err.code = 'NO_SEGMENTS';
        throw err;
      }

      const merged = await mergePdfBuffers(segmentBuffers);
      ensureDirectoryExists(paths.output);
      const finalPath = path.join(paths.output, finalName);

      if (fs.existsSync(finalPath)) {
        try {
          fs.unlinkSync(finalPath);
        } catch (e) {
          logger.warn(`Retry delete ${finalPath}: ${e.message}`);
          await new Promise((r) => setTimeout(r, 100));
          if (fs.existsSync(finalPath)) fs.unlinkSync(finalPath);
        }
      }

      fs.writeFileSync(finalPath, merged);

      return {
        success: true,
        fileNum: String(fileNum),
        sessionId: Number(sessionId),
        resultFileName: pdfBase,
        filePath: finalPath,
        fileName: finalName,
        segmentCount: segmentBuffers.length,
        skippedRecords: skipped,
      };
    } finally {
      cleanupDirectory(tempRoot);
    }
  });
}

let _fileCopyHelper = null;
let _fileCopyHelperKey = null;
function getOrCreateFileCopyHelper(paths) {
  const key = `${paths.sourceImageDir}::${paths.fallbackImageDir}::${paths.localImageDir}`;
  if (_fileCopyHelper && _fileCopyHelperKey === key) return _fileCopyHelper;
  _fileCopyHelperKey = key;
  _fileCopyHelper = new FileCopyHelper(
    paths.sourceImageDir,
    paths.fallbackImageDir,
    paths.localImageDir,
  );
  logger.info('Initialized FileCopyHelper', { key });
  return _fileCopyHelper;
}

let _templateSelector = null;
let _templateSelectorKey = null;
function getOrCreateTemplateSelector(paths) {
  const key = `${paths.templates}`;
  if (_templateSelector && _templateSelectorKey === key) return _templateSelector;
  _templateSelectorKey = key;
  _templateSelector = new TemplateSelector(paths.templates);
  logger.info('Initialized TemplateSelector', { key });
  return _templateSelector;
}

module.exports = {
  generatePdfByFileNumAndSessionId,
  resolveOutputPdfBaseName,
};
