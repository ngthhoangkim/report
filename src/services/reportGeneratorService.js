const fs = require('fs');
const path = require('path');
const os = require('os');
const crypto = require('crypto');
const logger = require('../utils/logger');
const { getPaths } = require('../config/paths');
const {
  getReportDataListByFileNumAndSessionId,
  getLatestPacsInfoForSession,
} = require('../repositories/reportRepository');
const { fetchPdfBuffer } = require('../utils/fetchPdf');
const { FileCopyHelper } = require('./fileCopyHelper');
const { TemplateSelector } = require('./templateSelector');
const { renderRecordToPdf } = require('./reportDocumentService');
const { collectCnFilesSessionMedia } = require('./cnFilesMediaService');
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

function resolveWriteOutputToDisk(options) {
  if (options.writeOutput === true) return true;
  if (options.writeOutput === false) return false;
  const raw = process.env.REPORT_WRITE_OUTPUT_TO_DISK;
  // Default behavior: DO NOT write PDFs to PATHS_OUTPUT unless explicitly enabled,
  // or when running test scripts via npm lifecycle events.
  //
  // This prevents automation/backfill/API runs from filling disk by default.
  if (raw === undefined || raw === '') {
    const ev = String(process.env.npm_lifecycle_event || '').toLowerCase().trim();
    const nodeEnv = String(process.env.NODE_ENV || '').toLowerCase().trim();
    const isTest =
      nodeEnv === 'test' ||
      ev === 'test' ||
      ev === 'test:report' ||
      ev === 'test:prescription';
    return isTest;
  }
  return String(raw).toLowerCase() !== 'false';
}

/**
 * @param {object} [options]
 * @param {string} [options.resultFileName] — tên nền file kết quả (không .pdf), override tự suy từ DB
 * @param {boolean} [options.writeOutput] — true/false ép ghi hoặc không ghi ra PATHS_OUTPUT; bỏ qua thì dùng REPORT_WRITE_OUTPUT_TO_DISK (mặc định ghi)
 * @param {boolean} [options.returnPdfBuffer] — khi không ghi đĩa: trả về `pdfBuffer` (Buffer) cho upload / tích hợp
 */
async function generatePdfByFileNumAndSessionId(fileNum, sessionId, options = {}) {
  const key = jobKey(fileNum, sessionId);
  return withJobLock(key, async () => {
    const paths = getPaths();
    const localMediaUsed = new Set();
    const trackLocalMediaPath = (p) => {
      if (!p) return;
      try {
        const rp = path.resolve(String(p));
        const localRoot = paths.localImageDir ? path.resolve(String(paths.localImageDir)) : '';
        if (!localRoot) return;
        // Only track files inside PATHS_LOCAL_IMAGE_DIR to avoid deleting source/fallback.
        if (rp === localRoot || !rp.startsWith(localRoot + path.sep)) return;
        localMediaUsed.add(rp);
      } catch (_) {
        // ignore
      }
    };
    const records = await getReportDataListByFileNumAndSessionId(fileNum, sessionId);

    if (!records.length) {
      const err = new Error(
        `No imaging records for FileNum=${fileNum}, SessionId=${sessionId}`,
      );
      err.code = 'NO_RECORDS';
      throw err;
    }

    const pacsRows = await getLatestPacsInfoForSession(fileNum, sessionId);
    const pacsByRequestId = new Map();
    for (const row of pacsRows) {
      pacsByRequestId.set(row.requestId, row);
    }
    for (const r of records) {
      if (r.requestId != null && pacsByRequestId.has(r.requestId)) {
        const p = pacsByRequestId.get(r.requestId);
        r.pacs = {
          viewUrl: p.viewUrl || '',
          fileResultUrl: p.fileResultUrl || '',
          accessCode: p.accessCode || '',
        };
      }
    }

    const mergePacsPdf =
      String(process.env.PACS_MERGE_PDF || 'true').toLowerCase() !== 'false';
    const pacsFetchTimeoutMs = parseInt(
      process.env.PACS_FETCH_TIMEOUT_MS || '45000',
      10,
    );

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

    const cnFilesMediaDir = path.join(tempRoot, '_cn_files_session');
    ensureDirectoryExists(cnFilesMediaDir);
    let cnFilesMediaPaths = [];
    const mergeCnFilesMedia =
      String(process.env.REPORT_MERGE_CN_FILES_MEDIA || 'true').toLowerCase() !== 'false';
    if (mergeCnFilesMedia) {
      try {
        cnFilesMediaPaths = await collectCnFilesSessionMedia(
          fileCopyHelper,
          fileNum,
          sessionId,
          cnFilesMediaDir,
          trackLocalMediaPath,
        );
        if (cnFilesMediaPaths.length > 0) {
          logger.info('CN_FILES session media attached to report job', {
            pathCount: cnFilesMediaPaths.length,
            fileNum,
            sessionId,
          });
        }
      } catch (e) {
        logger.warn(`collectCnFilesSessionMedia failed: ${e.message}`);
      }
    }

    const ctx = {
      fileCopyHelper,
      templateSelector,
      templatesDir: paths.templates,
      cnFilesMediaPaths,
      reportSegmentCount: records.length,
      trackLocalMediaPath,
    };

    const segmentBuffers = [];
    let skipped = 0;
    const pacsMergedFromUrls = [];
    const pacsFetchErrors = [];

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

      const pacsPdfBuffers = [];
      if (mergePacsPdf && pacsRows.length > 0) {
        const seenUrl = new Set();
        for (const row of pacsRows) {
          const u = row.fileResultUrl && String(row.fileResultUrl).trim();
          if (!u || seenUrl.has(u)) continue;
          seenUrl.add(u);
          try {
            const buf = await fetchPdfBuffer(u, {
              timeoutMs: Number.isNaN(pacsFetchTimeoutMs)
                ? 45000
                : pacsFetchTimeoutMs,
            });
            pacsPdfBuffers.push(buf);
            pacsMergedFromUrls.push(u);
          } catch (e) {
            pacsFetchErrors.push({ url: u, message: e.message });
            logger.warn(
              `PACS PDF fetch failed RequestId=${row.requestId}: ${e.message}`,
            );
          }
        }
      }

      const allBuffers = segmentBuffers.concat(pacsPdfBuffers);

      if (!allBuffers.length) {
        const err = new Error(
          `No PDF output for FileNum=${fileNum}, SessionId=${sessionId} (CDHA skipped=${skipped}; PACS URLs tried=${pacsMergedFromUrls.length + pacsFetchErrors.length}).`,
        );
        err.code = 'NO_SEGMENTS';
        throw err;
      }

      const merged = await mergePdfBuffers(allBuffers);
      const writeDisk = resolveWriteOutputToDisk(options);
      let finalPath = null;
      if (writeDisk) {
        ensureDirectoryExists(paths.output);
        finalPath = path.join(paths.output, finalName);

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
      }

      const out = {
        success: true,
        fileNum: String(fileNum),
        sessionId: Number(sessionId),
        resultFileName: pdfBase,
        filePath: finalPath,
        fileName: finalName,
        wroteToDisk: writeDisk,
        segmentCount: segmentBuffers.length,
        skippedRecords: skipped,
        pacsRequestRows: pacsRows.length,
        pacsPdfAppended: pacsPdfBuffers.length,
        pacsMergedUrls: pacsMergedFromUrls,
        pacsFetchErrors: pacsFetchErrors.length ? pacsFetchErrors : undefined,
      };
      if (!writeDisk && options.returnPdfBuffer) {
        out.pdfBuffer = merged;
      }
      return out;
    } finally {
      // Optional: purge local media cache (PATHS_LOCAL_IMAGE_DIR) used by this job.
      if (String(process.env.REPORT_PURGE_LOCAL_MEDIA_CACHE || '').toLowerCase().trim() === 'true') {
        let removed = 0;
        let failed = 0;
        for (const p of localMediaUsed) {
          try {
            if (fs.existsSync(p)) {
              fs.unlinkSync(p);
              removed += 1;
            }
          } catch (_) {
            failed += 1;
          }
        }
        if (removed || failed) {
          logger.info('Purged local media cache for report job', {
            fileNum: String(fileNum),
            sessionId: Number(sessionId),
            removed,
            failed,
            localRoot: paths.localImageDir,
          });
        }
      }
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
  resolveWriteOutputToDisk,
};
