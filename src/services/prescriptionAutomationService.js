const path = require('path');
const fs = require('fs');
const logger = require('../utils/logger');
const { uploadPdfMultipartBuffer } = require('../utils/s3UploadMultipart');
const {
  readPrescriptionCheckpoint,
  writePrescriptionCheckpoint,
} = require('./prescriptionCheckpointService');
const {
  getUpdatedPrescriptionRowsSince,
} = require('../repositories/prescriptionReportRepository');
const { generatePrescriptionPdf } = require('./prescriptionReportService');
const {
  PRESCRIPTION_CONFIG,
  prescriptionOutputDirAbs,
  normalizePrescriptionS3Prefix,
} = require('../config/prescriptionConstants');

function parseBool(v, def = false) {
  if (v === undefined) return def;
  const s = String(v).toLowerCase().trim();
  if (s === 'true') return true;
  if (s === 'false') return false;
  return def;
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function isDefaultPrescriptionCheckpoint(checkpoint) {
  const id = Number(checkpoint?.lastPrescriptionRowId || 0);
  const at = String(checkpoint?.lastUpdatedAt || '');
  return id === 0 && at.startsWith('1970-01-01');
}

function groupByPrescriptionSession(rows) {
  const map = new Map();
  for (const r of rows) {
    const key = `${r.fileNum}::${r.sessionId}`;
    if (!map.has(key)) map.set(key, { fileNum: r.fileNum, sessionId: r.sessionId, rows: [] });
    map.get(key).rows.push(r);
  }
  return [...map.values()];
}

function rowWatermarkCmp(a, b) {
  const ta = new Date(a.updatedAt).getTime();
  const tb = new Date(b.updatedAt).getTime();
  if (ta !== tb) return ta - tb;
  return a.prescriptionRowId - b.prescriptionRowId;
}

function resolvePrescriptionOutputDir() {
  return prescriptionOutputDirAbs();
}

/**
 * Có S3_UPLOAD_API_BASE → chỉ upload. Không có → ghi output/prescription-auto.
 */
function prescriptionDeliveryMode() {
  const uploadBase = String(process.env.S3_UPLOAD_API_BASE || '').trim();
  const willUpload = Boolean(uploadBase);
  const writeLocal = !willUpload;
  return { uploadBase, willUpload, writeLocal, s3Prefix: normalizePrescriptionS3Prefix() };
}

/**
 * Một vòng polling: log + automation-summary (jobType: prescription).
 */
async function runPrescriptionAutomationOnce() {
  const startedAt = new Date();
  let { filePath: checkpointPath, checkpoint } = readPrescriptionCheckpoint();

  if (isDefaultPrescriptionCheckpoint(checkpoint)) {
    const startIso = String(PRESCRIPTION_CONFIG.HISTORY_START_ISO || '').trim();
    const parsedStart = startIso ? new Date(startIso) : null;
    const hasValidStart = parsedStart && !Number.isNaN(parsedStart.getTime());

    const initialYears = PRESCRIPTION_CONFIG.HISTORY_YEARS;
    const to = new Date();
    const from = hasValidStart
      ? parsedStart
      : new Date(to.getFullYear() - initialYears, to.getMonth(), to.getDate());
    const eps = new Date(from.getTime() - 1000);
    writePrescriptionCheckpoint({
      lastUpdatedAt: eps.toISOString(),
      lastPrescriptionRowId: 0,
    });
    ({ checkpoint } = readPrescriptionCheckpoint());
    logger.info('Prescription automation: checkpoint mốc lịch sử (prescriptionConstants)', {
      checkpointPath,
      historyStartIso: hasValidStart ? from.toISOString() : undefined,
      historyYears: hasValidStart ? undefined : initialYears,
      windowStartInclusive: from.toISOString(),
      checkpointAfterInit: checkpoint,
    });
  }

  const pollLimit = PRESCRIPTION_CONFIG.POLL_BATCH_SIZE;
  const rows = await getUpdatedPrescriptionRowsSince(
    checkpoint.lastUpdatedAt,
    checkpoint.lastPrescriptionRowId,
    pollLimit,
  );

  logger.info('Prescription automation tick', {
    startedAt: startedAt.toISOString(),
    checkpointPath,
    checkpoint,
    rowsFetched: rows.length,
  });

  if (!rows.length) return;

  const groups = groupByPrescriptionSession(rows);
  const { uploadBase, willUpload, writeLocal, s3Prefix } = prescriptionDeliveryMode();
  const outDir = resolvePrescriptionOutputDir();
  const minPdf = PRESCRIPTION_CONFIG.MIN_PDF_BYTES;
  if (writeLocal) {
    fs.mkdirSync(outDir, { recursive: true });
  }

  let ok = 0;
  let failed = 0;
  let bestWatermark = null;

  for (const g of groups) {
    const jobStarted = new Date();
    const sessionIdStr = String(g.sessionId);
    const uploadName = `${sessionIdStr}.pdf`;
    const outPdf = writeLocal ? path.join(outDir, `toa_${g.fileNum}_${g.sessionId}.pdf`) : null;
    try {
      const pdfBuf = await generatePrescriptionPdf(g.fileNum, g.sessionId, {});
      if (!Buffer.isBuffer(pdfBuf) || pdfBuf.length < minPdf) {
        throw new Error(`PDF không hợp lệ hoặc quá nhỏ (< ${minPdf} bytes)`);
      }

      let uploadResponse = null;
      if (willUpload) {
        uploadResponse = await uploadPdfMultipartBuffer(pdfBuf, uploadName, uploadBase, s3Prefix);
      }
      if (outPdf) {
        fs.mkdirSync(path.dirname(outPdf), { recursive: true });
        fs.writeFileSync(outPdf, pdfBuf);
      }

      ok += 1;
      const durationMs = Date.now() - jobStarted.getTime();
      const destLabel = willUpload ? `${s3Prefix}${uploadName}` : outPdf;
      logger.info('Prescription automation delivered PDF', {
        jobType: 'prescription',
        at: jobStarted.toISOString(),
        fileNum: g.fileNum,
        sessionId: g.sessionId,
        uploaded: willUpload,
        s3Prefix: willUpload ? s3Prefix : undefined,
        uploadFileName: willUpload ? uploadName : undefined,
        wroteLocal: Boolean(outPdf),
        localPath: outPdf || undefined,
        uploadResponseKeys:
          uploadResponse && typeof uploadResponse === 'object' && !Array.isArray(uploadResponse)
            ? Object.keys(uploadResponse)
            : null,
        durationMs,
      });
      logger.appendAutomationSummary({
        jobType: 'prescription',
        status: 'ok',
        fileNum: g.fileNum,
        sessionId: g.sessionId,
        filePath: destLabel,
        uploaded: willUpload,
        durationMs,
      });
    } catch (e) {
      failed += 1;
      const durationMs = Date.now() - jobStarted.getTime();
      logger.error('Prescription automation generate failed', {
        jobType: 'prescription',
        at: new Date().toISOString(),
        fileNum: g.fileNum,
        sessionId: g.sessionId,
        durationMs,
        errorMessage: e?.message || String(e),
        errorStack: e?.stack || null,
      });
      logger.appendAutomationSummary({
        jobType: 'prescription',
        status: 'error',
        fileNum: g.fileNum,
        sessionId: g.sessionId,
        durationMs,
        errorMessage: e?.message || String(e),
      });
    }

    for (const r of g.rows) {
      if (!bestWatermark || rowWatermarkCmp(r, bestWatermark) > 0) bestWatermark = r;
    }

    await sleep(50);
  }

  if (bestWatermark) {
    const nextCheckpoint = {
      lastUpdatedAt: new Date(bestWatermark.updatedAt).toISOString(),
      lastPrescriptionRowId: Number(bestWatermark.prescriptionRowId),
    };
    writePrescriptionCheckpoint(nextCheckpoint);
    logger.info('Prescription automation checkpoint updated', {
      at: new Date().toISOString(),
      checkpointPath,
      nextCheckpoint,
      ok,
      failed,
      durationMs: Date.now() - startedAt.getTime(),
    });
  }
}

function startPrescriptionAutomation() {
  const enabled = parseBool(
    process.env.PRESCRIPTION_AUTOMATION_ENABLED,
    PRESCRIPTION_CONFIG.ENABLED_DEFAULT,
  );
  if (!enabled) {
    logger.info('Prescription automation tắt (PRESCRIPTION_AUTOMATION_ENABLED=false)');
    return { stop() {} };
  }

  const intervalSeconds = Math.max(5, PRESCRIPTION_CONFIG.POLL_INTERVAL_SECONDS);
  const intervalMs = intervalSeconds * 1000;

  const mode = prescriptionDeliveryMode();
  logger.info('Prescription automation starting', {
    intervalSeconds,
    historyYears: PRESCRIPTION_CONFIG.HISTORY_YEARS,
    pollBatch: PRESCRIPTION_CONFIG.POLL_BATCH_SIZE,
    checkpointFile: PRESCRIPTION_CONFIG.CHECKPOINT_FILENAME,
    outputDir: resolvePrescriptionOutputDir(),
    s3Upload: mode.willUpload,
    s3Prefix: mode.willUpload ? mode.s3Prefix : undefined,
    writeLocalOutput: mode.writeLocal,
    minPdfBytes: PRESCRIPTION_CONFIG.MIN_PDF_BYTES,
  });

  let stopped = false;
  let running = false;
  let timer = null;

  const tick = async () => {
    if (stopped) return;
    if (running) return;
    running = true;
    try {
      await runPrescriptionAutomationOnce();
    } catch (e) {
      logger.error('Prescription automation tick crashed', {
        at: new Date().toISOString(),
        errorMessage: e?.message || String(e),
        errorStack: e?.stack || null,
      });
    } finally {
      running = false;
    }
  };

  const beginPolling = () => {
    if (stopped) return;
    timer = setInterval(tick, intervalMs);
    setTimeout(() => {
      if (stopped) return;
      tick().catch((e) => {
        logger.error('Prescription automation initial poll failed', {
          at: new Date().toISOString(),
          errorMessage: e?.message || String(e),
        });
      });
    }, 1500);
  };

  beginPolling();

  return {
    stop() {
      stopped = true;
      if (timer) clearInterval(timer);
      logger.info('Prescription automation stopped');
    },
  };
}

module.exports = {
  startPrescriptionAutomation,
  runPrescriptionAutomationOnce,
  resolvePrescriptionOutputDir,
};
