const logger = require('../utils/logger');
const { readCheckpoint, writeCheckpoint } = require('./checkpointService');
const { generatePdfByFileNumAndSessionId } = require('./reportGeneratorService');
const {
  getUpdatedSessionsSince,
  getLatestImagingResultWatermark,
} = require('../repositories/reportRepository');
const { runYearRangeBackfill } = require('./sessionBackfillRunner');

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

function parseInitialBackfillYears() {
  const raw = process.env.AUTOMATION_INITIAL_BACKFILL_YEARS;
  if (raw === undefined || raw === '') return 0;
  const n = Number(raw);
  return Number.isFinite(n) && n > 0 ? n : 0;
}

function isDefaultCheckpoint(checkpoint) {
  const id = Number(checkpoint?.lastImagingResultId || 0);
  const at = String(checkpoint?.lastUpdatedAt || '');
  return id === 0 && at.startsWith('1970-01-01');
}

function shouldRunInitialBackfill(years) {
  if (!years || years <= 0) return false;
  if (String(process.env.AUTOMATION_FORCE_BACKFILL || '').toLowerCase() === 'true') {
    return true;
  }
  const { checkpoint } = readCheckpoint();
  return isDefaultCheckpoint(checkpoint);
}

async function runInitialBackfillPhase(years) {
  const to = new Date();
  const from = new Date(to.getFullYear() - years, to.getMonth(), to.getDate());
  const concurrency = Number(process.env.BACKFILL_CONCURRENCY || '2') || 2;
  const delayMs = Number(process.env.BACKFILL_DELAY_MS || '0') || 0;

  logger.info('Automation initial backfill (historical sessions)', {
    years,
    from: from.toISOString(),
    to: to.toISOString(),
    concurrency,
    delayMs,
  });

  const { distinctSessions, monthWindows: mw } = await runYearRangeBackfill(from, to, {
    concurrency,
    delayMs,
    onSession: async (s) => {
      const jobStarted = new Date();
      try {
        const result = await generatePdfByFileNumAndSessionId(s.fileNum, s.sessionId);
        logger.info('Backfill generated PDF', {
          fileNum: s.fileNum,
          sessionId: s.sessionId,
          filePath: result.filePath,
          durationMs: Date.now() - jobStarted.getTime(),
        });
      } catch (e) {
        logger.error('Backfill generate failed', {
          fileNum: s.fileNum,
          sessionId: s.sessionId,
          message: e?.message || String(e),
          reason: e?.code || null,
        });
        throw e;
      }
    },
  });

  logger.info('Automation initial backfill finished', {
    distinctSessions,
    monthWindows: mw,
  });

  const wm = await getLatestImagingResultWatermark();
  if (wm) {
    const lastUpdatedAt = new Date(wm.lastUpdatedAt).toISOString();
    writeCheckpoint({
      lastUpdatedAt,
      lastImagingResultId: wm.lastImagingResultId,
    });
    logger.info('Checkpoint advanced to latest imaging row after backfill', {
      lastUpdatedAt,
      lastImagingResultId: wm.lastImagingResultId,
    });
  } else {
    logger.warn('No imaging rows — checkpoint not updated after backfill');
  }
}

function groupBySession(rows) {
  const map = new Map();
  for (const r of rows) {
    const key = `${r.fileNum}::${r.sessionId}`;
    if (!map.has(key)) map.set(key, { fileNum: r.fileNum, sessionId: r.sessionId, rows: [] });
    map.get(key).rows.push(r);
  }
  return [...map.values()];
}

async function runOnce() {
  const startedAt = new Date();
  const { filePath: checkpointPath, checkpoint } = readCheckpoint();

  const pollLimit = Number(process.env.POLLING_LIMIT || 200);
  const sessions = await getUpdatedSessionsSince(
    checkpoint.lastUpdatedAt,
    checkpoint.lastImagingResultId,
    pollLimit,
  );

  logger.info('Automation tick', {
    startedAt: startedAt.toISOString(),
    checkpointPath,
    checkpoint,
    rowsFetched: sessions.length,
  });

  if (!sessions.length) return;

  const groups = groupBySession(sessions);
  let ok = 0;
  let failed = 0;
  let lastRow = null;

  for (const g of groups) {
    const jobStarted = new Date();
    try {
      // Tên file PDF: mặc định theo FileName dòng mới nhất trong session (reportGeneratorService đã xử lý).
      const result = await generatePdfByFileNumAndSessionId(g.fileNum, g.sessionId);
      ok += 1;
      const durationMs = Date.now() - jobStarted.getTime();
      logger.info('Automation generated PDF', {
        at: jobStarted.toISOString(),
        fileNum: g.fileNum,
        sessionId: g.sessionId,
        resultFileName: result.resultFileName,
        fileName: result.fileName,
        filePath: result.filePath,
        segmentCount: result.segmentCount,
        skippedRecords: result.skippedRecords,
        durationMs,
      });
      logger.appendAutomationSummary({
        jobType: 'cdha',
        status: 'ok',
        fileNum: g.fileNum,
        sessionId: g.sessionId,
        resultFileName: result.resultFileName,
        filePath: result.filePath,
        segmentCount: result.segmentCount,
        skippedRecords: result.skippedRecords,
        durationMs,
      });
    } catch (e) {
      failed += 1;
      const durationMs = Date.now() - jobStarted.getTime();
      logger.error('Automation generate failed', {
        at: new Date().toISOString(),
        fileNum: g.fileNum,
        sessionId: g.sessionId,
        durationMs,
        errorMessage: e?.message || String(e),
        errorStack: e?.stack || null,
        reason: e?.code || null,
      });
      logger.appendAutomationSummary({
        jobType: 'cdha',
        status: 'error',
        fileNum: g.fileNum,
        sessionId: g.sessionId,
        durationMs,
        errorMessage: e?.message || String(e),
      });
    }

    // update checkpoint based on the newest row in this session group
    g.rows.sort((a, b) => {
      const ta = new Date(a.updatedAt).getTime();
      const tb = new Date(b.updatedAt).getTime();
      if (ta !== tb) return ta - tb;
      return a.imagingResultId - b.imagingResultId;
    });
    lastRow = g.rows[g.rows.length - 1];

    // small delay to reduce DB/IO burst
    await sleep(50);
  }

  if (lastRow) {
    const nextCheckpoint = {
      lastUpdatedAt: new Date(lastRow.updatedAt).toISOString(),
      lastImagingResultId: Number(lastRow.imagingResultId),
    };
    writeCheckpoint(nextCheckpoint);
    logger.info('Automation checkpoint updated', {
      at: new Date().toISOString(),
      checkpointPath,
      nextCheckpoint,
      ok,
      failed,
      durationMs: Date.now() - startedAt.getTime(),
    });
  }
}

function startAutomation() {
  const enabled = !parseBool(process.env.DISABLE_AUTOMATION, false);
  if (!enabled) {
    logger.warn('Automation disabled via DISABLE_AUTOMATION=true');
    return { stop() {} };
  }

  const intervalSeconds = Number(process.env.POLLING_INTERVAL_SECONDS || 30);
  const intervalMs = Math.max(5, intervalSeconds) * 1000;
  const backfillYears = parseInitialBackfillYears();
  logger.info('Automation starting', {
    intervalSeconds,
    intervalMs,
    checkpointFile: process.env.CHECKPOINT_FILE || './checkpoint.json',
    initialBackfillYears: backfillYears || null,
    willRunInitialBackfill: shouldRunInitialBackfill(backfillYears),
  });

  let stopped = false;
  let running = false;
  let timer = null;

  const tick = async () => {
    if (stopped) return;
    if (running) return;
    running = true;
    try {
      await runOnce();
    } catch (e) {
      logger.error('Automation tick crashed', {
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
        logger.error('Automation initial poll failed', {
          at: new Date().toISOString(),
          errorMessage: e?.message || String(e),
          errorStack: e?.stack || null,
        });
      });
    }, 1000);
  };

  (async () => {
    if (stopped) return;
    try {
      if (shouldRunInitialBackfill(backfillYears)) {
        await runInitialBackfillPhase(backfillYears);
      } else if (backfillYears > 0) {
        logger.info(
          'AUTOMATION_INITIAL_BACKFILL_YEARS is set but backfill skipped (checkpoint already moved). Set AUTOMATION_FORCE_BACKFILL=true to run again.',
          { years: backfillYears },
        );
      }
    } catch (e) {
      logger.error('Automation initial backfill crashed — polling will still start', {
        errorMessage: e?.message || String(e),
        errorStack: e?.stack || null,
      });
    }
    if (stopped) return;
    beginPolling();
  })();

  return {
    stop() {
      stopped = true;
      if (timer) clearInterval(timer);
      logger.info('Automation stopped');
    },
  };
}

module.exports = { startAutomation, runOnce };

