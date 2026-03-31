const logger = require('../utils/logger');
const { readCheckpoint, writeCheckpoint } = require('./checkpointService');
const { generatePdfByFileNumAndSessionId } = require('./reportGeneratorService');
const { getUpdatedSessionsSince } = require('../repositories/reportRepository');

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
      logger.info('Automation generated PDF', {
        at: jobStarted.toISOString(),
        fileNum: g.fileNum,
        sessionId: g.sessionId,
        resultFileName: result.resultFileName,
        fileName: result.fileName,
        filePath: result.filePath,
        segmentCount: result.segmentCount,
        skippedRecords: result.skippedRecords,
        durationMs: Date.now() - jobStarted.getTime(),
      });
    } catch (e) {
      failed += 1;
      logger.error('Automation generate failed', {
        at: new Date().toISOString(),
        fileNum: g.fileNum,
        sessionId: g.sessionId,
        durationMs: Date.now() - jobStarted.getTime(),
        errorMessage: e?.message || String(e),
        errorStack: e?.stack || null,
        reason: e?.code || null,
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
  logger.info('Automation starting', {
    intervalSeconds,
    intervalMs,
    checkpointFile: process.env.CHECKPOINT_FILE || './checkpoint.json',
  });

  let stopped = false;
  let running = false;

  const timer = setInterval(async () => {
    if (stopped) return;
    if (running) return; // prevent overlap
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
  }, intervalMs);

  // run immediately on startup
  setTimeout(() => {
    if (stopped) return;
    runOnce().catch((e) => {
      logger.error('Automation initial run failed', {
        at: new Date().toISOString(),
        errorMessage: e?.message || String(e),
        errorStack: e?.stack || null,
      });
    });
  }, 1000);

  return {
    stop() {
      stopped = true;
      clearInterval(timer);
      logger.info('Automation stopped');
    },
  };
}

module.exports = { startAutomation, runOnce };

