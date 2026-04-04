const logger = require('../utils/logger');
const { getDistinctSessionsCreatedBetween } = require('../repositories/reportRepository');

/**
 * Chia [from, to) thành các đoạn theo tháng (giống scripts/backfillGenerateAndUpload.js).
 */
function* monthWindows(from, to) {
  const end = new Date(to.getTime());
  if (!(from < end)) return;
  let cur = new Date(from.getTime());
  while (cur < end) {
    const nextMonth = new Date(cur.getFullYear(), cur.getMonth() + 1, 1);
    const sliceEnd = nextMonth < end ? nextMonth : end;
    yield { from: new Date(cur.getTime()), to: sliceEnd };
    cur = sliceEnd;
  }
}

async function runPool(items, concurrency, fn) {
  let idx = 0;
  const errors = [];

  async function worker() {
    for (;;) {
      const my = idx;
      idx += 1;
      if (my >= items.length) return;
      try {
        await fn(items[my], my);
      } catch (e) {
        errors.push({ item: items[my], error: e?.message || String(e) });
      }
    }
  }

  const workers = Array.from({ length: Math.min(concurrency, items.length || 1) }, () =>
    worker(),
  );
  await Promise.all(workers);
  return errors;
}

/**
 * Duyệt mọi (FileNum, SessionId) distinct có CN_ImagingResult.CreatedDate trong [from, to).
 * @param {{ concurrency: number, delayMs: number, onSession: (s: {fileNum:string,sessionId:number}) => Promise<void> }} opts
 */
async function runYearRangeBackfill(from, to, opts) {
  const concurrency = Math.max(1, Number(opts.concurrency) || 2);
  const delayMs = Math.max(0, Number(opts.delayMs) || 0);
  const onSession = opts.onSession;
  const seen = new Set();
  let windows = 0;

  for (const win of monthWindows(from, to)) {
    windows += 1;
    const sessions = await getDistinctSessionsCreatedBetween(win.from, win.to);
    const todo = [];
    for (const s of sessions) {
      const key = `${s.fileNum}::${s.sessionId}`;
      if (seen.has(key)) continue;
      seen.add(key);
      todo.push(s);
    }

    logger.info('Session backfill window', {
      from: win.from.toISOString(),
      to: win.to.toISOString(),
      sessionsInWindow: todo.length,
    });

    const errors = await runPool(todo, concurrency, async (s) => {
      await onSession(s);
      if (delayMs > 0) {
        await new Promise((r) => setTimeout(r, delayMs));
      }
    });
    for (const e of errors) {
      logger.error('Backfill session failed', {
        fileNum: e.item?.fileNum,
        sessionId: e.item?.sessionId,
        message: e.error,
      });
    }
  }

  return { distinctSessions: seen.size, monthWindows: windows };
}

module.exports = { monthWindows, runYearRangeBackfill, runPool };
