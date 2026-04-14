require('dotenv').config();
const fs = require('fs');
const db = require('../src/config/database');
const { getDistinctSessionsCreatedBetween } = require('../src/repositories/reportRepository');
const {
  generatePdfByFileNumAndSessionId,
  resolveWriteOutputToDisk,
} = require('../src/services/reportGeneratorService');
const logger = require('../src/utils/logger');
const { uploadPdfMultipartBuffer, uploadPdfMultipartFile } = require('../src/utils/s3UploadMultipart');

function parseEnvDays() {
  const raw = process.env.BACKFILL_DAYS;
  if (raw === undefined || raw === '') return null;
  const n = Number(raw);
  return Number.isFinite(n) && n > 0 ? n : null;
}

function parseArgs(argv) {
  const out = {
    years: 3,
    yearsCli: false,
    daysCli: null,
    daysEnv: parseEnvDays(),
    dryRun: false,
    upload: false,
    limit: 0,
    concurrency: Number(process.env.BACKFILL_CONCURRENCY || '2') || 2,
    delayMs: Number(process.env.BACKFILL_DELAY_MS || '0') || 0,
    from: null,
    to: null,
  };
  for (let i = 2; i < argv.length; i += 1) {
    const a = argv[i];
    if (a === '--dry-run') out.dryRun = true;
    else if (a === '--upload') out.upload = true;
    else if (a === '--no-upload') out.upload = false;
    else if (a === '--years' && argv[i + 1]) {
      out.yearsCli = true;
      out.years = Number(argv[++i]);
    } else if (a === '--days' && argv[i + 1]) {
      out.daysCli = Number(argv[++i]);
    } else if (a === '--limit' && argv[i + 1]) {
      out.limit = Number(argv[++i]);
    } else if (a === '--from' && argv[i + 1]) {
      out.from = new Date(argv[++i]);
    } else if (a === '--to' && argv[i + 1]) {
      out.to = new Date(argv[++i]);
    } else if (a === '--concurrency' && argv[i + 1]) {
      out.concurrency = Math.max(1, Number(argv[++i]) || 1);
    } else if (a === '--delay-ms' && argv[i + 1]) {
      out.delayMs = Math.max(0, Number(argv[++i]) || 0);
    }
  }
  return out;
}

/**
 * Chia [from, to) thành các đoạn không vượt qua ranh giới tháng — mỗi đoạn query DB một lần.
 * Luôn bắt đầu đúng từ `from` (không kéo về ngày 1 tháng).
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

  const workers = Array.from({ length: Math.min(concurrency, items.length) }, () => worker());
  await Promise.all(workers);
  return errors;
}

function resolveBackfillRange(args) {
  const to = args.to && !Number.isNaN(args.to.getTime()) ? args.to : new Date();

  if (args.from && !Number.isNaN(args.from.getTime())) {
    return { from: args.from, to, mode: 'explicit-from' };
  }

  // Ưu tiên: --days (CLI) > --years (CLI) > BACKFILL_DAYS (env) > mặc định theo năm lịch
  if (args.daysCli != null && Number.isFinite(args.daysCli) && args.daysCli > 0) {
    const from = new Date(to.getTime() - args.daysCli * 24 * 60 * 60 * 1000);
    return { from, to, mode: 'days', days: args.daysCli };
  }

  if (args.yearsCli) {
    const from = new Date(to.getFullYear() - args.years, to.getMonth(), to.getDate());
    return { from, to, mode: 'years', years: args.years };
  }

  if (args.daysEnv != null && Number.isFinite(args.daysEnv) && args.daysEnv > 0) {
    const from = new Date(to.getTime() - args.daysEnv * 24 * 60 * 60 * 1000);
    return { from, to, mode: 'days', days: args.daysEnv };
  }

  const from = new Date(to.getFullYear() - args.years, to.getMonth(), to.getDate());
  return { from, to, mode: 'years', years: args.years };
}

async function main() {
  const args = parseArgs(process.argv);
  const { from, to, mode, days, years } = resolveBackfillRange(args);

  const uploadBase = process.env.S3_UPLOAD_API_BASE || '';
  const prefix = process.env.S3_UPLOAD_PREFIX || 'khambenh/';

  if (args.limit > 0) {
    args.concurrency = 1;
  }

  logger.info('Backfill start', {
    from: from.toISOString(),
    to: to.toISOString(),
    rangeMode: mode,
    days: days ?? null,
    years: mode === 'years' ? years ?? args.years : null,
    dryRun: args.dryRun,
    upload: args.upload,
    limit: args.limit || null,
    concurrency: args.concurrency,
    uploadBase: uploadBase || '(not set)',
    prefix,
  });

  if (args.upload && !uploadBase) {
    logger.error('Set S3_UPLOAD_API_BASE in .env when using --upload');
    process.exitCode = 1;
    return;
  }

  await db.initializePool();

  const seen = new Set();
  let generated = 0;
  let uploaded = 0;
  let skippedDup = 0;
  let stoppedByLimit = false;

  try {
    for (const win of monthWindows(from, to)) {
      if (stoppedByLimit) break;
      const sessions = await getDistinctSessionsCreatedBetween(win.from, win.to);
      logger.info('Month window', {
        from: win.from.toISOString(),
        to: win.to.toISOString(),
        sessions: sessions.length,
      });

      const todo = [];
      for (const s of sessions) {
        const key = `${s.fileNum}::${s.sessionId}`;
        if (seen.has(key)) {
          skippedDup += 1;
          continue;
        }
        seen.add(key);
        todo.push(s);
      }

      const runOne = async (s) => {
        if (args.limit > 0 && generated >= args.limit) {
          stoppedByLimit = true;
          return;
        }
        if (args.dryRun) {
          logger.info('DRY-RUN would generate', {
            fileNum: s.fileNum,
            sessionId: s.sessionId,
          });
          generated += 1;
          return;
        }

        const writesDisk = resolveWriteOutputToDisk({});
        const result = await generatePdfByFileNumAndSessionId(s.fileNum, s.sessionId, {
          returnPdfBuffer: args.upload && !writesDisk,
        });
        generated += 1;
        logger.info('Generated', {
          fileNum: s.fileNum,
          sessionId: s.sessionId,
          filePath: result.filePath,
          wroteToDisk: result.wroteToDisk,
        });

        if (args.upload && result.pdfBuffer) {
          const up = await uploadPdfMultipartBuffer(
            result.pdfBuffer,
            result.fileName,
            uploadBase,
            prefix,
          );
          uploaded += 1;
          logger.info('Uploaded', {
            fileNum: s.fileNum,
            sessionId: s.sessionId,
            fileName: result.fileName,
            responseKeys: Array.isArray(up) ? up.length : Object.keys(up || {}),
          });
        } else if (args.upload && result.filePath && fs.existsSync(result.filePath)) {
          const up = await uploadPdfMultipartFile(result.filePath, uploadBase, prefix);
          uploaded += 1;
          logger.info('Uploaded', {
            fileNum: s.fileNum,
            sessionId: s.sessionId,
            fileName: result.fileName,
            responseKeys: Array.isArray(up) ? up.length : Object.keys(up || {}),
          });
        }

        if (args.delayMs > 0) {
          await new Promise((r) => setTimeout(r, args.delayMs));
        }
      };

      const errors = await runPool(todo, args.concurrency, runOne);
      for (const e of errors) {
        logger.error('Backfill item failed', {
          fileNum: e.item?.fileNum,
          sessionId: e.item?.sessionId,
          message: e.error,
        });
      }
    }
  } finally {
    await db.closePool();
  }

  logger.info('Backfill done', {
    distinctSessions: seen.size,
    skippedDup,
    generated,
    uploaded,
    stoppedByLimit: args.limit > 0 && generated >= args.limit,
  });
}

main().catch((e) => {
  logger.error(e?.message || String(e));
  process.exitCode = 1;
});
