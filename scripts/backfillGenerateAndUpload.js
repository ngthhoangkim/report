/**
 * Backfill: tạo PDF theo từng (FileNum, SessionId) trong khoảng thời gian, tùy chọn upload S3 qua API.
 *
 * Chạy theo TỪNG THÁNG (giảm tải DB), lọc trùng FileNum+SessionId trong cả job.
 *
 * Usage:
 *   node scripts/backfillGenerateAndUpload.js --years 3 --dry-run
 *   node scripts/backfillGenerateAndUpload.js --years 3 --no-upload
 *   node scripts/backfillGenerateAndUpload.js --years 3 --upload
 *
 * Env:
 *   S3_UPLOAD_API_BASE — ví dụ https://dsbiy10xl4.execute-api.ap-southeast-1.amazonaws.com
 *   S3_UPLOAD_PREFIX   — mặc định khambenh/
 *   BACKFILL_CONCURRENCY — số phiên song song (mặc định 2)
 *   BACKFILL_DELAY_MS    — nghỉ giữa mỗi phiên (mặc định 0)
 *
 * "Push" ở đây = upload multipart lên API (không phải git push).
 */
require('dotenv').config();
const fs = require('fs');
const path = require('path');
const { Blob } = require('buffer');
const db = require('../src/config/database');
const { getDistinctSessionsCreatedBetween } = require('../src/repositories/reportRepository');
const { generatePdfByFileNumAndSessionId } = require('../src/services/reportGeneratorService');
const logger = require('../src/utils/logger');

function parseArgs(argv) {
  const out = {
    years: 3,
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
      out.years = Number(argv[++i]);
    } else if (a === '--limit' && argv[i + 1]) {
      out.limit = Number(argv[++i]);
    } else if (a === '--from' && argv[i + 1]) {
      out.from = new Date(argv[++i]);
    } else if (a === '--to' && argv[i + 1]) {
      out.to = new Date(argv[++i]);
    } else if (a === '--concurrency' && argv[i + 1]) {
      out.concurrency = Math.max(1, Number(argv[++i]) || 1);
    }
  }
  return out;
}

function* monthWindows(from, to) {
  let d = new Date(from.getFullYear(), from.getMonth(), 1);
  const end = new Date(to.getTime());
  while (d < end) {
    const next = new Date(d.getFullYear(), d.getMonth() + 1, 1);
    const sliceTo = next > end ? end : next;
    yield { from: new Date(d.getTime()), to: sliceTo };
    d = next;
  }
}

async function uploadPdfMultipart(filePath, baseUrl, prefix) {
  const base = String(baseUrl || '').replace(/\/$/, '');
  if (!base) throw new Error('S3_UPLOAD_API_BASE is empty');
  const url = `${base}/api/v1/s3/upload-multiple`;
  const buf = fs.readFileSync(filePath);
  const name = path.basename(filePath);
  const body = new FormData();
  body.append('prefix', prefix || 'khambenh/');
  body.append('files', new Blob([buf], { type: 'application/pdf' }), name);

  const res = await fetch(url, { method: 'POST', body });
  const text = await res.text();
  if (!res.ok) {
    throw new Error(`Upload HTTP ${res.status}: ${text.slice(0, 500)}`);
  }
  try {
    return JSON.parse(text);
  } catch {
    return { raw: text };
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

async function main() {
  const args = parseArgs(process.argv);
  const to = args.to && !Number.isNaN(args.to.getTime()) ? args.to : new Date();
  const from =
    args.from && !Number.isNaN(args.from.getTime())
      ? args.from
      : new Date(to.getFullYear() - args.years, to.getMonth(), to.getDate());

  const uploadBase = process.env.S3_UPLOAD_API_BASE || '';
  const prefix = process.env.S3_UPLOAD_PREFIX || 'khambenh/';

  if (args.limit > 0) {
    args.concurrency = 1;
  }

  logger.info('Backfill start', {
    from: from.toISOString(),
    to: to.toISOString(),
    years: args.years,
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

        const result = await generatePdfByFileNumAndSessionId(s.fileNum, s.sessionId);
        generated += 1;
        logger.info('Generated', {
          fileNum: s.fileNum,
          sessionId: s.sessionId,
          filePath: result.filePath,
        });

        if (args.upload && result.filePath && fs.existsSync(result.filePath)) {
          const up = await uploadPdfMultipart(result.filePath, uploadBase, prefix);
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
