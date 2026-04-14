require('dotenv').config();

const fs = require('fs');
const path = require('path');
const db = require('../src/config/database');
const logger = require('../src/utils/logger');
const { uploadPdfMultipartBuffer } = require('../src/utils/s3UploadMultipart');
const { generatePrescriptionPdf } = require('../src/services/prescriptionReportService');
const { getDistinctPrescriptionSessionsCreatedBetween } = require('../src/repositories/prescriptionReportRepository');
const {
  PRESCRIPTION_CONFIG,
  prescriptionOutputDirAbs,
  normalizePrescriptionS3Prefix,
} = require('../src/config/prescriptionConstants');

function usageAndExit(code = 1) {
  // eslint-disable-next-line no-console
  console.log(
    [
      'Backfill toa thuốc theo khoảng thời gian (ViewRX.CreatedDate).',
      '',
      'Usage:',
      '  npm run backfill:prescription -- --from 2023-01-01 --to 2024-01-01',
      '',
      'Options:',
      '  --from <iso-or-date>   (required) vd: 2023-01-01 hoặc 2023-01-01T00:00:00.000Z',
      '  --to <iso-or-date>     (required) vd: 2024-01-01',
      '  --upload               upload S3 (cần S3_UPLOAD_API_BASE trong .env)',
      '  --dry-run              chỉ log, không generate/upload',
      '  --limit <n>            giới hạn số session (để test) → tự set concurrency=1',
      '  --concurrency <n>       mặc định 2',
      '  --delay-ms <ms>         delay giữa mỗi session (mặc định 0)',
      '  -h, --help',
      '',
      'Output local (nếu không --upload và không set S3_UPLOAD_API_BASE):',
      `  ${path.relative(process.cwd(), prescriptionOutputDirAbs())}`,
    ].join('\n'),
  );
  process.exit(code);
}

function parseArgs(argv) {
  const out = {
    from: null,
    to: null,
    dryRun: false,
    upload: false,
    limit: 0,
    concurrency: Number(process.env.BACKFILL_CONCURRENCY || '2') || 2,
    delayMs: Number(process.env.BACKFILL_DELAY_MS || '0') || 0,
  };
  for (let i = 2; i < argv.length; i += 1) {
    const a = argv[i];
    if (a === '--help' || a === '-h') out.help = true;
    else if (a === '--dry-run') out.dryRun = true;
    else if (a === '--upload') out.upload = true;
    else if (a === '--no-upload') out.upload = false;
    else if (a === '--limit' && argv[i + 1]) out.limit = Number(argv[++i]) || 0;
    else if (a === '--from' && argv[i + 1]) out.from = new Date(argv[++i]);
    else if (a === '--to' && argv[i + 1]) out.to = new Date(argv[++i]);
    else if (a === '--concurrency' && argv[i + 1]) out.concurrency = Math.max(1, Number(argv[++i]) || 1);
    else if (a === '--delay-ms' && argv[i + 1]) out.delayMs = Math.max(0, Number(argv[++i]) || 0);
  }
  return out;
}

function assertValidDate(d, label) {
  if (!(d instanceof Date) || Number.isNaN(d.getTime())) {
    throw new Error(`${label} không hợp lệ`);
  }
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

function resolveDeliveryMode({ uploadFlag }) {
  const uploadBase = String(process.env.S3_UPLOAD_API_BASE || '').trim();
  const willUpload = Boolean(uploadFlag) || Boolean(uploadBase);
  const writeLocal = !willUpload;
  return { uploadBase, willUpload, writeLocal, s3Prefix: normalizePrescriptionS3Prefix() };
}

async function main() {
  const args = parseArgs(process.argv);
  if (args.help) usageAndExit(0);

  assertValidDate(args.from, '--from');
  assertValidDate(args.to, '--to');
  if (!(args.from < args.to)) {
    throw new Error('--from phải nhỏ hơn --to');
  }

  if (args.limit > 0) args.concurrency = 1;

  const startedAt = new Date();
  const { uploadBase, willUpload, writeLocal, s3Prefix } = resolveDeliveryMode({ uploadFlag: args.upload });
  const outDir = prescriptionOutputDirAbs();
  const minPdf = PRESCRIPTION_CONFIG.MIN_PDF_BYTES;

  logger.info('Prescription backfill start', {
    from: args.from.toISOString(),
    to: args.to.toISOString(),
    dryRun: args.dryRun,
    upload: willUpload,
    uploadBase: uploadBase || '(not set)',
    s3Prefix: willUpload ? s3Prefix : undefined,
    writeLocal,
    outDir,
    limit: args.limit || null,
    concurrency: args.concurrency,
    delayMs: args.delayMs,
    pollBatch: PRESCRIPTION_CONFIG.POLL_BATCH_SIZE,
  });

  if (willUpload && !uploadBase) {
    throw new Error('Thiếu S3_UPLOAD_API_BASE trong .env (cần cho upload)');
  }

  if (writeLocal) fs.mkdirSync(outDir, { recursive: true });

  await db.initializePool();
  let totalSessions = 0;
  let ok = 0;
  let failed = 0;
  let stoppedByLimit = false;

  try {
    for (const win of monthWindows(args.from, args.to)) {
      if (stoppedByLimit) break;
      const sessions = await getDistinctPrescriptionSessionsCreatedBetween(win.from, win.to);
      logger.info('Prescription backfill month window', {
        from: win.from.toISOString(),
        to: win.to.toISOString(),
        sessions: sessions.length,
      });

      const todo = [];
      for (const s of sessions) {
        if (args.limit > 0 && totalSessions >= args.limit) {
          stoppedByLimit = true;
          break;
        }
        todo.push(s);
        totalSessions += 1;
      }

      const runOne = async (s) => {
        const jobStarted = new Date();
        const uploadName = `${String(s.sessionId)}.pdf`;
        const outPdf = writeLocal ? path.join(outDir, `toa_${s.fileNum}_${s.sessionId}.pdf`) : null;

        if (args.dryRun) {
          logger.info('DRY-RUN would generate prescription PDF', {
            fileNum: s.fileNum,
            sessionId: s.sessionId,
          });
          ok += 1;
          return;
        }

        const pdfBuf = await generatePrescriptionPdf(s.fileNum, s.sessionId, {});
        if (!Buffer.isBuffer(pdfBuf) || pdfBuf.length < minPdf) {
          throw new Error(`PDF không hợp lệ hoặc quá nhỏ (< ${minPdf} bytes)`);
        }

        if (willUpload) {
          await uploadPdfMultipartBuffer(pdfBuf, uploadName, uploadBase, s3Prefix);
        }
        if (outPdf) {
          fs.mkdirSync(path.dirname(outPdf), { recursive: true });
          fs.writeFileSync(outPdf, pdfBuf);
        }

        ok += 1;
        const durationMs = Date.now() - jobStarted.getTime();
        logger.info('Prescription backfill delivered PDF', {
          fileNum: s.fileNum,
          sessionId: s.sessionId,
          uploaded: willUpload,
          s3Prefix: willUpload ? s3Prefix : undefined,
          uploadFileName: willUpload ? uploadName : undefined,
          wroteLocal: Boolean(outPdf),
          localPath: outPdf || undefined,
          durationMs,
        });

        if (args.delayMs > 0) {
          await new Promise((r) => setTimeout(r, args.delayMs));
        }
      };

      const errors = await runPool(todo, args.concurrency, runOne);
      failed += errors.length;
      for (const e of errors) {
        logger.error('Prescription backfill item failed', {
          fileNum: e.item?.fileNum,
          sessionId: e.item?.sessionId,
          message: e.error,
        });
      }
    }
  } finally {
    await db.closePool();
  }

  logger.info('Prescription backfill done', {
    from: args.from.toISOString(),
    to: args.to.toISOString(),
    totalSessions,
    ok,
    failed,
    stoppedByLimit,
    durationMs: Date.now() - startedAt.getTime(),
  });
}

main().catch((e) => {
  logger.error(e?.message || String(e));
  process.exitCode = 1;
});

