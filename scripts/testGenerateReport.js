/**
 * Test end-to-end report generation (DB -> DOCX -> PDF output) với FileNum + SessionId cụ thể.
 *
 * Usage:
 *   npm run test:report -- 26003528 844466
 *   npm run test:report -- --fileNum=26003528 --sessionId=844466
 *   node scripts/testGenerateReport.js 26003528 844466
 *   node scripts/testGenerateReport.js --fileNum=15084356 --sessionId=799593
 *
 * Optional:
 *   npm run test:report -- 26003528 844466 --resultFileName=my_debug_name
 *
 * Notes:
 * - Cần `.env` kết nối SQL Server giống app.
 * - ZIP từ dbo.CN_FILES có mật khẩu (PAP/ECG): đặt `CN_FILES_ZIP_PASSWORD` trong `.env` để bung ảnh khi `REPORT_MERGE_CN_FILES_MEDIA` bật (mặc định bật).
 * - Không bật HTTP server / automation.
 * - Luôn ghi PDF ra PATHS_OUTPUT (writeOutput: true), kể cả khi .env có REPORT_WRITE_OUTPUT_TO_DISK=false.
 * - `npm test` chỉ chạy unit test; test theo hồ sơ thật dùng `npm run test:report -- ...`.
 *
 * Ví dụ đã dùng khi debug (đổi sang đúng hồ sơ trên DB của bạn):
 *   npm run test:report -- 26003528 844466
 */
require('dotenv').config();

const db = require('../src/config/database');
const logger = require('../src/utils/logger');
const { generatePdfByFileNumAndSessionId } = require('../src/services/reportGeneratorService');

function parseArgs(argv) {
  const out = {};
  for (const a of argv) {
    if (a === '--help' || a === '-h') out.help = true;
    else if (a.startsWith('--fileNum=')) out.fileNum = a.slice('--fileNum='.length);
    else if (a.startsWith('--filenum=')) out.fileNum = a.slice('--filenum='.length);
    else if (a.startsWith('--sessionId=')) out.sessionId = a.slice('--sessionId='.length);
    else if (a.startsWith('--sessionid=')) out.sessionId = a.slice('--sessionid='.length);
    else if (a.startsWith('--resultFileName=')) out.resultFileName = a.slice('--resultFileName='.length);
    else if (a.startsWith('--resultfilename=')) out.resultFileName = a.slice('--resultfilename='.length);
    else if (!a.startsWith('--') && out.fileNum == null) out.fileNum = a;
    else if (!a.startsWith('--') && out.sessionId == null) out.sessionId = a;
  }
  return out;
}

function usageAndExit(code = 1) {
  // eslint-disable-next-line no-console
  console.log(
    [
      'Generate PDF từ DB theo FileNum + SessionId (test thủ công / debug).',
      '',
      'Usage:',
      '  npm run test:report -- <fileNum> <sessionId>',
      '  npm run test:report -- --fileNum=26003528 --sessionId=844466',
      '  node scripts/testGenerateReport.js 26003528 844466',
      '',
      'Optional:',
      '  --resultFileName=custom_name_without_pdf',
      '',
      '.env: CN_FILES_ZIP_PASSWORD=... (ZIP CN_FILES ZipCrypto); ví dụ phiên: 26003528 844466',
      '',
      'Unit test (không cần DB): npm test',
    ].join('\n'),
  );
  process.exit(code);
}

async function main() {
  const startedAt = Date.now();
  const parsed = parseArgs(process.argv.slice(2));
  if (parsed.help) usageAndExit(0);
  const { fileNum, sessionId, resultFileName } = parsed;
  if (!fileNum || sessionId == null || String(sessionId).trim() === '') usageAndExit(1);

  const sid = Number(sessionId);
  if (Number.isNaN(sid)) {
    logger.error('Invalid sessionId (must be a number)', { sessionId });
    usageAndExit(1);
  }

  logger.info('Generate test starting', {
    fileNum: String(fileNum),
    sessionId: sid,
    resultFileName: resultFileName || null,
  });

  try {
    await db.initializePool();

    const result = await generatePdfByFileNumAndSessionId(String(fileNum), sid, {
      resultFileName: resultFileName || undefined,
      writeOutput: true,
    });

    logger.info('Generate test OK', {
      durationMs: Date.now() - startedAt,
      result,
    });

    // eslint-disable-next-line no-console
    console.log(JSON.stringify(result, null, 2));
    process.exitCode = 0;
  } catch (e) {
    logger.error('Generate test FAILED', {
      durationMs: Date.now() - startedAt,
      errorMessage: e?.message || String(e),
      errorStack: e?.stack || null,
      reason: e?.code || null,
    });
    process.exitCode = 1;
  } finally {
    try {
      await db.closePool();
    } catch (_) {
      // ignore
    }
  }
}

main();

