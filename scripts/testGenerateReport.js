/**
 * Test end-to-end report generation (DB -> DOCX -> PDF output).
 *
 * Usage:
 *   node scripts/testGenerateReport.js --fileNum=15084356 --sessionId=799593
 *   node scripts/testGenerateReport.js 15084356 799593
 *
 * Notes:
 * - Uses the same .env DB config as the app.
 * - Does NOT start the HTTP server.
 * - Automation is not started here.
 */
require('dotenv').config();

const db = require('../src/config/database');
const logger = require('../src/utils/logger');
const { generatePdfByFileNumAndSessionId } = require('../src/services/reportGeneratorService');

function parseArgs(argv) {
  const out = {};
  for (const a of argv) {
    if (a.startsWith('--fileNum=')) out.fileNum = a.slice('--fileNum='.length);
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
      'Usage:',
      '  node scripts/testGenerateReport.js --fileNum=15084356 --sessionId=799593',
      '  node scripts/testGenerateReport.js 15084356 799593',
      '',
      'Optional:',
      '  --resultFileName=custom_name_without_pdf',
    ].join('\n'),
  );
  process.exit(code);
}

async function main() {
  const startedAt = Date.now();
  const { fileNum, sessionId, resultFileName } = parseArgs(process.argv.slice(2));
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

