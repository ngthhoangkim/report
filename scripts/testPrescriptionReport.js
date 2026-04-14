require('dotenv').config();

const path = require('path');
const fs = require('fs');
const db = require('../src/config/database');
const logger = require('../src/utils/logger');
const { generatePrescriptionPdf } = require('../src/services/prescriptionReportService');
const {
  resolveFileNumSessionIdForPrescription,
} = require('../src/repositories/prescriptionReportRepository');
const {
  parsePrescriptionTestArgs,
  getPrescriptionCliArgv,
} = require('../src/utils/prescriptionReportTestArgs');
const { uploadPdfMultipartBuffer } = require('../src/utils/s3UploadMultipart');
const { PRESCRIPTION_CONFIG, normalizePrescriptionS3Prefix } = require('../src/config/prescriptionConstants');

function usageAndExit(code = 1) {
  // eslint-disable-next-line no-console
  console.log(
    [
      'PDF đơn thuốc (ViewRX + mẫu Templates/ToaThuoc).',
      '',
      'Usage (cần ít nhất fileNum HOẶC sessionId):',
      '  npm run test:prescription -- <fileNum>',
      '  npm run test:prescription -- --sessionId=<id>',
      '  npm run test:prescription -- <fileNum> <sessionId>',
      '  npm run test:prescription -- --fileNum=... --sessionId=...',
      '',
      'Chỉ fileNum: chọn phiên mới nhất có đơn thuốc.',
      'Chỉ sessionId: tra FileNum từ ViewRX.',
      '',
      'Output: output/prescription-test/toa_<FileNum>_<SessionId>.pdf (mặc định).',
      '',
      'Upload AWS (cùng API backfill — cần S3_UPLOAD_API_BASE trong .env):',
      '  npm run test:prescription -- --upload --sessionId=30338',
      '  Thêm --local để vừa upload vừa ghi file test ra đĩa.',
    ].join('\n'),
  );
  process.exit(code);
}

async function main() {
  const argv = getPrescriptionCliArgv();
  if (argv.includes('--help') || argv.includes('-h')) usageAndExit(0);

  const parsed = parsePrescriptionTestArgs(argv);
  if (
    (parsed.fileNum == null || String(parsed.fileNum).trim() === '') &&
    (parsed.sessionId == null || String(parsed.sessionId).trim() === '')
  ) {
    usageAndExit(1);
  }

  const root = path.join(__dirname, '..');
  const outDir = path.join(root, 'output', 'prescription-test');

  if (parsed.upload) {
    const base = String(process.env.S3_UPLOAD_API_BASE || '').trim();
    if (!base) {
      // eslint-disable-next-line no-console
      console.error('Thiếu S3_UPLOAD_API_BASE trong .env (cần cho --upload).');
      process.exitCode = 1;
      return;
    }
  }

  await db.initializePool();
  try {
    const { fileNum, sessionId } = await resolveFileNumSessionIdForPrescription(parsed);
    const writeLocal = !parsed.upload || parsed.local;
    const outPdf = writeLocal ? path.join(outDir, `toa_${fileNum}_${sessionId}.pdf`) : null;

    logger.info('Prescription PDF test starting', {
      fileNum,
      sessionId,
      outPdf: outPdf || '(không ghi đĩa)',
      upload: parsed.upload,
    });

    const pdf = await generatePrescriptionPdf(fileNum, sessionId, {});

    const minBytes = PRESCRIPTION_CONFIG.MIN_PDF_BYTES;
    if (!Buffer.isBuffer(pdf) || pdf.length < minBytes) {
      throw new Error(`PDF không hợp lệ hoặc quá nhỏ (< ${minBytes} bytes)`);
    }

    if (parsed.upload) {
      const prefix = normalizePrescriptionS3Prefix();
      const uploadName = `${sessionId}.pdf`;
      const base = String(process.env.S3_UPLOAD_API_BASE || '').trim();
      const up = await uploadPdfMultipartBuffer(pdf, uploadName, base, prefix);
      logger.info('Prescription PDF uploaded', {
        fileNum,
        sessionId,
        prefix,
        uploadName,
        responseKeys: up && typeof up === 'object' && !Array.isArray(up) ? Object.keys(up) : null,
      });
      // eslint-disable-next-line no-console
      console.log(`uploaded: ${prefix}${uploadName}`);
    }

    if (writeLocal) {
      fs.mkdirSync(path.dirname(outPdf), { recursive: true });
      fs.writeFileSync(outPdf, pdf);
    }

    logger.info('Prescription PDF OK', {
      fileNum,
      sessionId,
      bytes: pdf.length,
      outPdf: outPdf || null,
    });
    // eslint-disable-next-line no-console
    if (outPdf) console.log(outPdf);
    process.exitCode = 0;
  } catch (e) {
    logger.error('Prescription PDF failed', { message: e?.message || String(e) });
    process.exitCode = 1;
  } finally {
    await db.closePool();
  }
}

main();
