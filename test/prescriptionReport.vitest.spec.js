/**
 * Integration (Vitest): cùng logic với `npm run test:prescription`.
 *
 * Mặc định (không tham số): FileNum 15070213 + SessionId 30338.
 * Truyền tham số sau `--` của Vitest (không dùng biến môi trường):
 *   npx vitest run test/prescriptionReport.vitest.spec.js -- --sessionId=30338
 *   npx vitest run test/prescriptionReport.vitest.spec.js -- 15070213
 *   npx vitest run test/prescriptionReport.vitest.spec.js -- --fileNum=15070213 --sessionId=30338
 *
 * Khuyến nghị hằng ngày: npm run test:prescription -- ...
 * Upload API (cần S3_UPLOAD_API_BASE trong .env):
 *   npx vitest run test/prescriptionReport.vitest.spec.js -- --upload --sessionId=30338
 *   Thêm --local để vừa upload vừa ghi output/prescription-test/
 */
import path from 'node:path';
import fs from 'node:fs';
import { fileURLToPath } from 'node:url';
import { createRequire } from 'node:module';
import { describe, it, expect, beforeAll } from 'vitest';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, '..');
const require = createRequire(import.meta.url);

beforeAll(() => {
  require('dotenv').config({ path: path.join(root, '.env') });
  process.chdir(root);
});

describe('prescription PDF from ToaThuoc template', () => {
  it(
    'queries ViewRX and writes PDF (fileNum và/hoặc sessionId qua argv sau --)',
    async () => {
      const db = require('../src/config/database');
      const { generatePrescriptionPdf } = require('../src/services/prescriptionReportService');
      const { resolveFileNumSessionIdForPrescription } = require('../src/repositories/prescriptionReportRepository');
      const {
        parsePrescriptionTestArgs,
        getPrescriptionCliArgv,
      } = require('../src/utils/prescriptionReportTestArgs');
      const { PRESCRIPTION_CONFIG, normalizePrescriptionS3Prefix } = require('../src/config/prescriptionConstants');

      const parsed = parsePrescriptionTestArgs(getPrescriptionCliArgv());
      const hasFile = parsed.fileNum != null && String(parsed.fileNum).trim() !== '';
      const hasSess = parsed.sessionId != null && String(parsed.sessionId).trim() !== '';
      const merged =
        hasFile || hasSess ? parsed : { fileNum: '15070213', sessionId: '30338' };

      if (parsed.upload && !String(process.env.S3_UPLOAD_API_BASE || '').trim()) {
        throw new Error('Cần S3_UPLOAD_API_BASE trong .env khi dùng --upload');
      }

      const { uploadPdfMultipartBuffer } = require('../src/utils/s3UploadMultipart');

      await db.initializePool();
      try {
        const { fileNum, sessionId } = await resolveFileNumSessionIdForPrescription(merged);
        const outDir = path.join(root, 'output', 'prescription-test');
        const writeLocal = !parsed.upload || parsed.local;
        const outPdf = writeLocal ? path.join(outDir, `toa_${fileNum}_${sessionId}.pdf`) : null;
        const pdf = await generatePrescriptionPdf(fileNum, sessionId, {});
        const minBytes = PRESCRIPTION_CONFIG.MIN_PDF_BYTES;
        expect(Buffer.isBuffer(pdf)).toBe(true);
        expect(pdf.length).toBeGreaterThan(minBytes);
        if (parsed.upload) {
          const prefix = normalizePrescriptionS3Prefix();
          const uploadName = `${sessionId}.pdf`;
          const base = String(process.env.S3_UPLOAD_API_BASE || '').trim();
          const up = await uploadPdfMultipartBuffer(pdf, uploadName, base, prefix);
          expect(up).toBeDefined();
        }
        if (writeLocal) {
          fs.mkdirSync(path.dirname(outPdf), { recursive: true });
          fs.writeFileSync(outPdf, pdf);
          expect(fs.existsSync(outPdf)).toBe(true);
          expect(fs.statSync(outPdf).size).toBeGreaterThan(minBytes);
        }
      } finally {
        await db.closePool();
      }
    },
    180_000,
  );
});
