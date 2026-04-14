const fs = require('fs');
const path = require('path');
const os = require('os');
const crypto = require('crypto');
const PizZip = require('pizzip');
const Docxtemplater = require('docxtemplater');
const { convertWithOffice } = require('../utils/officeConvert');
const { validateWordTemplateFile } = require('../utils/wordFileValidate');
const logger = require('../utils/logger');
const {
  getPrescriptionReportContext,
  getDistinctPrescriptionSubSessionIds,
} = require('../repositories/prescriptionReportRepository');
const { fixToaThuocDocxPlaceholders } = require('../utils/toaThuocDocxPlaceholders');
const { renderBarcodeCode128Png } = require('../utils/prescriptionBarcode');
const {
  injectBarcodeIntoDocx,
  BARCODE_DOCX_PLACEHOLDER,
} = require('../utils/injectBarcodeIntoDocx');
const { mergePdfBuffers } = require('../utils/pdfMerge');
const {
  str,
  formatSummaryQuantity,
  formatBirthYear,
  formatRxItemLine1Split,
  formatRxPerDLine,
  formatRxDplCell,
  formatRxNoteBlock,
  formatRxTotalCell,
} = require('../utils/prescriptionPayloadHelpers');

const DEFAULT_TEMPLATE = path.join(
  __dirname,
  '../../Templates/ToaThuoc/toathuoc.docx',
);

function formatDateVN(dateInput) {
  if (!dateInput) return '';
  const d = new Date(dateInput);
  if (Number.isNaN(d.getTime())) return '';
  const dd = String(d.getDate()).padStart(2, '0');
  const mm = String(d.getMonth() + 1).padStart(2, '0');
  const yyyy = d.getFullYear();
  return `${dd}/${mm}/${yyyy}`;
}

/** Chẩn đoán: xuống dòng theo mệnh đề (;) — docxtemplater linebreaks → w:br, hiển thị dưới nhãn Chẩn đoán */
function formatConclusionMultiline(dxText) {
  let t = str(dxText).trim();
  if (!t) return '';
  // Tôn trọng template: giữ newline nếu có (docxtemplater linebreaks=true sẽ map \n → w:br)
  return t.replace(/\r\n/g, '\n');
}

function buildAddressLine(patientRow) {
  if (!patientRow) return '';
  const parts = [
    patientRow.AddressNo,
    patientRow.Street,
    patientRow.Ward,
    patientRow.District,
    patientRow.City,
  ]
    .map((x) => (x == null ? '' : String(x).trim()))
    .filter(Boolean);
  return parts.join(', ');
}

function buildPayloadFromContext(ctx, fileNum, sessionId) {
  const { patient, vitals, diagnoses, rxLines, patientBarcode, diagnosisFromImagingReport } =
    ctx;
  const refDate = rxLines[0]?.BeginDate || new Date();

  const dxFromImaging = str(diagnosisFromImagingReport).trim();
  const dxFromDxRows = (diagnoses || [])
    .map((d) => str(d.Notes) || [d.ICDCode1, d.ICDCode2].filter(Boolean).join(' — '))
    .filter(Boolean)
    .join('; ');
  const dxText = [dxFromImaging, dxFromDxRows].filter(Boolean).join('; ');

  const instructions = [
    ...new Set((rxLines || []).map((r) => str(r.INSTRUCTIONS).trim()).filter(Boolean)),
  ].join('\n');

  const first = rxLines[0] || {};
  const lastDoctor = [...rxLines].reverse().find((r) => str(r.DocName).trim());
  const maxRepeats = Math.max(0, ...(rxLines || []).map((r) => Number(r.REPEATS) || 0));

  const pulse = vitals && vitals.Pulse != null ? str(vitals.Pulse) : '';
  const temp = vitals && vitals.Temp != null ? str(vitals.Temp) : '';
  const bp =
    vitals && (vitals.Systolic != null || vitals.Diastolic != null)
      ? `${str(vitals.Systolic)}/${str(vitals.Diastolic)}`
      : '';
  const resp = vitals && vitals.Resp != null ? str(vitals.Resp) : '';

  const birthYear = formatBirthYear(patient?.Dob);
  const ageSpaced = birthYear ? ` ${birthYear}` : '';

  const qtySummary = formatSummaryQuantity(rxLines);
  const qtyFirst = qtySummary || (first.QUANTITY != null ? str(first.QUANTITY) : str((rxLines || []).length));

  const freq = first.FREQUENCY != null ? str(first.FREQUENCY) : '';
  const perD = freq ? `${freq} lần/ngày` : '';

  const items = (rxLines || []).map((r, i) => ({
    ItemLine: formatRxItemLine1Split(i, r),
    PerD: formatRxPerDLine(r),
    DPL: formatRxDplCell(r),
    Note: formatRxNoteBlock(r),
    RowQty: formatRxTotalCell(r),
  }));

  return {
    PatientName: str(patient?.FullName),
    Conclusion: formatConclusionMultiline(dxText),
    Pulse: pulse,
    Temp: temp,
    BloodPressure: bp,
    RespiratoryRate: resp,
    Gender: str(patient?.Sex),
    Address: buildAddressLine(patient),
    FileNm: str(fileNum),
    Barcode: str(patientBarcode),
    BarcodeImg: BARCODE_DOCX_PLACEHOLDER,
    Age: ageSpaced,
    Quantity: qtyFirst,
    Note: instructions,
    DPL: maxRepeats > 0 ? str(maxRepeats) : str(first.REPEATS ?? ''),
    PerD: perD,
    items,
    DateRpt: formatDateVN(refDate),
    ReExamDate: formatDateVN(first.FinishDate),
    Suggestion: '',
    Doctor: str(lastDoctor?.DocName),
    Test: '',
  };
}

/**
 * @param {string} fileNum
 * @param {number} sessionId
 * @param {{ templatePath?: string, outputPdfPath?: string }} [options]
 * @returns {Promise<Buffer>}
 */
async function generatePrescriptionPdf(fileNum, sessionId, options = {}) {
  const templatePath = options.templatePath
    ? path.resolve(options.templatePath)
    : path.resolve(DEFAULT_TEMPLATE);
  if (!fs.existsSync(templatePath)) {
    throw new Error(`Không thấy template đơn thuốc: ${templatePath}`);
  }

  // Một SessionId có nhiều SubSessionId → mỗi SubSessionId một toa (PDF gộp theo thứ tự).
  // Không có SubSessionId trên ViewRX → một toa theo cả SessionId (như cũ).
  const subSessionIds = await getDistinctPrescriptionSubSessionIds(fileNum, sessionId);
  const work = subSessionIds.length ? subSessionIds : [null];

  const buffers = [];
  for (let i = 0; i < work.length; i++) {
    const unit = work[i];
    const ctx = await getPrescriptionReportContext(fileNum, sessionId, unit);
    if (!ctx.patient) {
      throw new Error(`Không có bệnh nhân FileNum=${fileNum}`);
    }
    if (!ctx.rxLines || ctx.rxLines.length === 0) {
      const suffix = unit != null ? `, SubSessionId=${unit}` : '';
      throw new Error(
        `Không có dòng thuốc (ViewRX) cho FileNum=${fileNum}, SessionId=${sessionId}${suffix}`,
      );
    }

    if (ctx.vitalsSource) {
      logger.info('Prescription vitals resolved', {
        vitalsSource: ctx.vitalsSource,
        sessionId,
        subSessionId: ctx.subSessionId ?? undefined,
      });
    }

    const payload = buildPayloadFromContext(ctx, fileNum, sessionId);
    logger.info('ViewRX rows on prescription segment', {
      count: ctx.rxLines.length,
      fileNum,
      sessionId,
      subSessionId: ctx.subSessionId ?? undefined,
    });

    const tmp = path.join(os.tmpdir(), `rx_rpt_${crypto.randomBytes(8).toString('hex')}`);
    fs.mkdirSync(tmp, { recursive: true });

    try {
      validateWordTemplateFile(templatePath);

      const templateDocxPath = path.join(tmp, 'toa_template.docx');
      fs.copyFileSync(templatePath, templateDocxPath);
      fixToaThuocDocxPlaceholders(templateDocxPath);

      const barcodeImg = await renderBarcodeCode128Png(payload.Barcode);

      const postFixBuf = fs.readFileSync(templateDocxPath, 'binary');
      const docZip = new PizZip(postFixBuf);
      const doc = new Docxtemplater(docZip, {
        delimiters: { start: '<<', end: '>>' },
        linebreaks: true,
        paragraphLoop: true,
        nullGetter() {
          return '';
        },
      });

      const proxy = new Proxy(payload, {
        get(target, prop) {
          if (typeof prop === 'string' && !(prop in target)) return '';
          return target[prop];
        },
      });
      doc.render(proxy);

      const baseName =
        unit != null
          ? `toa_${fileNum}_${sessionId}_sub${unit}_${String(i + 1).padStart(2, '0')}`
          : `toa_${fileNum}_${sessionId}`;
      const renderedDocxPath = path.join(tmp, `${baseName}.docx`);
      fs.writeFileSync(renderedDocxPath, doc.getZip().generate({ type: 'nodebuffer' }));

      injectBarcodeIntoDocx(renderedDocxPath, barcodeImg);

      try {
        await convertWithOffice('pdf', renderedDocxPath, tmp);
      } catch (e) {
        if (String(process.env.PRESCRIPTION_DEBUG_FAILED_DOCX || '').toLowerCase() === 'true') {
          try {
            const failDir = path.resolve(process.cwd(), 'output', 'prescription-test');
            fs.mkdirSync(failDir, { recursive: true });
            const copyTo = path.join(failDir, `${baseName}_debug_fail.docx`);
            fs.copyFileSync(renderedDocxPath, copyTo);
            logger.warn('PRESCRIPTION_DEBUG_FAILED_DOCX: đã lưu docx', { copyTo });
          } catch (_) {
            /* ignore */
          }
        }
        throw e;
      }

      const pdfPath = path.join(tmp, `${baseName}.pdf`);
      if (!fs.existsSync(pdfPath)) {
        throw new Error(`Không tạo được PDF (LibreOffice/Word): ${renderedDocxPath}`);
      }
      buffers.push(fs.readFileSync(pdfPath));
    } finally {
      try {
        fs.rmSync(tmp, { recursive: true, force: true });
      } catch (_) {
        /* ignore */
      }
    }
  }

  const merged = buffers.length === 1 ? buffers[0] : await mergePdfBuffers(buffers);
  if (options.outputPdfPath) {
    const out = path.resolve(options.outputPdfPath);
    fs.mkdirSync(path.dirname(out), { recursive: true });
    fs.writeFileSync(out, merged);
  }
  return merged;
}

module.exports = {
  generatePrescriptionPdf,
  buildPayloadFromContext,
  DEFAULT_TEMPLATE,
};
