const fs = require('fs');
const PizZip = require('pizzip');
const sizeOf = require('image-size');
const templates = require('docxtemplater-image/js/templates');
const DocUtils = require('docxtemplater-image/js/docUtils');

const PLACEHOLDER = 'RXBCIMG';

function nextFreeRId(relsXml) {
  let max = 0;
  const re = /Id="rId(\d+)"/g;
  let m;
  while ((m = re.exec(relsXml)) !== null) {
    max = Math.max(max, parseInt(m[1], 10));
  }
  return max + 1;
}

function ensurePngContentType(zip) {
  const ctPath = '[Content_Types].xml';
  const f = zip.file(ctPath);
  if (!f) return;
  let xml = f.asText();
  if (xml.includes('Extension="png"')) return;
  xml = xml.replace(
    '</Types>',
    '<Default Extension="png" ContentType="image/png"/></Types>',
  );
  zip.file(ctPath, xml);
}

/**
 * Thay placeholder (sau docxtemplater) bằng ảnh PNG mã vạch.
 * docxtemplater-image dùng {%…} không tương thích khi toàn tài liệu dùng << >>.
 */
function injectBarcodeIntoDocx(docxPath, pngBuffer) {
  if (!pngBuffer || !Buffer.isBuffer(pngBuffer) || pngBuffer.length < 10) return;

  const zip = new PizZip(fs.readFileSync(docxPath));
  const docPath = 'word/document.xml';
  const f = zip.file(docPath);
  if (!f) return;
  let docXml = f.asText();
  if (!docXml.includes(PLACEHOLDER)) return;

  const relsPath = 'word/_rels/document.xml.rels';
  const relsFile = zip.file(relsPath);
  if (!relsFile) return;
  let relsXml = relsFile.asText();
  /** Số thứ tự cho Relationship Id="rIdN" — getImageXml đã nối sẵn tiền tố "rId". */
  const rIdNum = nextFreeRId(relsXml);
  const rid = `rId${rIdNum}`;

  const mediaName = `rx_barcode_${rIdNum}.png`;
  zip.file(`word/media/${mediaName}`, pngBuffer);

  ensurePngContentType(zip);

  relsXml = relsXml.replace(
    '</Relationships>',
    `<Relationship Id="${rid}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/image" Target="media/${mediaName}"/></Relationships>`,
  );
  zip.file(relsPath, relsXml);

  const maxW = Math.max(80, Math.min(400, Number(process.env.PRESCRIPTION_BARCODE_MAX_WIDTH_PX) || 140));
  let wPx = maxW;
  let hPx = Math.max(22, Math.round(0.25 * maxW));
  try {
    const dim = sizeOf(pngBuffer);
    if (dim.width && dim.height) {
      wPx = Math.min(dim.width, maxW);
      hPx = Math.max(22, Math.round((dim.height / dim.width) * wPx));
    }
  } catch (_) {
    /* default */
  }

  const emuW = DocUtils.convertPixelsToEmus(wPx);
  const emuH = DocUtils.convertPixelsToEmus(hPx);
  const drawingXml = templates.getImageXml(rIdNum, [emuW, emuH]);

  /** Không dùng [\s\S]*? tới w:t — sẽ vượt </w:r> và nuốt cả bảng (mất thẻ w:tc, LibreOffice lỗi). */
  const runRe = new RegExp(
    `<w:r\\b[^>]*>(?:(?!</w:r>).)*?<w:t[^>]*>${PLACEHOLDER}</w:t>[\\s\\S]*?</w:r>`,
    'g',
  );
  docXml = docXml.replace(runRe, `<w:r><w:rPr/>${drawingXml}</w:r>`);

  zip.file(docPath, docXml);
  fs.writeFileSync(docxPath, zip.generate({ type: 'nodebuffer' }));
}

module.exports = {
  injectBarcodeIntoDocx,
  BARCODE_DOCX_PLACEHOLDER: PLACEHOLDER,
};
