const fs = require('fs');
const PizZip = require('pizzip');
const logger = require('./logger');
const {
  escapeXmlText,
  formatRxTotalCell,
  formatRxItemLine1Split,
  formatRxPerDLine,
  formatRxDplCell,
  formatRxNoteBlock,
} = require('./prescriptionPayloadHelpers');

const MARKER = '__RX_C1__';

/** Lệch dọc (twips) giữa hai thuốc: giữ khoảng (y_hàng_dưới − y_hàng_trên) + chiều cao hàng dưới + margin */
const RX_ROW_STEP_TWIPS = Number(process.env.PRESCRIPTION_RX_ROW_STEP_TWIPS) || 1000;

/**
 * Sau N thuốc trên một trang, chèn page break và reset w:y (tránh chồng lên chẩn đoán / footer).
 * Chỉnh PRESCRIPTION_RX_ROWS_PER_PAGE nếu máy in / mẫu khác chiều cao vùng thuốc.
 */
const RX_ROWS_PER_PAGE = Math.max(
  1,
  Number(process.env.PRESCRIPTION_RX_ROWS_PER_PAGE) || 8,
);

const PAGE_BREAK_PARA = '<w:p><w:r><w:br w:type="page"/></w:r></w:p>';

/** w:y (twips) của dòng đầu khối thuốc mẫu — neo cho trang 1 */
function extractFrameYFromPara(paraXml) {
  const m = paraXml.match(/w:y="(\d+)"/);
  return m ? parseInt(m[1], 10) : null;
}

/**
 * Trang 2+: neo danh sách gần mép trên (tránh giữ offset như sau phần chẩn đoán trang 1).
 * Chỉnh nếu vẫn chồng header / quá sát mép.
 */
const RX_CONTINUATION_ANCHOR_TWIPS = Math.max(
  0,
  Number(process.env.PRESCRIPTION_RX_CONTINUATION_ANCHOR_TWIPS) || 540,
);

const PART_RE =
  /^word\/(document\.xml|header\d+\.xml|footer\d+\.xml|footnotes\.xml|endnotes\.xml)$/i;

const RX_TEMPLATE_PARAS = 7;

/**
 * Trích 7 <w:p> mẫu: <<Quantity>>, <<Items>> (marker), <<Note>>, "Ngày", ", mỗi lần", <<DPL>>, <<PerD>>
 * Gọi sau fixToaThuocDocxPlaceholders, trước docxtemplater.render.
 */
function extractRxRowTemplateSeven(documentXml) {
  const list = splitParagraphs(documentXml);
  const idx = list.findIndex((p) => p.includes(MARKER));
  if (idx < 1 || idx + 5 >= list.length) {
    logger.warn(
      'Toa thuốc: không trích được 7 đoạn mẫu (Quantity…PerD) — thiếu marker __RX_C1__ hoặc thứ tự đoạn đổi',
    );
    return null;
  }
  return list.slice(idx - 1, idx + 6);
}

function splitParagraphs(docXml) {
  const re = /<w:p\b[\s\S]*?<\/w:p>/g;
  const out = [];
  let m;
  while ((m = re.exec(docXml)) !== null) out.push(m[0]);
  return out;
}

function findRxBlockCharRange(docXml) {
  const re = /<w:p\b[\s\S]*?<\/w:p>/g;
  const paras = [];
  let m;
  while ((m = re.exec(docXml)) !== null) {
    paras.push({ start: m.index, end: m.index + m[0].length, xml: m[0] });
  }
  const idx = paras.findIndex((p) => p.xml.includes(MARKER));
  if (idx < 1 || idx + 5 >= paras.length) return null;
  return { start: paras[idx - 1].start, end: paras[idx + 5].end };
}

/**
 * Sau docxtemplater: xóa 7 đoạn mẫu, chèn N×7 đoạn (mỗi thuốc giữ đúng cấu trúc + viền mẫu), chỉ đổi text và w:y.
 */
function injectClonedPrescriptionItemParagraphs(docxPath, rxLines, templateSevenParas) {
  const rows = rxLines || [];
  const zip = new PizZip(fs.readFileSync(docxPath));
  let touched = false;

  for (const name of Object.keys(zip.files)) {
    const entry = zip.files[name];
    if (!entry || entry.dir) continue;
    const n = name.replace(/\\/g, '/');
    if (!PART_RE.test(n)) continue;

    let xml = zip.file(name).asText();
    if (!xml.includes(MARKER)) continue;

    const range = findRxBlockCharRange(xml);
    if (!range) continue;

    let block = '';
    if (rows.length > 0) {
      if (!templateSevenParas || templateSevenParas.length !== RX_TEMPLATE_PARAS) {
        logger.warn(
          'Toa thuốc: thiếu template 7 đoạn — không inject (cần extractRxRowTemplateSeven trước render)',
        );
        continue;
      }
      const rowRefY = extractFrameYFromPara(templateSevenParas[0]);
      if (rowRefY == null) {
        logger.warn(
          'Toa thuốc: không đọc được w:y từ đoạn mẫu — offset trang sau có thể lệch (kiểm tra mẫu Word)',
        );
      }
      const layout = { rowRefY, continuationAnchorY: RX_CONTINUATION_ANCHOR_TWIPS };

      block = rows
        .map((r, i) => {
          const idxInPage = i % RX_ROWS_PER_PAGE;
          const pagePrefix =
            i > 0 && idxInPage === 0 ? PAGE_BREAK_PARA : '';
          const pageIndex = Math.floor(i / RX_ROWS_PER_PAGE);
          return (
            pagePrefix +
            buildOneRxRowFromTemplate(
              templateSevenParas,
              idxInPage,
              i,
              r,
              RX_ROW_STEP_TWIPS,
              pageIndex,
              layout,
            )
          );
        })
        .join('');
    }

    const out = xml.slice(0, range.start) + block + xml.slice(range.end);
    if (out !== xml) {
      zip.file(name, out);
      touched = true;
    }
  }

  if (!touched && rows.length > 0) {
    logger.warn(
      'Toa thuốc: không thấy marker hoặc không tìm được khối 7 đoạn — kiểm tra fixToaThuocDocxPlaceholders / <<Items>>',
    );
  }

  if (touched) {
    fs.writeFileSync(docxPath, zip.generate({ type: 'nodebuffer' }));
  }
}

function extractPPr(paraXml) {
  const mm = paraXml.match(/<w:pPr\b[\s\S]*?<\/w:pPr>/);
  return mm ? mm[0] : '<w:pPr/>';
}

function offsetParagraphFrameY(paraXml, deltaTwips) {
  if (!deltaTwips) return paraXml;
  return paraXml.replace(/(<w:framePr\b[^>]*\s)w:y="(\d+)"/g, (_, prefix, y) => {
    return `${prefix}w:y="${parseInt(y, 10) + deltaTwips}"`;
  });
}

function softenExactFrames(paraXml) {
  return paraXml.replace(/w:hRule="exact"/g, 'w:hRule="atLeast"');
}

/** INSTRUCTIONS có thể xuống dòng — nới khung tối thiểu (mẫu gốc ~279 twips) */
function widenNoteFrameMinHeight(paraXml) {
  return paraXml.replace(/(<w:framePr\b[^>]*\s)w:h="(\d+)"/, (full, prefix, h) => {
    const n = parseInt(h, 10);
    return `${prefix}w:h="${Math.max(n, 480)}"`;
  });
}

/**
 * Giữ w:pPr (frame, tabs, pBdr mẫu), thay nội dung một run — giữ rPr từ run đầu nếu có
 */
function replaceParagraphPlainText(paraXml, text) {
  const pPr = extractPPr(paraXml);
  const firstRunMatch = paraXml.match(/<w:r\b[\s\S]*?<\/w:r>/);
  let rPrInner = '';
  if (firstRunMatch) {
    const inner = firstRunMatch[0].match(/<w:rPr\b[\s\S]*?<\/w:rPr>/);
    if (inner) rPrInner = inner[0];
  }
  const body = rPrInner
    ? `<w:r>${rPrInner}<w:t xml:space="preserve">${escapeXmlText(text)}</w:t></w:r>`
    : `<w:r><w:t xml:space="preserve">${escapeXmlText(text)}</w:t></w:r>`;
  return `<w:p>${pPr}${body}</w:p>`;
}

function buildOneRxRowFromTemplate(
  tpl7,
  yIndexOnPage,
  drugDisplayIndex,
  r,
  stepTwips,
  pageIndex,
  layout,
) {
  let baseShift;
  if (layout.rowRefY != null) {
    const anchor = pageIndex === 0 ? layout.rowRefY : layout.continuationAnchorY;
    baseShift = anchor - layout.rowRefY + yIndexOnPage * stepTwips;
  } else {
    baseShift = yIndexOnPage * stepTwips;
  }
  const payloads = [
    formatRxTotalCell(r),
    formatRxItemLine1Split(drugDisplayIndex, r),
    formatRxNoteBlock(r),
    null,
    null,
    formatRxDplCell(r),
    formatRxPerDLine(r),
  ];

  let xml = '';
  for (let i = 0; i < RX_TEMPLATE_PARAS; i += 1) {
    let p = offsetParagraphFrameY(tpl7[i], baseShift);
    p = softenExactFrames(p);
    if (i === 2) p = widenNoteFrameMinHeight(p);
    if (payloads[i] !== null) p = replaceParagraphPlainText(p, payloads[i]);
    xml += p;
  }
  return xml;
}

module.exports = {
  injectClonedPrescriptionItemParagraphs,
  extractRxRowTemplateSeven,
  RX_TEMPLATE_PARAS,
  RX_ROW_MARKERS: {
    c1: '__RX_C1__',
    c2: '__RX_C2__',
    c3: '__RX_C3__',
    c4: '__RX_C4__',
  },
};
