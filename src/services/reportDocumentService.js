const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const PizZip = require('pizzip');
const Docxtemplater = require('docxtemplater');
const logger = require('../utils/logger');
const { decompressToString } = require('../utils/gzipHelper');
const { extractImagesFromArchiveOrRaw, ensureDirectoryExists } = require('../utils/fileHelper');
const { convertWithOffice } = require('../utils/officeConvert');
const { rtfToPlainText } = require('../utils/rtfToPlain');
const { mergeBasePdfWithImagePages } = require('../utils/pdfMerge');
const { writeRtfFileForOffice } = require('../utils/rtfFileWrite');
const { validateWordTemplateFile } = require('../utils/wordFileValidate');
const { calcAge } = require('../utils/age');
const { getPrintedImageFilenames } = require('../repositories/reportRepository');
const { rtfToHtmlLocal, htmlToTextLoose } = require('../utils/rtfToHtmlLocal');

function nowMs() {
  return Number(process.hrtime.bigint() / 1000000n);
}

function timingEnabled() {
  return String(process.env.REPORT_TIMING || '').toLowerCase().trim() === 'true';
}

function richTextMode() {
  // legacy default: rtf_inject
  // test: rtf_html_plain (RTF->HTML local -> plain text; avoids RTF->DOCX conversion)
  return String(process.env.REPORT_RICH_TEXT_MODE || 'rtf_inject').toLowerCase().trim();
}

function formatDateVN(dateInput) {
  if (!dateInput) return '';
  const d = new Date(dateInput);
  if (Number.isNaN(d.getTime())) return '';
  const dd = String(d.getDate()).padStart(2, '0');
  const mm = String(d.getMonth() + 1).padStart(2, '0');
  const yyyy = d.getFullYear();
  return `${dd}/${mm}/${yyyy}`;
}

function buildDoctorLabel(record) {
  let name = record.doctor || '';
  if (record.doctorQualification && String(record.doctorQualification).trim()) {
    name = `${record.doctorQualification} ${name}`.trim();
  }
  return name;
}

function str(v) {
  if (v === undefined || v === null) return '';
  let s = String(v);
  try {
    if (typeof s.normalize === 'function') s = s.normalize('NFC');
  } catch (_) {
    /* ignore */
  }
  return s;
}

function reportLogRtfMetaEnabled() {
  return String(process.env.REPORT_LOG_RTF_META || '').toLowerCase() === 'true';
}

function logRtfBlobMeta(label, rawBuffer, decodedString) {
  if (!reportLogRtfMetaEnabled()) return;
  const b = rawBuffer && Buffer.isBuffer(rawBuffer) ? rawBuffer : null;
  const looksGzip = b && b.length >= 2 && b[0] === 0x1f && b[1] === 0x8b;
  const s = decodedString == null ? '' : String(decodedString);
  const preview = s.slice(0, 160).replace(/\s+/g, ' ').trim();
  logger.info(`RTF blob ${label}`, {
    gzipMagic: looksGzip,
    rawBytes: b ? b.length : 0,
    decodedLen: s.length,
    startsWithRtf: /^\s*\{\\rtf/i.test(s),
    preview,
  });
}

/** Giảm block newline/spaces thừa từ DB khi render plain vào template (không đụng token RTF/ảnh). */
function strReportField(v) {
  const s = str(v);
  if (!s) return '';
  if (s.includes('__RTF_') || s.includes('__IMG_')) return s;
  return s
    .replace(/\r\n/g, '\n')
    .replace(/[ \t]+\n/g, '\n')
    .replace(/\n[ \t]+/g, '\n')
    .replace(/\n{5,}/g, '\n\n\n\n')
    .trim();
}

/**
 * Payload render docx.
 * - Tránh "undefined" bằng cách luôn trả '' cho key thiếu/không set.
 * - Pre-fill Image1..ImageN để các placeholder ảnh không bị tràn sang undefined.
 */
function buildRenderPayload(record, rtfTokens) {
  const IMAGE_KEYS_MAX = 200;
  const ageRaw = str(calcAge(record.dob, record.ngayKham));
  /** Một số template dính <<PatientName>><<Age>> không có khoảng — thêm space đầu tuổi. */
  const ageSpaced = ageRaw ? ` ${ageRaw}` : '';
  const payload = {
    FileNm: str(record.fileNum || record.itemNum),
    PatientName: str(record.patientName),
    Age: ageSpaced,
    Gender: str(record.gender),
    Diagnosis: strReportField(record.conclusion),
    ReferDoctor: str(record.requestedDoctor),
    Doctor: str(buildDoctorLabel(record)),
    ItemNum: str(record.itemNum),
    SampleNumber: str(record.sampleNumber || record.itemNum),
    Address: str(record.address),
    PathologyName: '',
    CDNS: '',
    DateRpt: str(formatDateVN(record.ngayKham)),
    Result: strReportField(rtfTokens.resultToken),
    Conclusion: strReportField(rtfTokens.conclusionToken),
    Suggestion: strReportField(rtfTokens.suggestionToken),
    SessionId: str(record.sessionId),
    FileNum: str(record.fileNum),
    PacsViewURL: str(record.pacs?.viewUrl),
    PacsFileResultURL: str(record.pacs?.fileResultUrl),
    PacsAccessCode: str(record.pacs?.accessCode),
  };
  for (let i = 1; i <= IMAGE_KEYS_MAX; i += 1) {
    payload[`Image${i}`] = '';
  }
  if (rtfTokens && rtfTokens.imageTokens) {
    for (const [k, v] of Object.entries(rtfTokens.imageTokens)) {
      payload[k] = str(v);
    }
  }

  return new Proxy(payload, {
    get(target, prop) {
      if (typeof prop === 'string' && !(prop in target)) return '';
      return target[prop];
    },
  });
}

async function convertWithLibreOffice(mode, inputPath, outDir) {
  await convertWithOffice(mode, inputPath, outDir);
}

function extractWordDocumentXmlInnerBody(docXml) {
  const bodyStart = docXml.indexOf('<w:body>');
  const bodyEnd = docXml.indexOf('</w:body>');
  if (bodyStart < 0 || bodyEnd < 0 || bodyEnd <= bodyStart) return '';

  let inner = docXml.slice(bodyStart + '<w:body>'.length, bodyEnd);

  const sectStart = inner.lastIndexOf('<w:sectPr');
  if (sectStart >= 0) {
    inner = inner.slice(0, sectStart);
  }
  return inner;
}

const OOXML_PARA_REGEX = /<w:p\b[\s\S]*?<\/w:p>/g;

function decodeXmlEntities(s) {
  if (!s) return '';
  return String(s)
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&apos;/g, "'");
}

/** Ghép mọi <w:t> trong một <w:p> — Word hay tách placeholder thành nhiều run. */
function paragraphPlainText(paraXml) {
  let text = '';
  const re = /<w:t[^>]*>([\s\S]*?)<\/w:t>/g;
  let m;
  while ((m = re.exec(paraXml)) !== null) {
    text += decodeXmlEntities(m[1]);
  }
  return text;
}

function paragraphPlainTextWithLineBreaks(paraXml) {
  const normalized = String(paraXml || '')
    .replace(/<w:br\b[^>]*\/?>/gi, '\n')
    .replace(/<w:cr\b[^>]*\/?>/gi, '\n');
  let text = '';
  const re = /<w:t[^>]*>([\s\S]*?)<\/w:t>/g;
  let m;
  while ((m = re.exec(normalized)) !== null) {
    text += decodeXmlEntities(m[1]);
  }
  return text;
}

function docContainsTokenInParagraphs(docXml, token) {
  if (!token) return false;
  if (docXml.includes(token)) return true;
  OOXML_PARA_REGEX.lastIndex = 0;
  let m;
  while ((m = OOXML_PARA_REGEX.exec(docXml)) !== null) {
    if (paragraphPlainText(m[0]).includes(token)) return true;
  }
  return false;
}

function replaceParagraphContainingToken(docXml, token, replacementInnerXml) {
  if (!token) return docXml;

  // Nhanh: token còn nguyên trong XML
  if (docXml.includes(token)) {
    const idx = docXml.indexOf(token);
    const before = docXml.slice(0, idx);
    const matches = [...before.matchAll(/<w:p(\s|>)/g)];
    const pStart = matches.length ? matches[matches.length - 1].index : -1;
    const pEnd = docXml.indexOf('</w:p>', idx);
    if (pStart < 0 || pEnd < 0) return docXml;
    const pEndWithTag = pEnd + '</w:p>'.length;
    return docXml.slice(0, pStart) + replacementInnerXml + docXml.slice(pEndWithTag);
  }

  // Token bị tách giữa nhiều <w:t> trong cùng một đoạn
  OOXML_PARA_REGEX.lastIndex = 0;
  let match;
  while ((match = OOXML_PARA_REGEX.exec(docXml)) !== null) {
    const full = match[0];
    if (paragraphPlainText(full).includes(token)) {
      return (
        docXml.slice(0, match.index) +
        replacementInnerXml +
        docXml.slice(match.index + full.length)
      );
    }
  }

  return docXml;
}

function normalizeParaTextForAlignment(plain) {
  return String(plain || '')
    .replace(/\s+/g, ' ')
    .trim();
}

/**
 * Sau inject RTF, có thể cần chỉnh đoạn chữ ký. Tiêu đề (PHIẾU / KẾT LUẬN / Đề nghị / ngày):
 * mặc định KHÔNG ghi đè — giữ căn như RTF + template (giống nguồn).
 *
 * - REPORT_ALIGN_CLOSING_BLOCK=false — tắt toàn bộ bước này.
 * - REPORT_CLOSING_HEADING_JC=preserve|rtf (mặc định) — không đụng tiêu đề/ ngày.
 *   center | both | left — ép cùng một kiểu căn cho các dòng khớp mẫu (both = căn đều).
 * - REPORT_ALIGN_DOCTOR_RIGHT=false — không ép căn phải dòng BS.
 * - REPORT_ALIGN_REFER_DOCTOR_LINES=true (mặc định) — ép căn PHẢI đoạn có nhãn
 *   kiểu "Bác sĩ chỉ định" / "BS chỉ định" (<<ReferDoctor>>), không cần bật REPORT_ALIGN_CLOSING_BLOCK.
 */
function stripVnAccents(s) {
  return String(s)
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/đ/g, 'd');
}

function closingHeadingJcFromEnv() {
  const v = String(process.env.REPORT_CLOSING_HEADING_JC || 'preserve')
    .toLowerCase()
    .trim();
  if (v === 'preserve' || v === 'rtf' || v === 'template' || v === '') {
    return null;
  }
  if (v === 'center') return 'center';
  if (v === 'both' || v === 'justify') {
    return 'both';
  }
  if (v === 'left') return 'left';
  return null;
}

function isClosingHeadingPattern(plainNorm, a) {
  const t = plainNorm;
  if (/^PHIẾU KẾT QUẢ\b/u.test(t) || /^Phiếu kết quả\b/iu.test(t) || a.startsWith('phieu ket qua')) {
    return true;
  }
  if ((/^KẾT LUẬN\b/u.test(t) || a.startsWith('ket luan')) && t.length < 64) {
    return true;
  }
  if ((/^Đề nghị\b/iu.test(t) || a.startsWith('de nghi')) && t.length < 48) {
    return true;
  }
  if (/^\d{1,2}\/\d{1,2}\/\d{4}$/.test(t)) {
    return true;
  }
  return false;
}

const DOCTOR_LINE_RE =
  /^(BS\.|Bs\.|BÁC\s*SĨ|Bác\s+sĩ|PGS[\s.]|P\.GS\.|TS[\s.]|ThS[\s.]|GS[\s.]|CK[\s.]*I\.|CKI\.)\b/i;

function lineLooksLikeDoctorSignature(line) {
  const x = String(line || '').trim();
  if (!x) return false;
  return DOCTOR_LINE_RE.test(x);
}

/** Đoạn có nhãn bác sĩ / BS chỉ định (thường là <<ReferDoctor>> sau khi render). */
function paragraphLooksLikeReferDoctorLabel(plainNorm, linesFromBr) {
  const parts = linesFromBr.length ? linesFromBr : [plainNorm];
  const joined = normalizeParaTextForAlignment(parts.join(' '));
  if (!joined || joined.length > 420) return false;
  const a = stripVnAccents(joined);
  if (!/\bchi\s*dinh\b/.test(a)) return false;
  if (/^(bs\.?|bac\s*si|bac\s*sy)\b/.test(a)) return true;
  if (/\b(bs\.?|bac\s*si|bac\s*sy)\s+chi\s*dinh\b/.test(a)) return true;
  if (/^(nguoi\s*chi\s*dinh|bac\s*si\s*chi\s*dinh)\b/.test(a)) return true;
  return false;
}

function classifyReferDoctorParagraphJc(plainNorm, linesFromBr) {
  const on =
    String(process.env.REPORT_ALIGN_REFER_DOCTOR_LINES || 'true').toLowerCase() !== 'false';
  if (!on) return null;
  const doctorRight =
    String(process.env.REPORT_ALIGN_DOCTOR_RIGHT || 'true').toLowerCase() !== 'false';
  if (!doctorRight) return null;
  if (!paragraphLooksLikeReferDoctorLabel(plainNorm, linesFromBr)) return null;
  return 'right';
}

function applyReferDoctorAlignmentToDocXml(docXml) {
  const off =
    String(process.env.REPORT_ALIGN_REFER_DOCTOR_LINES || 'true').toLowerCase() === 'false';
  if (off || !docXml) return docXml;

  OOXML_PARA_REGEX.lastIndex = 0;
  let out = '';
  let lastIndex = 0;
  let m;
  while ((m = OOXML_PARA_REGEX.exec(docXml)) !== null) {
    const full = m[0];
    const withBreaks = paragraphPlainTextWithLineBreaks(full);
    const linesFromBr = withBreaks
      .split(/\r?\n/)
      .map((ln) => ln.replace(/\s+/g, ' ').trim())
      .filter(Boolean);
    const plain = normalizeParaTextForAlignment(
      linesFromBr.length ? linesFromBr.join(' ') : paragraphPlainText(full),
    );
    const jc = classifyReferDoctorParagraphJc(plain, linesFromBr);
    const replacement = jc ? setParagraphJcSafe(full, jc) : full;
    out += docXml.slice(lastIndex, m.index) + replacement;
    lastIndex = m.index + full.length;
  }
  out += docXml.slice(lastIndex);
  return out;
}

function applyReferDoctorAlignmentToDocxPath(docxPath) {
  if (!docxPath || !fs.existsSync(docxPath)) return;
  const off =
    String(process.env.REPORT_ALIGN_REFER_DOCTOR_LINES || 'true').toLowerCase() === 'false';
  if (off) return;

  try {
    const zip = new PizZip(fs.readFileSync(docxPath));
    const docFile = zip.file('word/document.xml');
    if (!docFile) return;
    const before = docFile.asText();
    const after = applyReferDoctorAlignmentToDocXml(before);
    if (after !== before) {
      zip.file('word/document.xml', after);
      fs.writeFileSync(docxPath, zip.generate({ type: 'nodebuffer' }));
      logger.info('Applied REPORT_ALIGN_REFER_DOCTOR_LINES (jc=right) to document.xml', {
        file: path.basename(docxPath),
      });
    }
  } catch (e) {
    logger.warn(`applyReferDoctorAlignment skipped: ${e.message}`);
  }
}

function stripImagePlaceholderTokensInDocXml(docXml, tokens) {
  let out = docXml;
  for (const token of tokens || []) {
    if (!token) continue;
    // remove whole paragraph that contains token (covers split-runs case)
    out = replaceParagraphContainingToken(
      out,
      token,
      '<w:p><w:r><w:t xml:space="preserve"></w:t></w:r></w:p>',
    );
  }
  return out;
}

function stripImagePlaceholderTokensInDocxPath(docxPath, tokens) {
  if (!docxPath || !fs.existsSync(docxPath) || !tokens || !tokens.length) return;
  try {
    const zip = new PizZip(fs.readFileSync(docxPath));
    const docFile = zip.file('word/document.xml');
    if (!docFile) return;
    const before = docFile.asText();
    const after = stripImagePlaceholderTokensInDocXml(before, tokens);
    if (after !== before) {
      zip.file('word/document.xml', after);
      fs.writeFileSync(docxPath, zip.generate({ type: 'nodebuffer' }));
      logger.info('Stripped __IMG_* placeholder tokens from document.xml', {
        file: path.basename(docxPath),
      });
    }
  } catch (e) {
    logger.warn(`stripImagePlaceholderTokens skipped: ${e.message}`);
  }
}

function classifyParagraphJc(plainNorm, linesFromBr) {
  const referJc = classifyReferDoctorParagraphJc(plainNorm, linesFromBr);
  if (referJc) return referJc;

  const doctorRight =
    String(process.env.REPORT_ALIGN_DOCTOR_RIGHT || 'true').toLowerCase() !== 'false';
  const dateRight =
    String(process.env.REPORT_ALIGN_DATE_RIGHT || 'true').toLowerCase() !== 'false';

  if (doctorRight && linesFromBr && linesFromBr.length) {
    if (linesFromBr.some((ln) => lineLooksLikeDoctorSignature(ln))) {
      return 'right';
    }
  }

  const t = plainNorm;
  if (doctorRight && dateRight && t && /^\d{1,2}\/\d{1,2}\/\d{4}$/.test(String(t).trim())) {
    return 'right';
  }

  if (doctorRight && t) {
    if (DOCTOR_LINE_RE.test(t)) {
      return 'right';
    }
    if (t.length < 400 && /\d{1,2}\/\d{1,2}\/\d{4}\s+BS\./i.test(t)) {
      return 'right';
    }
  }

  if (!t) return null;
  if (t.length > 220) return null;

  const a = stripVnAccents(t);
  const headingJc = closingHeadingJcFromEnv();
  if (headingJc && isClosingHeadingPattern(t, a)) {
    return headingJc;
  }

  return null;
}

function setParagraphJcSafe(paraXml, jcVal) {
  if (!jcVal || !paraXml) return paraXml;
  if (/<w:jc\b[^>]*w:val="/i.test(paraXml)) {
    return paraXml.replace(
      /(<w:jc\b[^>]*w:val=")[^"]*(")/i,
      `$1${jcVal}$2`,
    );
  }
  if (/<w:jc\b[^>]*\/>/.test(paraXml)) {
    return paraXml.replace(/<w:jc\b[^>]*\/>/i, `<w:jc w:val="${jcVal}"/>`);
  }
  if (/<w:pPr\b[^>]*>/.test(paraXml)) {
    return paraXml.replace(
      /<w:pPr\b([^>]*)>/,
      `<w:pPr$1><w:jc w:val="${jcVal}"/>`,
    );
  }
  return paraXml.replace(
    /<w:p\b([^>]*)>/,
    `<w:p$1><w:pPr><w:jc w:val="${jcVal}"/></w:pPr>`,
  );
}

function applyClinicalClosingAlignmentToDocXml(docXml) {
  const off =
    String(process.env.REPORT_ALIGN_CLOSING_BLOCK || 'false').toLowerCase() ===
    'false';
  if (off || !docXml) return docXml;

  OOXML_PARA_REGEX.lastIndex = 0;
  let out = '';
  let lastIndex = 0;
  let m;
  while ((m = OOXML_PARA_REGEX.exec(docXml)) !== null) {
    const full = m[0];
    const withBreaks = paragraphPlainTextWithLineBreaks(full);
    const linesFromBr = withBreaks
      .split(/\r?\n/)
      .map((ln) => ln.replace(/\s+/g, ' ').trim())
      .filter(Boolean);
    const plain = normalizeParaTextForAlignment(
      linesFromBr.length ? linesFromBr.join(' ') : paragraphPlainText(full),
    );
    const jc = classifyParagraphJc(plain, linesFromBr);
    const replacement = jc ? setParagraphJcSafe(full, jc) : full;
    out += docXml.slice(lastIndex, m.index) + replacement;
    lastIndex = m.index + full.length;
  }
  out += docXml.slice(lastIndex);
  return out;
}

function applyClinicalClosingAlignmentToDocxPath(docxPath) {
  if (!docxPath || !fs.existsSync(docxPath)) return;
  const off =
    String(process.env.REPORT_ALIGN_CLOSING_BLOCK || 'false').toLowerCase() ===
    'false';
  if (off) return;

  try {
    const zip = new PizZip(fs.readFileSync(docxPath));
    const docFile = zip.file('word/document.xml');
    if (!docFile) return;
    const before = docFile.asText();
    const after = applyClinicalClosingAlignmentToDocXml(before);
    if (after !== before) {
      zip.file('word/document.xml', after);
      fs.writeFileSync(docxPath, zip.generate({ type: 'nodebuffer' }));
      logger.info('Applied REPORT_ALIGN closing jc to document.xml', {
        file: path.basename(docxPath),
      });
    }
  } catch (e) {
    logger.warn(`applyClinicalClosingAlignment skipped: ${e.message}`);
  }
}

/**
 * Một số template fallback (vd. XrayResultTemplate.doc) không có <<Result>>/<<Conclusion>>.
 * Aspose trên server tạo bookmark cuối document khi không tìm thấy placeholder — ta chèn đoạn chứa
 * token __RTF_* trước w:sectPr để injectRtfIntoDocx vẫn chạy.
 */
function insertXmlBeforeBodySectPr(docXml, innerFragmentsXml) {
  const sectIdx = docXml.lastIndexOf('<w:sectPr');
  if (sectIdx >= 0) {
    return docXml.slice(0, sectIdx) + innerFragmentsXml + docXml.slice(sectIdx);
  }
  const bodyEnd = docXml.lastIndexOf('</w:body>');
  if (bodyEnd < 0) return docXml;
  return docXml.slice(0, bodyEnd) + innerFragmentsXml + docXml.slice(bodyEnd);
}

function ensureRtfPlaceholderParagraphsInDocx(renderedDocxPath, tokenToRtf) {
  if (!tokenToRtf || !Object.keys(tokenToRtf).length) return;

  const zip = new PizZip(fs.readFileSync(renderedDocxPath));
  const docFile = zip.file('word/document.xml');
  if (!docFile) return;

  let docXml = docFile.asText();
  const fragments = [];

  for (const [token, rtfText] of Object.entries(tokenToRtf)) {
    if (!rtfText || !String(rtfText).trim()) continue;
    if (docContainsTokenInParagraphs(docXml, token)) continue;
    fragments.push(
      `<w:p><w:r><w:t xml:space="preserve">${token}</w:t></w:r></w:p>`,
    );
    logger.warn(
      `Template missing RTF token ${token} — inserting paragraph before sectPr (fallback, same idea as Aspose EnsureBookmarkExists).`,
    );
  }

  if (!fragments.length) return;

  docXml = insertXmlBeforeBodySectPr(docXml, fragments.join(''));
  zip.file('word/document.xml', docXml);
  fs.writeFileSync(renderedDocxPath, zip.generate({ type: 'nodebuffer' }));
}

async function injectRtfIntoDocx(renderedDocxPath, tokenToRtf, tempDirForRtf) {
  if (!tokenToRtf || !Object.keys(tokenToRtf).length) return;
  ensureDirectoryExists(tempDirForRtf);

  // Open rendered docx zip and read main document.xml
  const renderedZip = new PizZip(fs.readFileSync(renderedDocxPath));
  const mainDocXmlFile = renderedZip.file('word/document.xml');
  if (!mainDocXmlFile) {
    throw new Error(`Missing word/document.xml in ${renderedDocxPath}`);
  }
  let mainDocXml = mainDocXmlFile.asText();

  for (const [token, rtfText] of Object.entries(tokenToRtf)) {
    if (!rtfText) continue;
    if (!docContainsTokenInParagraphs(mainDocXml, token)) {
      if (reportLogRtfMetaEnabled()) {
        logger.warn(`RTF inject skipped — token not found in document.xml: ${token}`);
      }
      continue;
    }

    const rtfPath = path.join(tempDirForRtf, `${token}.rtf`);
    writeRtfFileForOffice(rtfPath, rtfText);

    const outDir = path.join(tempDirForRtf, `rtf_${token}_docx`);
    ensureDirectoryExists(outDir);

    await convertWithLibreOffice('docx', rtfPath, outDir);

    // LibreOffice outputs <basename>.docx into outDir
    const docxPath = path.join(outDir, `${path.parse(rtfPath).name}.docx`);
    if (!fs.existsSync(docxPath)) {
      throw new Error(`LibreOffice did not produce docx from rtf: ${docxPath}`);
    }

    const convZip = new PizZip(fs.readFileSync(docxPath));
    const convDocFile = convZip.file('word/document.xml');
    if (!convDocFile) {
      throw new Error(`Missing converted word/document.xml for token ${token}`);
    }

    // Merge numbering/styles so bullet & indentation from converted RTF render correctly.
    // Without these, LibreOffice-created <w:numPr> references can point to missing definitions.
    for (const f of ['word/numbering.xml', 'word/styles.xml']) {
      const convF = convZip.file(f);
      if (convF) {
        renderedZip.file(f, convF.asText());
      }
    }

    const convDocXml = convDocFile.asText();
    let replacementInnerXml = extractWordDocumentXmlInnerBody(convDocXml);
    replacementInnerXml = sanitizeInjectedRtfParagraphs(replacementInnerXml);

    // Replace paragraph containing token with paragraphs from converted rtf.
    mainDocXml = replaceParagraphContainingToken(mainDocXml, token, replacementInnerXml);
    if (reportLogRtfMetaEnabled()) {
      logger.info(`RTF inject merged token ${token}`, {
        rtfChars: String(rtfText || '').length,
        replacementParasApprox: (replacementInnerXml.match(/<w:p\b/g) || []).length,
      });
    }
  }

  renderedZip.file('word/document.xml', mainDocXml);
  fs.writeFileSync(renderedDocxPath, renderedZip.generate({ type: 'nodebuffer' }));
}

function getPngSize(buf) {
  // PNG IHDR: width/height at bytes 16-23 big-endian
  if (buf.length < 24) return null;
  const sig = buf.subarray(0, 8).toString('hex');
  if (sig !== '89504e470d0a1a0a') return null;
  return { width: buf.readUInt32BE(16), height: buf.readUInt32BE(20) };
}

function getJpegSize(buf) {
  // Minimal JPEG SOF parser
  if (buf.length < 4 || buf[0] !== 0xff || buf[1] !== 0xd8) return null;
  let i = 2;
  while (i + 9 < buf.length) {
    if (buf[i] !== 0xff) {
      i += 1;
      continue;
    }
    const marker = buf[i + 1];
    // SOF0..SOF3, SOF5..SOF7, SOF9..SOF11, SOF13..SOF15
    const isSof =
      (marker >= 0xc0 && marker <= 0xc3) ||
      (marker >= 0xc5 && marker <= 0xc7) ||
      (marker >= 0xc9 && marker <= 0xcb) ||
      (marker >= 0xcd && marker <= 0xcf);
    const len = buf.readUInt16BE(i + 2);
    if (isSof && i + 2 + len <= buf.length) {
      const height = buf.readUInt16BE(i + 5);
      const width = buf.readUInt16BE(i + 7);
      return { width, height };
    }
    if (len < 2) break;
    i += 2 + len;
  }
  return null;
}

function readImageSize(imagePath) {
  const buf = fs.readFileSync(imagePath);
  return getPngSize(buf) || getJpegSize(buf) || null;
}

function fitSizeToBox(w, h, maxW, maxH) {
  if (!w || !h) return { w: maxW, h: maxH };
  const s = Math.min(maxW / w, maxH / h, 1);
  return { w: Math.round(w * s), h: Math.round(h * s) };
}

function nextRelIdFromRels(relsXml) {
  const ids = [...relsXml.matchAll(/Id=\"rId(\d+)\"/g)].map((m) => Number(m[1]));
  const max = ids.length ? Math.max(...ids) : 0;
  return `rId${max + 1}`;
}

function addImageRelationship(relsXml, rid, target) {
  const rel = `<Relationship Id="${rid}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/image" Target="${target}"/>`;
  const insertAt = relsXml.lastIndexOf('</Relationships>');
  if (insertAt < 0) throw new Error('Invalid rels xml');
  return relsXml.slice(0, insertAt) + rel + relsXml.slice(insertAt);
}

function buildInlineImageParagraphXml(rid, cx, cy) {
  // cx/cy in EMU
  const docPrId = Math.floor(Math.random() * 100000) + 1;
  return (
    `<w:p>` +
    `<w:pPr><w:jc w:val="center"/></w:pPr>` +
    `<w:r>` +
    `<w:drawing>` +
    `<wp:inline distT="0" distB="0" distL="0" distR="0">` +
    `<wp:extent cx="${cx}" cy="${cy}"/>` +
    `<wp:docPr id="${docPrId}" name="Picture${docPrId}"/>` +
    `<a:graphic xmlns:a="http://schemas.openxmlformats.org/drawingml/2006/main">` +
    `<a:graphicData uri="http://schemas.openxmlformats.org/drawingml/2006/picture">` +
    `<pic:pic xmlns:pic="http://schemas.openxmlformats.org/drawingml/2006/picture">` +
    `<pic:nvPicPr><pic:cNvPr id="0" name="image"/><pic:cNvPicPr/></pic:nvPicPr>` +
    `<pic:blipFill>` +
    `<a:blip r:embed="${rid}" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"/>` +
    `<a:stretch><a:fillRect/></a:stretch>` +
    `</pic:blipFill>` +
    `<pic:spPr>` +
    `<a:xfrm><a:off x="0" y="0"/><a:ext cx="${cx}" cy="${cy}"/></a:xfrm>` +
    `<a:prstGeom prst="rect"><a:avLst/></a:prstGeom>` +
    `</pic:spPr>` +
    `</pic:pic>` +
    `</a:graphicData>` +
    `</a:graphic>` +
    `</wp:inline>` +
    `</w:drawing>` +
    `</w:r>` +
    `</w:p>`
  );
}

const LOGO_CANDIDATES = ['logo.jpg', 'logo.jpeg', 'logo.png'];

/** Chèn fragment ngay sau mở body (sau </w:bodyPr> nếu có). */
function insertXmlAfterBodyOpen(docXml, fragment) {
  const closeBodyPr = '</w:bodyPr>';
  const i = docXml.indexOf(closeBodyPr);
  if (i >= 0) {
    const pos = i + closeBodyPr.length;
    return docXml.slice(0, pos) + fragment + docXml.slice(pos);
  }
  // LibreOffice/Word thường dùng <w:body w:rsidR="..."> — không khớp chuỗi '<w:body>' cố định
  const m = docXml.match(/<w:body\b[^>]*>/);
  if (!m) {
    logger.warn('insertLogo: no <w:body> tag found in document.xml — logo not inserted');
    return docXml;
  }
  const pos = m.index + m[0].length;
  return docXml.slice(0, pos) + fragment + docXml.slice(pos);
}

/** Trả về đường dẫn tuyệt đối tới logo hoặc null (thử jpg/jpeg/png). */
function resolveLogoFileOnDisk(templateBasePath) {
  const envLogo = process.env.PATHS_LOGO || process.env.LOGO_PATH;
  if (envLogo && String(envLogo).trim()) {
    const p0 = String(envLogo).trim();
    const abs = path.isAbsolute(p0) ? p0 : path.resolve(process.cwd(), p0);
    try {
      if (fs.existsSync(abs)) {
        const st = fs.statSync(abs);
        if (st.isFile()) return abs;
        if (st.isDirectory()) {
          for (const name of LOGO_CANDIDATES) {
            const p = path.join(abs, name);
            if (fs.existsSync(p)) return p;
          }
        }
      }
    } catch (_) {
      // ignore
    }
  }

  if (!templateBasePath) return null;
  for (const name of LOGO_CANDIDATES) {
    const p = path.join(templateBasePath, name);
    if (fs.existsSync(p)) return p;
  }
  return null;
}

function buildLogoParagraphXml(rid, cx, cy) {
  const docPrId = Math.floor(Math.random() * 100000) + 1;
  return (
    `<w:p>` +
    `<w:pPr><w:jc w:val="center"/><w:spacing w:before="0" w:after="0"/></w:pPr>` +
    `<w:r>` +
    `<w:drawing>` +
    `<wp:inline distT="0" distB="0" distL="0" distR="0">` +
    `<wp:extent cx="${cx}" cy="${cy}"/>` +
    `<wp:docPr id="${docPrId}" name="Logo${docPrId}"/>` +
    `<a:graphic xmlns:a="http://schemas.openxmlformats.org/drawingml/2006/main">` +
    `<a:graphicData uri="http://schemas.openxmlformats.org/drawingml/2006/picture">` +
    `<pic:pic xmlns:pic="http://schemas.openxmlformats.org/drawingml/2006/picture">` +
    `<pic:nvPicPr><pic:cNvPr id="0" name="logo"/><pic:cNvPicPr/></pic:nvPicPr>` +
    `<pic:blipFill>` +
    `<a:blip r:embed="${rid}" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"/>` +
    `<a:stretch><a:fillRect/></a:stretch>` +
    `</pic:blipFill>` +
    `<pic:spPr>` +
    `<a:xfrm><a:off x="0" y="0"/><a:ext cx="${cx}" cy="${cy}"/></a:xfrm>` +
    `<a:prstGeom prst="rect"><a:avLst/></a:prstGeom>` +
    `</pic:spPr>` +
    `</pic:pic>` +
    `</a:graphicData>` +
    `</a:graphic>` +
    `</wp:inline>` +
    `</w:drawing>` +
    `</w:r>` +
    `</w:p>`
  );
}

/** Đoạn chỉ có khoảng trắng / không có chữ — thường gây khoảng trắng dọc lớn sau khi LibreOffice chuyển RTF. */
function isParagraphXmlEffectivelyEmpty(pXml) {
  if (!pXml) return false;
  if (
    pXml.includes('<w:drawing') ||
    pXml.includes('<w:object') ||
    pXml.includes('<w:pict') ||
    pXml.includes('<w:tbl') ||
    pXml.includes('<w:hyperlink')
  ) {
    return false;
  }
  const plain = paragraphPlainText(pXml);
  return plain.replace(/\s+/g, '').length === 0;
}

/**
 * Bỏ đoạn trống đầu/cuối và gộp nhiều đoạn trống liên tiếp (từ body RTF merge).
 * Tắt: REPORT_SANITIZE_RTF_WHITESPACE=false
 * Số đoạn trống tối đa liên tiếp giữ lại: REPORT_RTF_MAX_CONSECUTIVE_EMPTY_PARAS (mặc định 2)
 */
function sanitizeInjectedRtfParagraphs(innerXml) {
  if (
    String(process.env.REPORT_SANITIZE_RTF_WHITESPACE || 'true').toLowerCase() === 'false'
  ) {
    return innerXml;
  }
  if (!innerXml || !innerXml.trim()) return innerXml;

  const parsed = parseInt(process.env.REPORT_RTF_MAX_CONSECUTIVE_EMPTY_PARAS || '2', 10);
  const maxRun = Number.isFinite(parsed) ? Math.max(1, Math.min(10, parsed)) : 2;

  const paras = [];
  OOXML_PARA_REGEX.lastIndex = 0;
  let m;
  while ((m = OOXML_PARA_REGEX.exec(innerXml)) !== null) {
    paras.push(m[0]);
  }
  if (!paras.length) return innerXml;

  while (paras.length && isParagraphXmlEffectivelyEmpty(paras[0])) paras.shift();
  while (paras.length && isParagraphXmlEffectivelyEmpty(paras[paras.length - 1])) paras.pop();

  const out = [];
  let emptyRun = 0;
  for (const p of paras) {
    if (isParagraphXmlEffectivelyEmpty(p)) {
      emptyRun += 1;
      if (emptyRun <= maxRun) out.push(p);
    } else {
      emptyRun = 0;
      out.push(p);
    }
  }
  return out.join('');
}

function removeConsecutiveEmptyParagraphsAfter(docXml, afterIdx, maxRemove = 6) {
  let out = docXml;
  let idx = afterIdx;
  let removed = 0;
  while (removed < maxRemove) {
    const start = out.indexOf('<w:p', idx);
    if (start < 0) break;
    const end = out.indexOf('</w:p>', start);
    if (end < 0) break;
    const endTag = end + '</w:p>'.length;
    const pXml = out.slice(start, endTag);
    if (!isParagraphXmlEffectivelyEmpty(pXml)) break;
    out = out.slice(0, start) + out.slice(endTag);
    idx = start;
    removed += 1;
  }
  return out;
}

/** Sau đoạn logo: chỉ giữ đúng một đoạn trống (khoảng cách) trước nội dung tiếp theo. */
function normalizeSpacingAfterLogoParagraph(docXml, logoParaXml) {
  const pos = docXml.indexOf(logoParaXml);
  if (pos < 0) return docXml;
  const afterLogo = pos + logoParaXml.length;
  const rest = docXml.slice(afterLogo);
  const emptyRanges = [];
  let offset = 0;
  while (true) {
    const start = rest.indexOf('<w:p', offset);
    if (start < 0) break;
    const end = rest.indexOf('</w:p>', start);
    if (end < 0) break;
    const endTag = end + '</w:p>'.length;
    const pXml = rest.slice(start, endTag);
    if (!isParagraphXmlEffectivelyEmpty(pXml)) break;
    emptyRanges.push({ start: afterLogo + start, end: afterLogo + endTag });
    offset = endTag;
  }
  const spacer =
    '<w:p><w:pPr><w:spacing w:before="0" w:after="0" w:line="240" w:lineRule="auto"/></w:pPr></w:p>';
  if (emptyRanges.length === 0) {
    return docXml.slice(0, afterLogo) + spacer + docXml.slice(afterLogo);
  }
  if (emptyRanges.length === 1) {
    return docXml;
  }
  const first = emptyRanges[0].start;
  const last = emptyRanges[emptyRanges.length - 1].end;
  return docXml.slice(0, first) + spacer + docXml.slice(last);
}

const KHAM_SUC_KHOE_BINH_THUONG = 'KhamSucKhoe (BinhThuong).doc';

/**
 * Chèn logo chỉ cho template KhamSucKhoe (BinhThuong).doc — đặt logo.jpg trong thư mục Templates (PATHS_TEMPLATES).
 * @param {string} templateFileBasename — basename file gốc, ví dụ KhamSucKhoe (BinhThuong).doc
 */
function insertLogoIntoDocxIfExists(docxPath, templateBasePath, templateFileBasename) {
  if (!docxPath || !templateBasePath) return false;
  if (String(templateFileBasename || '').trim() !== KHAM_SUC_KHOE_BINH_THUONG) {
    return false;
  }
  const logoPath = resolveLogoFileOnDisk(templateBasePath);
  if (!logoPath) {
    logger.info(
      `No logo file (tried ${LOGO_CANDIDATES.join(', ')}) in ${path.resolve(templateBasePath)} — skip (KhamSucKhoe)`,
    );
    return false;
  }
  try {
    const zip = new PizZip(fs.readFileSync(docxPath));
    const docFile = zip.file('word/document.xml');
    const relsFile = zip.file('word/_rels/document.xml.rels');
    if (!docFile || !relsFile) {
      logger.warn('insertLogo: missing word/document.xml or word/_rels/document.xml.rels');
      return false;
    }
    let docXml = docFile.asText();
    let relsXml = relsFile.asText();

    if (docXml.includes('name="logo"') || docXml.includes('name="Logo')) {
      logger.info(`insertLogo: logo marker already exists in ${path.basename(docxPath)} — skip`);
      return true;
    }

    const logoExt = path.extname(logoPath).toLowerCase() || '.jpg';
    const mediaName = `logo_${crypto.randomBytes(4).toString('hex')}${logoExt}`;
    const mediaTarget = `media/${mediaName}`;
    const rid = nextRelIdFromRels(relsXml);
    relsXml = addImageRelationship(relsXml, rid, mediaTarget);
    zip.file(`word/${mediaTarget}`, fs.readFileSync(logoPath));
    const size = readImageSize(logoPath) || { width: 800, height: 200 };
    const maxWRaw = parseInt(process.env.REPORT_LOGO_BOX_W || '560', 10);
    const maxHRaw = parseInt(process.env.REPORT_LOGO_BOX_H || '140', 10);
    const maxW = Number.isFinite(maxWRaw) ? Math.max(120, Math.min(1200, maxWRaw)) : 560;
    const maxH = Number.isFinite(maxHRaw) ? Math.max(40, Math.min(600, maxHRaw)) : 140;
    const fitted = fitSizeToBox(size.width, size.height, maxW, maxH);
    const EMU_PER_PX = 9525;
    const cx = fitted.w * EMU_PER_PX;
    const cy = fitted.h * EMU_PER_PX;
    const logoPara = buildLogoParagraphXml(rid, cx, cy);
    docXml = insertXmlAfterBodyOpen(docXml, logoPara);
    docXml = normalizeSpacingAfterLogoParagraph(docXml, logoPara);

    zip.file('word/document.xml', docXml);
    zip.file('word/_rels/document.xml.rels', relsXml);
    fs.writeFileSync(docxPath, zip.generate({ type: 'nodebuffer' }));
    logger.info(`Inserted logo from ${path.basename(logoPath)} into ${path.basename(docxPath)} (KhamSucKhoe)`);
    return true;
  } catch (e) {
    logger.warn(`Failed to insert logo: ${e.message}`);
    return false;
  }
}

function relsPathForWordPart(partName) {
  const n = partName.replace(/\\/g, '/');
  const base = path.posix.basename(n);
  return `word/_rels/${base}.rels`;
}

function listEmbedCandidateXmlParts(zip) {
  const out = [];
  for (const name of Object.keys(zip.files)) {
    const entry = zip.files[name];
    if (!entry || entry.dir) continue;
    const n = name.replace(/\\/g, '/');
    if (
      /^word\/document\.xml$/i.test(n) ||
      /^word\/header\d+\.xml$/i.test(n) ||
      /^word\/footer\d+\.xml$/i.test(n) ||
      /^word\/footnotes\.xml$/i.test(n) ||
      /^word\/endnotes\.xml$/i.test(n)
    ) {
      out.push(n);
    }
  }
  return out.sort();
}

const EMPTY_RELS_XML =
  '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>' +
  '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"></Relationships>';

/**
 * Một số template gõ nhầm `___IMG_1___` (ba gạch); code chỉ thay `__IMG_1__`.
 * Chuẩn hóa trong document/header/footer trước khi embed và in PDF.
 */
function normalizeImagePlaceholderTokensInDocx(docxPath) {
  if (!docxPath || !fs.existsSync(docxPath)) return;
  if (String(process.env.REPORT_NORMALIZE_IMG_TOKENS || 'true').toLowerCase() === 'false') {
    return;
  }
  try {
    const zip = new PizZip(fs.readFileSync(docxPath));
    let changed = false;
    for (const name of Object.keys(zip.files)) {
      const entry = zip.files[name];
      if (!entry || entry.dir) continue;
      const n = name.replace(/\\/g, '/');
      if (
        n !== 'word/document.xml' &&
        !/^word\/header\d+\.xml$/i.test(n) &&
        !/^word\/footer\d+\.xml$/i.test(n) &&
        n !== 'word/footnotes.xml' &&
        n !== 'word/endnotes.xml'
      ) {
        continue;
      }
      let xml = zip.file(name).asText();
      const patched = xml.replace(/___IMG_(\d+)___/g, '__IMG_$1__');
      if (patched !== xml) {
        zip.file(name, patched);
        changed = true;
      }
    }
    if (changed) {
      fs.writeFileSync(docxPath, zip.generate({ type: 'nodebuffer' }));
    }
  } catch (e) {
    logger.warn(`normalizeImagePlaceholderTokensInDocx skipped: ${e.message}`);
  }
}

async function embedImagesIntoDocx(renderedDocxPath, tokenToImagePath) {
  const zip = new PizZip(fs.readFileSync(renderedDocxPath));
  if (!zip.file('word/document.xml')) {
    return { embedded: 0, embeddedPaths: [] };
  }

  let embedded = 0;
  const embeddedPaths = [];
  const parts = listEmbedCandidateXmlParts(zip);

  for (const [token, imagePath] of Object.entries(tokenToImagePath || {})) {
    if (!imagePath || !fs.existsSync(imagePath)) continue;

    let partName = null;
    let partXml = '';
    for (const candidate of parts) {
      const xml = zip.file(candidate).asText();
      if (docContainsTokenInParagraphs(xml, token)) {
        partName = candidate;
        partXml = xml;
        break;
      }
    }
    if (!partName) {
      logger.warn(
        `Embed: không tìm thấy token trong document/header/footer/footnotes/endnotes — ${token} (file=${path.basename(renderedDocxPath)})`,
      );
      continue;
    }

    const relsKey = relsPathForWordPart(partName);
    let relsXml = zip.file(relsKey)?.asText();
    if (!relsXml) {
      relsXml = EMPTY_RELS_XML;
    }

    const ext = path.extname(imagePath).toLowerCase() || '.jpg';
    const mediaName = `img_${crypto.randomBytes(6).toString('hex')}${ext}`;
    const mediaTarget = `media/${mediaName}`;
    const rid = nextRelIdFromRels(relsXml);
    relsXml = addImageRelationship(relsXml, rid, mediaTarget);

    const imgBytes = fs.readFileSync(imagePath);
    zip.file(`word/${mediaTarget}`, imgBytes);

    const size = readImageSize(imagePath) || { width: 800, height: 600 };
    const fitted = fitSizeToBox(size.width, size.height, 480, 360);
    const EMU_PER_PX = 9525;
    const cx = fitted.w * EMU_PER_PX;
    const cy = fitted.h * EMU_PER_PX;

    const paraXml = buildInlineImageParagraphXml(rid, cx, cy);
    partXml = replaceParagraphContainingToken(partXml, token, paraXml);
    zip.file(partName, partXml);
    zip.file(relsKey, relsXml);
    embedded += 1;
    embeddedPaths.push(path.resolve(imagePath));
  }

  fs.writeFileSync(renderedDocxPath, zip.generate({ type: 'nodebuffer' }));
  return { embedded, embeddedPaths };
}

function cnFilesAttachKeywordMatch(record) {
  const blob = `${record.templateFile || ''} ${record.pathologyType || ''} ${record.conclusion || ''}`.toLowerCase();
  const raw =
    process.env.REPORT_CN_FILES_ATTACH_KEYWORDS ||
    'ecg,pap,điện tim,dien tim,electrocardiogram,rest-ecg,siêu âm mạch máu chi';
  const keys = raw.split(/[,;|]/).map((s) => s.trim().toLowerCase()).filter(Boolean);
  for (const k of keys) {
    if (!k) continue;
    if (blob.includes(k)) return true;
    const nk = stripVnAccents(k);
    const nb = stripVnAccents(blob);
    if (nk && nb.includes(nk)) return true;
  }
  return false;
}

/**
 * Ảnh từ CN_FILES (ZIP ECG/PAP) chỉ ghép vào segment phù hợp — không dán vào mọi báo cáo.
 * REPORT_CN_FILES_ATTACH_MODE: last | match | last_or_match | all | never
 */
function shouldUseCnFilesMediaForSegment(ctx, record, segmentIndex) {
  const mode = String(process.env.REPORT_CN_FILES_ATTACH_MODE || 'last').toLowerCase();
  if (mode === 'never' || mode === 'off' || mode === 'false') return false;
  const total = Math.max(1, Number(ctx.reportSegmentCount) || 1);
  const idx = Number(segmentIndex);
  if (mode === 'all') return true;
  if (mode === 'match' || mode === 'keywords' || mode === 'ecg_pap') {
    return cnFilesAttachKeywordMatch(record);
  }
  if (mode === 'last_or_match') {
    return idx === total - 1 || cnFilesAttachKeywordMatch(record);
  }
  return idx === total - 1;
}

/**
 * One imaging record → one PDF buffer, hoặc null nếu không có template trên disk.
 */
async function renderRecordToPdf(record, segmentIndex, tempDir, ctx) {
  const t0 = timingEnabled() ? nowMs() : 0;
  const { fileCopyHelper, templateSelector } = ctx;
  const richMode = richTextMode();

  const extractDir = path.join(tempDir, `extract_${record.imagingResultId}_${segmentIndex}`);
  ensureDirectoryExists(extractDir);

  let zipPath = null;
  if (record.fileName) {
    zipPath = await fileCopyHelper.copyFileWithFallback(record.fileName);
    if (zipPath && typeof ctx.trackLocalMediaPath === 'function') {
      try {
        ctx.trackLocalMediaPath(zipPath);
      } catch (_) {
        // ignore
      }
    }
    if (!zipPath) {
      logger.warn(`Media not found for ItemNum=${record.itemNum}, FileName=${record.fileName}`);
    }
  }

  let extractedFiles = [];
  if (zipPath && fs.existsSync(zipPath)) {
    try {
      extractedFiles = extractImagesFromArchiveOrRaw(zipPath, extractDir);
    } catch (ex) {
      logger.warn(`Cannot read imaging media file ${zipPath}: ${ex.message}`);
    }
  }

  const printedNames = await getPrintedImageFilenames(record.imagingResultId);
  const cnPool = Array.isArray(ctx.cnFilesMediaPaths) ? ctx.cnFilesMediaPaths : [];
  const useCnFilesThisSegment = shouldUseCnFilesMediaForSegment(ctx, record, segmentIndex);
  function combinedMediaPool() {
    return extractedFiles.concat(useCnFilesThisSegment ? cnPool : []);
  }
  function findMediaInPool(pool, wantFile, wantNoExt) {
    return pool.find((f) => {
      const fFile = path.basename(f);
      if (String(fFile).toLowerCase() === wantFile.toLowerCase()) return true;
      const fNoExt = path.parse(fFile).name.toLowerCase();
      return fNoExt === wantNoExt;
    });
  }

  let imageFiles = [];
  const imageFilesSet = new Set();
  for (const filename of printedNames) {
    const wantRaw = String(filename).trim();
    if (!wantRaw) continue;

    const wantFile = path.basename(wantRaw);
    const wantNoExt = path.parse(wantFile).name.toLowerCase();

    let abs = findMediaInPool(combinedMediaPool(), wantFile, wantNoExt);
    if (!abs || !fs.existsSync(abs)) {
      // Resolve từ disk theo basename (ưu tiên jpg/png trước)
      abs = fileCopyHelper.resolveMediaPathOrNull(wantFile);
      if ((!abs || !fs.existsSync(abs)) && wantNoExt) {
        abs = fileCopyHelper.resolveMediaPathOrNull(wantNoExt);
      }
    }
    if (abs && typeof ctx.trackLocalMediaPath === 'function') {
      try {
        ctx.trackLocalMediaPath(abs);
      } catch (_) {
        // ignore
      }
    }

    if (abs && fs.existsSync(abs)) {
      // Nếu resolve ra zip thì extract rồi thử match lại theo want
      if (abs.toLowerCase().endsWith('.zip')) {
        try {
          const extractedFromZip = extractImagesFromArchiveOrRaw(abs, extractDir);
          extractedFiles = extractedFiles.concat(extractedFromZip);
          abs = findMediaInPool(combinedMediaPool(), wantFile, wantNoExt);
        } catch (ex) {
          logger.warn(
            `Failed to extract zip for ImagingResultId=${record.imagingResultId}, zip=${abs}: ${ex.message}`,
          );
        }
      }

      // Chỉ merge ảnh jpg/png (pdf-lib không render bmp/webp trực tiếp).
      const low = abs && fs.existsSync(abs) ? abs.toLowerCase() : '';
      if (low.endsWith('.jpg') || low.endsWith('.jpeg') || low.endsWith('.png')) {
        const ap = abs;
        if (!imageFilesSet.has(ap)) {
          imageFilesSet.add(ap);
          imageFiles.push(ap);
        }
      } else {
        logger.warn(
          `Skip non JPEG/PNG for PDF append (need convert): ${abs} (wanted ${wantRaw})`,
        );
      }
    } else {
      logger.warn(
        `Image not on disk for Filename=${wantRaw} ImagingResultId=${record.imagingResultId}`,
      );
    }
  }

  const appendCnOrphans =
    useCnFilesThisSegment &&
    String(process.env.REPORT_APPEND_UNMATCHED_CN_FILES_IMAGES || 'true').toLowerCase() !==
      'false';
  if (appendCnOrphans && cnPool.length > 0) {
    let appended = 0;
    for (const p of cnPool) {
      const rp = path.resolve(p);
      if (!fs.existsSync(rp)) continue;
      const low = rp.toLowerCase();
      if (!low.endsWith('.jpg') && !low.endsWith('.jpeg') && !low.endsWith('.png')) continue;
      if (imageFilesSet.has(rp)) continue;
      imageFilesSet.add(rp);
      imageFiles.push(rp);
      appended += 1;
    }
    if (appended > 0) {
      logger.info(
        `CN_FILES: appended ${appended} image(s) not listed in Printed (ImagingResultId=${record.imagingResultId})`,
      );
    }
  }

  logger.info(
    `Resolved ${imageFiles.length} image(s) for ImagingResultId=${record.imagingResultId} (printed=${printedNames.length}, CN_FILES pool=${cnPool.length}, useCnPoolThisSegment=${useCnFilesThisSegment})`,
  );
  if (t0) {
    logger.info('Segment timing', {
      step: 'resolve_media',
      imagingResultId: record.imagingResultId,
      segmentIndex,
      richTextMode: richMode,
      imageCount: imageFiles.length,
      durationMs: nowMs() - t0,
    });
  }

  const resultRtf = decompressToString(record.resultData);
  const conclusionRtf = decompressToString(record.conclusionData);
  const suggestionRtf = decompressToString(record.suggestionData);
  logRtfBlobMeta('ResultData', record.resultData, resultRtf);
  logRtfBlobMeta('ConclusionData', record.conclusionData, conclusionRtf);
  logRtfBlobMeta('SuggestionData', record.suggestionData, suggestionRtf);

  const hasBlobText =
    (resultRtf && String(resultRtf).trim()) ||
    (conclusionRtf && String(conclusionRtf).trim()) ||
    (suggestionRtf && String(suggestionRtf).trim());
  const hasPlainConclusion = record.conclusion && String(record.conclusion).trim();

  if (!hasBlobText && !hasPlainConclusion && imageFiles.length === 0) {
    logger.warn(
      `Skip ImagingResultId=${record.imagingResultId}: no Result/Conclusion/Suggestion data, no Conclusion column, no images — skip PDF (SKIP_EMPTY_CONTENT)`,
    );
    return null;
  }

  const templatePath = templateSelector.selectTemplate(
    record.templateFile,
    record.pathologyType,
    imageFiles.length,
  );

  if (!templatePath) {
    logger.warn(
      `Skip ImagingResultId=${record.imagingResultId}: no template (TemplateFile=${record.templateFile || ''}, PathologyType=${record.pathologyType})`,
    );
    return null;
  }

  const imageLimit = templateSelector.getImageLimit(templatePath);
  if (imageFiles.length > imageLimit) {
    logger.warn(`Image count ${imageFiles.length} exceeds limit ${imageLimit}, truncating`);
    imageFiles = imageFiles.slice(0, imageLimit);
  }

  const stagedTemplate = path.join(tempDir, `tpl_${segmentIndex}_${path.basename(templatePath)}`);
  fs.copyFileSync(templatePath, stagedTemplate);
  validateWordTemplateFile(stagedTemplate);

  try {
    const st0 = fs.statSync(stagedTemplate);
    logger.info('Template staged', {
      imagingResultId: record.imagingResultId,
      segmentIndex,
      templatePath,
      stagedTemplate,
      size: st0.size,
    });
    if (st0.size <= 0) {
      throw new Error(`Staged template is empty: ${stagedTemplate}`);
    }
  } catch (e) {
    throw new Error(
      `Template staging failed for ImagingResultId=${record.imagingResultId}: ${e?.message || String(e)}`,
    );
  }

  const tTpl0 = timingEnabled() ? nowMs() : 0;
  const stagedExt = path.extname(stagedTemplate).toLowerCase();
  let templateDocxPath = stagedTemplate;
  if (stagedExt !== '.docx') {
    await convertWithOffice('docx', stagedTemplate, tempDir);
    templateDocxPath = path.join(tempDir, `${path.parse(stagedTemplate).name}.docx`);
    if (!fs.existsSync(templateDocxPath)) {
      throw new Error(`Office converter could not convert template to DOCX: ${stagedTemplate}`);
    }
  }
  if (tTpl0) {
    logger.info('Segment timing', {
      step: stagedExt === '.docx' ? 'template_docx_reuse' : 'template_doc_to_docx',
      imagingResultId: record.imagingResultId,
      segmentIndex,
      durationMs: nowMs() - tTpl0,
    });
  }
  if (!fs.existsSync(templateDocxPath)) {
    throw new Error(`Template DOCX not found: ${templateDocxPath}`);
  }

  insertLogoIntoDocxIfExists(
    templateDocxPath,
    path.dirname(templatePath),
    path.basename(templatePath),
  );

  const tmpPrefix = path.join(tempDir, `rtf_${record.imagingResultId}_${segmentIndex}`);

  const mode = richMode;
  let resultPlain = '';
  let conclusionPlain = '';
  let suggestionPlain = '';
  if (mode === 'rtf_html_plain') {
    const r0 = resultRtf ? await rtfToHtmlLocal(resultRtf) : { html: '' };
    const c0 = conclusionRtf ? await rtfToHtmlLocal(conclusionRtf) : { html: '' };
    const s0 = suggestionRtf ? await rtfToHtmlLocal(suggestionRtf) : { html: '' };
    resultPlain = htmlToTextLoose(r0.html);
    conclusionPlain = htmlToTextLoose(c0.html);
    suggestionPlain = htmlToTextLoose(s0.html);
    if (timingEnabled()) {
      logger.info('Segment timing', {
        step: 'rtf_to_html_local',
        imagingResultId: record.imagingResultId,
        segmentIndex,
        resultMs: r0.durationMs || 0,
        conclusionMs: c0.durationMs || 0,
        suggestionMs: s0.durationMs || 0,
      });
    }
  } else {
    // Legacy fallback plain-text (used if RTF injection fails)
    resultPlain = resultRtf ? rtfToPlainText(resultRtf, `${tmpPrefix}_result`) : '';
    conclusionPlain = conclusionRtf ? rtfToPlainText(conclusionRtf, `${tmpPrefix}_conclusion`) : '';
    suggestionPlain = suggestionRtf ? rtfToPlainText(suggestionRtf, `${tmpPrefix}_suggestion`) : '';
  }

  // Tokens để docxtemplater render ra vị trí; sau đó ta thay bằng nội dung RTF giữ format.
  const imageTokens = {};
  const tokenToImagePath = {};
  for (let i = 0; i < imageFiles.length; i += 1) {
    const k = `Image${i + 1}`;
    const token = `__IMG_${i + 1}__`;
    imageTokens[k] = token;
    tokenToImagePath[token] = imageFiles[i];
  }
  const tokens = {
    resultToken: mode === 'rtf_html_plain' ? (resultPlain || '') : (resultRtf ? '__RTF_RESULT__' : ''),
    conclusionToken: mode === 'rtf_html_plain' ? (conclusionPlain || '') : (conclusionRtf ? '__RTF_CONCLUSION__' : ''),
    suggestionToken: mode === 'rtf_html_plain' ? (suggestionPlain || '') : (suggestionRtf ? '__RTF_SUGGESTION__' : ''),
    imageTokens,
  };

  const templateDocxBinary = fs.readFileSync(templateDocxPath, 'binary');
  const docZip = new PizZip(templateDocxBinary);
  const doc = new Docxtemplater(docZip, {
    delimiters: { start: '<<', end: '>>' },
    linebreaks: true,
    paragraphLoop: true,
    nullGetter() {
      return '';
    },
  });

  const tRender0 = timingEnabled() ? nowMs() : 0;
  doc.render(buildRenderPayload(record, tokens));
  if (tRender0) {
    logger.info('Segment timing', {
      step: 'docxtemplater_render',
      imagingResultId: record.imagingResultId,
      segmentIndex,
      durationMs: nowMs() - tRender0,
    });
  }

  const baseName = `rendered_${record.imagingResultId}_${segmentIndex}`;
  const renderedDocxPath = path.join(tempDir, `${baseName}.docx`);
  fs.writeFileSync(renderedDocxPath, doc.getZip().generate({ type: 'nodebuffer' }));
  normalizeImagePlaceholderTokensInDocx(renderedDocxPath);

  const tokenToRtf = {};
  if (mode !== 'rtf_html_plain') {
    if (resultRtf) tokenToRtf.__RTF_RESULT__ = resultRtf;
    if (conclusionRtf) tokenToRtf.__RTF_CONCLUSION__ = conclusionRtf;
    if (suggestionRtf) tokenToRtf.__RTF_SUGGESTION__ = suggestionRtf;
  }
  if (Object.keys(tokenToRtf).length) {
    ensureRtfPlaceholderParagraphsInDocx(renderedDocxPath, tokenToRtf);
  }

  const basePdfPath = path.join(tempDir, `${baseName}.pdf`);

  try {
    const tInject0 = timingEnabled() ? nowMs() : 0;
    if (Object.keys(tokenToRtf).length) {
      await injectRtfIntoDocx(
        renderedDocxPath,
        tokenToRtf,
        path.join(tempDir, `rtf_inject_${record.imagingResultId}_${segmentIndex}`),
      );
    }
    if (tInject0) {
      logger.info('Segment timing', {
        step: 'rtf_inject',
        imagingResultId: record.imagingResultId,
        segmentIndex,
        enabled: Object.keys(tokenToRtf).length > 0,
        tokenCount: Object.keys(tokenToRtf).length,
        durationMs: nowMs() - tInject0,
      });
    }

    applyReferDoctorAlignmentToDocxPath(renderedDocxPath);
    applyClinicalClosingAlignmentToDocxPath(renderedDocxPath);

    const tPdf0 = timingEnabled() ? nowMs() : 0;
    await convertWithLibreOffice('pdf', renderedDocxPath, tempDir);
    if (!fs.existsSync(basePdfPath)) {
      throw new Error(`LibreOffice produced no PDF: ${basePdfPath}`);
    }
    if (tPdf0) {
      logger.info('Segment timing', {
        step: 'docx_to_pdf',
        imagingResultId: record.imagingResultId,
        segmentIndex,
        durationMs: nowMs() - tPdf0,
      });
    }
  } catch (e) {
    logger.warn(
      `RTF injection/convert failed for ImagingResultId=${record.imagingResultId} (segment=${segmentIndex}); fallback to plain text: ${e.message}`,
    );

    // Re-render with plain text (always produce a PDF)
    const plainTokens = {
      resultToken: resultPlain || '',
      conclusionToken: conclusionPlain || '',
      suggestionToken: suggestionPlain || '',
    };

    const docPlainZip = new PizZip(templateDocxBinary);
    const docPlain = new Docxtemplater(docPlainZip, {
      delimiters: { start: '<<', end: '>>' },
      linebreaks: true,
      paragraphLoop: true,
      nullGetter() {
        return '';
      },
    });
    docPlain.render(buildRenderPayload(record, plainTokens));
    fs.writeFileSync(renderedDocxPath, docPlain.getZip().generate({ type: 'nodebuffer' }));
    normalizeImagePlaceholderTokensInDocx(renderedDocxPath);
    applyReferDoctorAlignmentToDocxPath(renderedDocxPath);
    applyClinicalClosingAlignmentToDocxPath(renderedDocxPath);
    const tPdfFallback0 = timingEnabled() ? nowMs() : 0;
    await convertWithLibreOffice('pdf', renderedDocxPath, tempDir);
    if (!fs.existsSync(basePdfPath)) {
      throw new Error(`LibreOffice produced no PDF after fallback: ${basePdfPath}`);
    }
    if (tPdfFallback0) {
      logger.info('Segment timing', {
        step: 'docx_to_pdf_fallback_plain',
        imagingResultId: record.imagingResultId,
        segmentIndex,
        durationMs: nowMs() - tPdfFallback0,
      });
    }
  }

  const finalPdfPath = path.join(tempDir, `${baseName}_final.pdf`);
  let embeddedCount = 0;
  let embeddedPaths = [];
  try {
    const emb = await embedImagesIntoDocx(renderedDocxPath, tokenToImagePath);
    embeddedCount = emb.embedded || 0;
    embeddedPaths = emb.embeddedPaths || [];
    if (imageFiles.length > 0 && embeddedCount < imageFiles.length) {
      logger.warn(
        `Chỉ embed ${embeddedCount}/${imageFiles.length} ảnh (file thiếu, hoặc placeholder không khớp <<ImageN>> / __IMG_n__) — ImagingResultId=${record.imagingResultId}`,
      );
    }
  } catch (e) {
    logger.warn(`Embed images into docx failed, will append pages: ${e.message}`);
  }

  const embeddedSet = new Set(embeddedPaths.map((p) => path.resolve(p)));
  const remainingImages = imageFiles.filter((p) => !embeddedSet.has(path.resolve(p)));

  let pdfBaseForAppend = basePdfPath;
  if (embeddedCount > 0) {
    try {
      applyReferDoctorAlignmentToDocxPath(renderedDocxPath);
      applyClinicalClosingAlignmentToDocxPath(renderedDocxPath);
      const tRePdf0 = timingEnabled() ? nowMs() : 0;
      await convertWithLibreOffice('pdf', renderedDocxPath, tempDir);
      if (!fs.existsSync(basePdfPath)) {
        throw new Error(`LibreOffice produced no PDF: ${basePdfPath}`);
      }
      pdfBaseForAppend = basePdfPath;
      if (tRePdf0) {
        logger.info('Segment timing', {
          step: 'docx_to_pdf_after_embed',
          imagingResultId: record.imagingResultId,
          segmentIndex,
          durationMs: nowMs() - tRePdf0,
        });
      }
    } catch (e) {
      logger.warn(
        `Re-convert after embedding failed for ImagingResultId=${record.imagingResultId} (segment=${segmentIndex}). Fallback to appending all image pages. reason=${e?.message || String(e)}`,
      );
      await mergeBasePdfWithImagePages(basePdfPath, imageFiles, finalPdfPath);
      return fs.readFileSync(finalPdfPath);
    }
  }

  // If we are going to append images as pages (no embedded images), remove __IMG_* tokens from the text PDF.
  // This avoids PDFs showing raw tokens like "__IMG_1__" in the report body.
  if (embeddedCount === 0 && remainingImages.length > 0) {
    try {
      const tokens = Object.keys(tokenToImagePath || {});
      stripImagePlaceholderTokensInDocxPath(renderedDocxPath, tokens);
      applyReferDoctorAlignmentToDocxPath(renderedDocxPath);
      applyClinicalClosingAlignmentToDocxPath(renderedDocxPath);
      const tStripPdf0 = timingEnabled() ? nowMs() : 0;
      await convertWithLibreOffice('pdf', renderedDocxPath, tempDir);
      if (fs.existsSync(basePdfPath)) {
        pdfBaseForAppend = basePdfPath;
      }
      if (tStripPdf0) {
        logger.info('Segment timing', {
          step: 'docx_to_pdf_after_strip_img_tokens',
          imagingResultId: record.imagingResultId,
          segmentIndex,
          durationMs: nowMs() - tStripPdf0,
        });
      }
    } catch (e) {
      logger.warn(`Failed to strip __IMG_* tokens before appending pages: ${e?.message || String(e)}`);
    }
  }

  if (remainingImages.length > 0) {
    logger.info(
      `Appending ${remainingImages.length} image page(s) after text PDF (embedded in template=${embeddedCount}, total resolved=${imageFiles.length}) for ImagingResultId=${record.imagingResultId}`,
    );
  }
  await mergeBasePdfWithImagePages(pdfBaseForAppend, remainingImages, finalPdfPath);

  return fs.readFileSync(finalPdfPath);
}

module.exports = {
  renderRecordToPdf,
  buildRenderPayload,
  insertLogoIntoDocxIfExists,
  stripImagePlaceholderTokensInDocXml,
  stripImagePlaceholderTokensInDocxPath,
  paragraphLooksLikeReferDoctorLabel,
  classifyReferDoctorParagraphJc,
};
