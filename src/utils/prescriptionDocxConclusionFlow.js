const fs = require('fs');
const PizZip = require('pizzip');
const logger = require('./logger');
const { escapeXmlText } = require('./prescriptionPayloadHelpers');

const PART_RE =
  /^word\/(document\.xml|header\d+\.xml|footer\d+\.xml|footnotes\.xml|endnotes\.xml)$/i;

function splitParagraphs(documentXml) {
  const re = /<w:p\b[\s\S]*?<\/w:p>/g;
  const out = [];
  let m;
  while ((m = re.exec(documentXml)) !== null) out.push(m[0]);
  return out;
}

function extractParagraphInnerText(paraXml) {
  const texts = [];
  const re = /<w:t([^>]*)>([\s\S]*?)<\/w:t>/g;
  let m;
  while ((m = re.exec(paraXml)) !== null) {
    texts.push(
      m[2]
        .replace(/<w:tab\b[^/]*\/>/g, '\t')
        .replace(/<w:br\b[^/]*\/>/g, '\n'),
    );
  }
  return texts
    .join('')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&amp;/g, '&');
}

function stripFrameAndSoften(paraXml) {
  let s = paraXml.replace(/<w:framePr\b[\s\S]*?<\/w:framePr>/gi, '');
  s = s.replace(/<w:framePr\b[^/>]*\/>/gi, '');
  s = s.replace(/w:hRule="exact"/g, 'w:hRule="atLeast"');
  return s;
}

function extractPPr(paraXml) {
  const mm = paraXml.match(/<w:pPr\b[\s\S]*?<\/w:pPr>/);
  return mm ? mm[0] : '<w:pPr/>';
}

function extractFirstRPr(paraXml) {
  const m = paraXml.match(/<w:r\b[\s\S]*?<\/w:r>/);
  if (!m) return '';
  const rpr = m[0].match(/<w:rPr\b[\s\S]*?<\/w:rPr>/);
  return rpr ? rpr[0] : '';
}

function paragraphTextMatchesNeedle(paraXml, needle) {
  const inner = extractParagraphInnerText(paraXml);
  const n = Math.min(48, needle.length);
  if (inner.includes(needle.slice(0, n))) return true;
  const collapsedInner = inner.replace(/\s+/g, ' ');
  const collapsedNeedle = needle.replace(/\s+/g, ' ');
  return collapsedInner.includes(collapsedNeedle.slice(0, n));
}

function normalizeNeedle(s) {
  return String(s || '').replace(/\s+/g, ' ').trim();
}

function clearAllWTTextKeepingMarkup(paraXml) {
  // Giữ nguyên pPr/frame/border của template, chỉ xóa text để nó không “nhảy” ra đầu trang.
  return paraXml.replace(/(<w:t\b[^>]*>)[\s\S]*?(<\/w:t>)/g, '$1$2');
}

/**
 * Tìm đúng vị trí để hiển thị chẩn đoán: dòng nhãn "Chẩn đoán :" trong mẫu.
 */
function paragraphLooksLikeDiagnosisLabel(paraXml) {
  const t = extractParagraphInnerText(paraXml)
    .replace(/\s+/g, ' ')
    .trim()
    .toLowerCase();
  return t.includes('chẩn đoán') && t.includes(':');
}

function formatDiagnosisLines(plain) {
  // Không cố giữ xuống dòng/bullet để tránh layout “bay” lên đầu trang.
  return normalizeNeedle(plain);
}

function paragraphContainsNeedleLoose(paraXml, needle) {
  const n = normalizeNeedle(needle);
  if (!n || n.length < 8) return false;
  const inner = normalizeNeedle(extractParagraphInnerText(paraXml));
  if (!inner) return false;
  // so sánh theo prefix để chịu lỗi Word/LO đổi dấu tab/space, xuống dòng
  const prefixLen = Math.min(32, n.length);
  const key = n.slice(0, prefixLen);
  return inner.includes(key) || paragraphTextMatchesNeedle(paraXml, key);
}

function replaceNthWParagraph(documentXml, index, replacementXml) {
  let i = 0;
  return documentXml.replace(/<w:p\b[\s\S]*?<\/w:p>/g, (match) => {
    const cur = i;
    i += 1;
    return cur === index ? replacementXml : match;
  });
}

function stripDiagnosisStraysInXml(xml, needle) {
  if (!xml || !needle) return xml;
  const paras = splitParagraphs(xml);
  // cutoff = vị trí "Đội ngũ chuyên môn" nếu có (chỉ có ở document body)
  let cutoff = -1;
  for (let i = 0; i < paras.length; i += 1) {
    const t = extractParagraphInnerText(paras[i]).replace(/\s+/g, ' ').trim().toLowerCase();
    if (t.includes('đội ngũ chuyên môn')) {
      cutoff = i;
      break;
    }
  }

  let idx = 0;
  return xml.replace(/<w:p\b[\s\S]*?<\/w:p>/g, (match) => {
    const cur = idx;
    idx += 1;
    // nếu có cutoff thì chỉ động vào phần trước cutoff; nếu không có cutoff (header/footer) thì chỉ động
    // vào những đoạn có dấu hiệu chứa chẩn đoán.
    const hasFrame = /<w:framePr\b/i.test(match);
    const hasText = normalizeNeedle(extractParagraphInnerText(match));
    if (!hasText) return match;
    const looksLikeDx = paragraphContainsNeedleLoose(match, needle);
    // Case quan trọng: diagnosis nằm trong frame/textbox neo vị trí (có thể nằm *sau* cutoff trong XML)
    // nhưng hiển thị ở đầu trang. Nếu match diagnosis thì xóa luôn để tránh “bay lên”.
    if (looksLikeDx) {
      return clearAllWTTextKeepingMarkup(match);
    }
    if (cutoff > 0 && cur < cutoff) {
      // Trước cutoff: KHÔNG xóa bừa mọi frame (dễ làm mất nhãn/khung khác).
      // Chỉ xóa nếu nó thật sự là đoạn diagnosis (đã xử lý ở trên).
      return match;
    }
    if (cutoff <= 0) {
      return match;
    }
    return match;
  });
}

function findConclusionParagraphIndex(paras, conclusionPlain) {
  const needle = String(conclusionPlain || '').trim();
  if (!needle || needle.length < 8) return -1;
  // Ưu tiên: đoạn có frame/textbox chứa diagnosis (thường bị trôi lên đầu trang)
  for (let i = 0; i < paras.length; i += 1) {
    if (!/<w:framePr\b/i.test(paras[i])) continue;
    if (paragraphTextMatchesNeedle(paras[i], needle)) return i;
  }
  // Fallback: bất kỳ đoạn nào match (ít dùng)
  for (let i = 0; i < paras.length; i += 1) {
    if (paragraphTextMatchesNeedle(paras[i], needle)) return i;
  }
  return -1;
}

/**
 * Sửa chẩn đoán cho đúng mẫu:
 * - Xóa (hoặc làm rỗng) đoạn diagnosis bị nằm trong textbox phía trên đầu trang (nếu có).
 * - Chèn diagnosis ngay sau dòng nhãn "Chẩn đoán :" (xuống dòng bằng w:br).
 */
function applyPrescriptionConclusionFlow(docxPath, conclusionPlainText) {
  const needle = normalizeNeedle(conclusionPlainText);
  if (!needle) return;

  const buf = fs.readFileSync(docxPath);
  const zip = new PizZip(buf);
  // 1) Chặn “textbox trôi lên đầu trang” ở mọi part (document/header/footer...).
  // LO có thể bung textbox ra header/footer hoặc mất framePr.
  for (const name of Object.keys(zip.files)) {
    const entry = zip.files[name];
    if (!entry || entry.dir) continue;
    const n = name.replace(/\\/g, '/');
    if (!PART_RE.test(n)) continue;
    const f = zip.file(name);
    if (!f) continue;
    const before = f.asText();
    const after = stripDiagnosisStraysInXml(before, needle);
    if (after !== before) zip.file(name, after);
  }

  const main = zip.file('word/document.xml');
  if (!main) {
    fs.writeFileSync(docxPath, zip.generate({ type: 'nodebuffer' }));
    return;
  }

  let xml = main.asText();
  const paras2 = splitParagraphs(xml);

  // 2) Find the "Chẩn đoán :" label line
  let idxLabel = -1;
  for (let i = 0; i < paras2.length; i += 1) {
    if (paragraphLooksLikeDiagnosisLabel(paras2[i])) {
      idxLabel = i;
      break;
    }
  }
  if (idxLabel < 0) {
    logger.warn('Toa thuốc: không tìm thấy dòng nhãn "Chẩn đoán :" để chèn nội dung', {
      preview: needle.slice(0, 80),
    });
    fs.writeFileSync(docxPath, zip.generate({ type: 'nodebuffer' }));
    return;
  }

  const pPr = extractPPr(paras2[idxLabel]);
  const rPr = extractFirstRPr(paras2[idxLabel]) || '';
  const text = formatDiagnosisLines(needle);
  if (!text) return;
  // Thay vì chèn paragraph mới (dễ bị rơi ra ngoài vùng frame/table),
  // mình append trực tiếp vào *đúng paragraph nhãn* bằng w:br + run text.
  // Giữ pPr/frame của paragraph nhãn để text nằm đúng vị trí "Chẩn đoán :"
  const insertRuns =
    `<w:r>${rPr}<w:br/>` +
    `<w:t xml:space="preserve">${escapeXmlText(text)}</w:t></w:r>`;

  let seen = 0;
  const out = xml.replace(/<w:p\b[\s\S]*?<\/w:p>/g, (match) => {
    const cur = seen;
    seen += 1;
    if (cur !== idxLabel) return match;
    // append before closing </w:p>
    const end = match.lastIndexOf('</w:p>');
    if (end < 0) return match;
    // Giữ nguyên frame/anchor của paragraph nhãn để nó không “bay” ra đầu trang.
    return match.slice(0, end) + insertRuns + match.slice(end);
  });

  zip.file('word/document.xml', out);
  fs.writeFileSync(docxPath, zip.generate({ type: 'nodebuffer' }));
}

module.exports = {
  applyPrescriptionConclusionFlow,
};
