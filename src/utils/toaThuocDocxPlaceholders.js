const fs = require('fs');
const PizZip = require('pizzip');

/**
 * Gộp << … Conclusion … >> trong **một** <w:p> chứa Conclusion.
 * Không dùng whole <w:tc>: regex [\s\S]*? có thể nuốt </w:p><w:p> → XML hỏng (LibreOffice "could not be loaded").
 */
function mergeConclusionPlaceholderRuns(xml) {
  const lp = xml.search(/<w:t[^>]*>Conclusion<\/w:t>/);
  if (lp < 0) return xml;
  const pStart = xml.lastIndexOf('<w:p', lp);
  const pEnd = xml.indexOf('</w:p>', lp);
  if (pStart < 0 || pEnd < 0) return xml;
  const para = xml.slice(pStart, pEnd + 6);
  /** Mỗi nhánh là một <w:r>…</w:r> — không dùng [\s\S]*? tới &lt;&lt; (sẽ nuốt cả nhãn "Chuẩn đoán:"). */
  const runLt =
    /<w:r\b[^>]*>(?:(?!<\/w:r>).)*?<w:t[^>]*>&lt;&lt;<\/w:t>(?:(?!<\/w:r>).)*?<\/w:r>/;
  const runMid =
    /<w:r\b[^>]*>(?:(?!<\/w:r>).)*?<w:t[^>]*>Conclusion<\/w:t>(?:(?!<\/w:r>).)*?<\/w:r>/;
  const runGt =
    /<w:r\b[^>]*>(?:(?!<\/w:r>).)*?<w:t[^>]*>&gt;&gt;<\/w:t>(?:(?!<\/w:r>).)*?<\/w:r>/;
  const re = new RegExp(`${runLt.source}[\\s\\S]*?${runMid.source}[\\s\\S]*?${runGt.source}`);
  if (!re.test(para)) return xml;
  const replacement =
    '<w:r><w:rPr><w:rFonts w:ascii="Times New Roman" w:hAnsi="Times New Roman" w:cs="Times New Roman"/>' +
    '<w:sz w:val="20"/><w:szCs w:val="20"/><w:lang w:val="vi-VN"/></w:rPr>' +
    '<w:t>&lt;&lt;Conclusion&gt;&gt;</w:t></w:r>';
  const newPara = para.replace(re, replacement);
  return xml.slice(0, pStart) + newPara + xml.slice(pEnd + 6);
}

/**
 * Gộp << … Address … >> trong cùng đoạn với nhãn Địa chỉ (tránh khớp << placeholder khác trong tài liệu).
 */
/**
 * Mẫu Word: << trong một run, PatientName>> gộp trong run sau (không khớp merge2/merge3).
 */
function mergePatientNamePlaceholderRuns(xml) {
  const lp = xml.indexOf('PatientName&gt;&gt;');
  if (lp < 0) return xml;
  const pStart = xml.lastIndexOf('<w:p', lp);
  const pEnd = xml.indexOf('</w:p>', lp);
  if (pStart < 0 || pEnd < 0) return xml;
  const para = xml.slice(pStart, pEnd + 6);
  const runLt =
    /<w:r\b[^>]*>(?:(?!<\/w:r>).)*?<w:t[^>]*>&lt;&lt;<\/w:t>(?:(?!<\/w:r>).)*?<\/w:r>/;
  const runMid =
    /<w:r\b[^>]*>(?:(?!<\/w:r>).)*?<w:t[^>]*>PatientName&gt;&gt;<\/w:t>(?:(?!<\/w:r>).)*?<\/w:r>/;
  const re = new RegExp(`${runLt.source}[\\s\\S]*?${runMid.source}`);
  if (!re.test(para)) return xml;
  const replacement =
    '<w:r><w:rPr><w:rFonts w:ascii="Times New Roman" w:hAnsi="Times New Roman" w:cs="Times New Roman"/>' +
    '<w:sz w:val="20"/><w:szCs w:val="20"/><w:lang w:val="vi-VN"/></w:rPr>' +
    '<w:t>&lt;&lt;PatientName&gt;&gt;</w:t></w:r>';
  const newPara = para.replace(re, replacement);
  return xml.slice(0, pStart) + newPara + xml.slice(pEnd + 6);
}

function mergeAddressPlaceholderRuns(xml) {
  const lp = xml.search(/<w:t[^>]*>Address<\/w:t>/);
  if (lp < 0) return xml;
  const pStart = xml.lastIndexOf('<w:p', lp);
  const pEnd = xml.indexOf('</w:p>', lp);
  if (pStart < 0 || pEnd < 0) return xml;
  const para = xml.slice(pStart, pEnd + 6);
  const runLt =
    /<w:r\b[^>]*>(?:(?!<\/w:r>).)*?<w:t[^>]*>&lt;&lt;<\/w:t>(?:(?!<\/w:r>).)*?<\/w:r>/;
  const runMid =
    /<w:r\b[^>]*>(?:(?!<\/w:r>).)*?<w:t[^>]*>Address<\/w:t>(?:(?!<\/w:r>).)*?<\/w:r>/;
  const runGt =
    /<w:r\b[^>]*>(?:(?!<\/w:r>).)*?<w:t[^>]*>&gt;&gt;<\/w:t>(?:(?!<\/w:r>).)*?<\/w:r>/;
  const re = new RegExp(`${runLt.source}[\\s\\S]*?${runMid.source}[\\s\\S]*?${runGt.source}`);
  if (!re.test(para)) return xml;
  const replacement =
    '<w:r><w:rPr><w:rFonts w:ascii="Times New Roman" w:hAnsi="Times New Roman" w:cs="Times New Roman"/>' +
    '<w:sz w:val="20"/><w:szCs w:val="20"/><w:lang w:val="vi-VN"/></w:rPr>' +
    '<w:t>&lt;&lt;Address&gt;&gt;</w:t></w:r>';
  const newPara = para.replace(re, replacement);
  return xml.slice(0, pStart) + newPara + xml.slice(pEnd + 6);
}

/**
 * Bỏ in đậm ở run chứa <<Address>> (giá trị địa chỉ không đậm; nhãn "Địa chỉ:" vẫn đậm ở run riêng).
 */
function findLastRunOpenBefore(xml, beforeIndex) {
  const sub = xml.slice(0, beforeIndex);
  const re = /<w:r(?:\s[^>]*)?>/g;
  let last = -1;
  let m;
  while ((m = re.exec(sub)) !== null) last = m.index;
  return last;
}

function stripBoldFromAddressValueRun(xml) {
  const token = '&lt;&lt;Address&gt;&gt;';
  let idx = 0;
  let s = xml;
  while (true) {
    const pos = s.indexOf(token, idx);
    if (pos < 0) break;
    const rStart = findLastRunOpenBefore(s, pos);
    if (rStart < 0) {
      idx = pos + 1;
      continue;
    }
    const rEnd = s.indexOf('</w:r>', pos);
    if (rEnd < 0) break;
    const run = s.slice(rStart, rEnd + 6);
    const m = run.match(/<w:rPr\b[\s\S]*?<\/w:rPr>/);
    if (m) {
      const newRPr = m[0].replace(/<w:b\/\s*>/g, '').replace(/<w:bCs\/\s*>/g, '');
      const newRun = run.replace(m[0], newRPr);
      s = s.slice(0, rStart) + newRun + s.slice(rEnd + 6);
      idx = rStart + newRun.length;
    } else {
      idx = pos + 1;
    }
  }
  return s;
}

/** Dòng thuốc lặp: không tách dòng giữa hai trang — đẩy cả dòng sang trang sau nếu không đủ chỗ. */
function addCantSplitToItemsTemplateRow(xml) {
  const marker = '&lt;&lt;#items&gt;&gt;';
  const i = xml.indexOf(marker);
  if (i < 0) return xml;
  const before = xml.slice(0, i);
  const trStart = before.lastIndexOf('<w:tr');
  if (trStart < 0) return xml;
  const afterTrOpen = xml.indexOf('>', trStart) + 1;
  const head = xml.slice(trStart, afterTrOpen);
  if (head.includes('<w:trPr>')) return xml;
  return `${xml.slice(0, afterTrOpen)}<w:trPr><w:cantSplit/></w:trPr>${xml.slice(afterTrOpen)}`;
}

function paragraphHasRenderableTextOrImage(pXml) {
  return (
    /<w:t\b/.test(pXml) ||
    /<w:drawing\b/.test(pXml) ||
    /<w:pict\b/.test(pXml) ||
    /<w:object\b/.test(pXml) ||
    /<w:tab\b/.test(pXml) ||
    /<w:br\b/.test(pXml)
  );
}

/**
 * Mẫu toa có chuỗi nhiều <w:p> không có chữ nằm **trước** đoạn chỉ có <w:tab/> (căn chỉnh).
 * stripTrailingEmptyParagraphsBeforeSect không xóa được vì từ cuối file gặp tab rồi mới tới các đoạn trống đó.
 * Các đoạn trống vẫn chiếm chiều cao → Word/LO có thể tách hàng bảng ngoài sang trang 2 (ô trống + footer).
 */
function collapseConsecutiveEmptyParagraphRuns(xml) {
  if (String(process.env.PRESCRIPTION_KEEP_EMPTY_PARA_RUNS || '').toLowerCase() === 'true') {
    return xml;
  }
  const minRun = 2;
  const matches = [...xml.matchAll(/<w:p\b[\s\S]*?<\/w:p>/g)].map((m) => ({
    start: m.index,
    end: m.index + m[0].length,
    xml: m[0],
  }));
  if (matches.length === 0) return xml;
  const remove = new Set();
  let runStart = -1;
  const flushRun = (endIdx) => {
    if (runStart < 0) return;
    const runLen = endIdx - runStart;
    if (runLen >= minRun) {
      for (let j = runStart; j < endIdx; j += 1) remove.add(j);
    }
    runStart = -1;
  };
  for (let i = 0; i < matches.length; i += 1) {
    const empty = !paragraphHasRenderableTextOrImage(matches[i].xml);
    if (empty) {
      if (runStart < 0) runStart = i;
    } else {
      flushRun(i);
    }
  }
  flushRun(matches.length);
  if (remove.size === 0) return xml;
  let out = '';
  let last = 0;
  for (let i = 0; i < matches.length; i += 1) {
    out += xml.slice(last, matches[i].start);
    if (!remove.has(i)) {
      out += matches[i].xml;
    }
    last = matches[i].end;
  }
  out += xml.slice(last);
  return out;
}

/**
 * Bỏ các <w:p> trống **liên tiếp ở cuối** (trước sectPr), không cắt ngang từ w:p tới hết chuỗi —
 * nếu không sẽ xóa luôn </w:tc></w:tr></w:tbl> sau các đoạn trống.
 */
function stripTrailingEmptyParagraphsBeforeSect(xml) {
  const sectIdx = xml.indexOf('<w:sectPr');
  if (sectIdx < 0) return xml;
  const head = xml.slice(0, sectIdx);
  const tail = xml.slice(sectIdx);
  const paraRe = /<w:p\b[\s\S]*?<\/w:p>/g;
  const ranges = [];
  let m;
  while ((m = paraRe.exec(head)) !== null) {
    ranges.push({ start: m.index, end: m.index + m[0].length, xml: m[0] });
  }
  if (ranges.length === 0) return xml;
  let trimFrom = ranges.length;
  for (let i = ranges.length - 1; i >= 0; i--) {
    if (paragraphHasRenderableTextOrImage(ranges[i].xml)) {
      trimFrom = i + 1;
      break;
    }
  }
  if (trimFrom >= ranges.length) return xml;
  const cutStart = ranges[trimFrom].start;
  const cutEnd = ranges[ranges.length - 1].end;
  const newHead = `${head.slice(0, cutStart)}${head.slice(cutEnd)}`.replace(/\s+$/, '');
  return newHead + tail;
}

/** Hàng đầu mẫu: bỏ trHeight ~13580 twips (Word ép gần hết trang → cột phải dư khoảng trắng rất lớn). */
function removeOuterTableFirstRowForcedHeight(xml) {
  if (String(process.env.PRESCRIPTION_KEEP_OUTER_ROW_HEIGHT || '').toLowerCase() === 'true') {
    return xml;
  }
  return xml.replace(/<w:trPr>\s*<w:trHeight\s+w:val="13580"\s*\/>\s*<\/w:trPr>/, '');
}

/**
 * LibreOffice tách &lt;&lt;token&gt;&gt; thành nhiều <w:t>. Gộp lại để docxtemplater thấy <<token>> liền mạch.
 */
function fixDocumentXmlPlaceholders(xml) {
  let s = xml;

  const merge3 = (name) => {
    const re = new RegExp(
      `(<w:t[^>]*>)(&lt;&lt;)(</w:t></w:r><w:r[^>]*>[\\s\\S]*?<w:t[^>]*>)(${name})(</w:t></w:r><w:r[^>]*>[\\s\\S]*?<w:t[^>]*>)(&gt;&gt;)(</w:t>)`,
      'g',
    );
    s = s.replace(re, `$1&lt;&lt;${name}&gt;&gt;$7`);
  };
  merge3('Items');

  const merge2 = (suffix) => {
    const re = new RegExp(
      `(<w:t[^>]*>)(&lt;&lt;${suffix})(</w:t></w:r><w:r[^>]*>[\\s\\S]*?<w:t[^>]*>)(&gt;&gt;)(</w:t>)`,
      'g',
    );
    s = s.replace(re, `$1&lt;&lt;${suffix}&gt;&gt;$5`);
  };
  [
    'Temp',
    'PerD',
    'Note',
    'DPL',
    'Test',
    'ReExamDate',
    'Barcode',
    'Quantity',
    '#items',
    'ItemLine',
    'RowQty',
    '/items',
  ].forEach(merge2);

  s = s.replace(/&lt;&lt;Barcode&gt;&gt;/g, '&lt;&lt;BarcodeImg&gt;&gt;');
  merge2('BarcodeImg');

  // Một dòng bảng = một thuốc
  s = s.replace(/&lt;&lt;Items&gt;&gt;/g, '&lt;&lt;#items&gt;&gt;&lt;&lt;ItemLine&gt;&gt;');
  s = s.replace(/&lt;&lt;Quantity&gt;&gt;/g, '&lt;&lt;RowQty&gt;&gt;&lt;&lt;/items&gt;&gt;');

  s = mergeConclusionPlaceholderRuns(s);
  s = mergePatientNamePlaceholderRuns(s);
  s = mergeAddressPlaceholderRuns(s);
  s = stripBoldFromAddressValueRun(s);
  s = removeOuterTableFirstRowForcedHeight(s);
  s = collapseConsecutiveEmptyParagraphRuns(s);
  s = stripTrailingEmptyParagraphsBeforeSect(s);
  // cantSplit trên mọi dòng thuốc — với đơn dài (hàng chục dòng) LibreOffice có thể không tạo được PDF.
  if (String(process.env.PRESCRIPTION_RX_ROW_CANT_SPLIT || '').toLowerCase() === 'true') {
    s = addCantSplitToItemsTemplateRow(s);
  }

  return s;
}

function fixToaThuocDocxPlaceholders(docxPath) {
  const buf = fs.readFileSync(docxPath);
  const zip = new PizZip(buf);
  let touched = false;

  const doc = zip.file('word/document.xml');
  if (doc) {
    const xml = doc.asText();
    const out = fixDocumentXmlPlaceholders(xml);
    if (out !== xml) {
      zip.file('word/document.xml', out);
      touched = true;
    }
  }

  if (touched) {
    fs.writeFileSync(docxPath, zip.generate({ type: 'nodebuffer' }));
  }
}

module.exports = {
  fixDocumentXmlPlaceholders,
  fixToaThuocDocxPlaceholders,
  collapseConsecutiveEmptyParagraphRuns,
};
