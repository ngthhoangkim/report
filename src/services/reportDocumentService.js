const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { execFile } = require('child_process');
const { promisify } = require('util');
const PizZip = require('pizzip');
const Docxtemplater = require('docxtemplater');
const logger = require('../utils/logger');
const { decompressToString } = require('../utils/gzipHelper');
const { extractImagesFromArchiveOrRaw, ensureDirectoryExists } = require('../utils/fileHelper');
const { convertWithOffice } = require('../utils/officeConvert');
const { rtfToPlainText } = require('../utils/rtfToPlain');
const { mergeBasePdfWithImagePages } = require('../utils/pdfMerge');
const { validateWordTemplateFile } = require('../utils/wordFileValidate');
const { calcAge } = require('../utils/age');
const { getPrintedImageFilenames } = require('../repositories/reportRepository');

const execFileAsync = promisify(execFile);

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
  return String(v);
}

/**
 * Payload render docx.
 * - Tránh "undefined" bằng cách luôn trả '' cho key thiếu/không set.
 * - Pre-fill Image1..ImageN để các placeholder ảnh không bị tràn sang undefined.
 */
function buildRenderPayload(record, rtfTokens) {
  const IMAGE_KEYS_MAX = 200;
  const payload = {
    FileNm: str(record.fileNum || record.itemNum),
    PatientName: str(record.patientName),
    Age: str(calcAge(record.dob, record.ngayKham)),
    Gender: str(record.gender),
    Diagnosis: '',
    ReferDoctor: str(record.requestedDoctor),
    Doctor: str(buildDoctorLabel(record)),
    ItemNum: str(record.itemNum),
    SampleNumber: str(record.sampleNumber || record.itemNum),
    Address: str(record.address),
    PathologyName: '',
    CDNS: '',
    DateRpt: str(formatDateVN(record.ngayKham)),
    Result: str(rtfTokens.resultToken),
    Conclusion: str(rtfTokens.conclusionToken),
    Suggestion: str(rtfTokens.suggestionToken),
    SessionId: str(record.sessionId),
    FileNum: str(record.fileNum),
  };
  for (let i = 1; i <= IMAGE_KEYS_MAX; i += 1) {
    payload[`Image${i}`] = '';
  }
  // Optional: allow caller to keep an image placeholder token for later XML-based injection.
  if (rtfTokens && rtfTokens.imageTokens) {
    for (const [k, v] of Object.entries(rtfTokens.imageTokens)) {
      payload[k] = str(v);
    }
  }

  // Trả '' cho các key không tồn tại để docxtemplater không in "undefined"
  return new Proxy(payload, {
    get(target, prop) {
      if (typeof prop === 'string' && !(prop in target)) return '';
      return target[prop];
    },
  });
}

async function convertWithLibreOffice(mode, inputPath, outDir) {
  // Backward-compat wrapper name used throughout this module.
  // On Windows: may use Word (USE_WORD=true). On macOS/Linux: uses soffice.
  await convertWithOffice(mode, inputPath, outDir);
}

function extractWordDocumentXmlInnerBody(docXml) {
  const bodyStart = docXml.indexOf('<w:body>');
  const bodyEnd = docXml.indexOf('</w:body>');
  if (bodyStart < 0 || bodyEnd < 0 || bodyEnd <= bodyStart) return '';

  let inner = docXml.slice(bodyStart + '<w:body>'.length, bodyEnd);

  // Remove trailing section properties if present; inserting those mid-document can break structure.
  const sectStart = inner.lastIndexOf('<w:sectPr');
  if (sectStart >= 0) {
    inner = inner.slice(0, sectStart);
  }
  return inner;
}

function replaceParagraphContainingToken(docXml, token, replacementInnerXml) {
  const idx = docXml.indexOf(token);
  if (idx < 0) return docXml;

  // Replace the whole paragraph containing the token to avoid invalid nesting.
  // Important: don't match <w:pPr ...> (it also starts with "<w:p").
  const before = docXml.slice(0, idx);
  const matches = [...before.matchAll(/<w:p(\s|>)/g)];
  const pStart = matches.length ? matches[matches.length - 1].index : -1;
  const pEnd = docXml.indexOf('</w:p>', idx);
  if (pStart < 0 || pEnd < 0) return docXml;

  const pEndWithTag = pEnd + '</w:p>'.length;
  return docXml.slice(0, pStart) + replacementInnerXml + docXml.slice(pEndWithTag);
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
    if (!mainDocXml.includes(token)) continue;

    const rtfPath = path.join(tempDirForRtf, `${token}.rtf`);
    fs.writeFileSync(rtfPath, rtfText, 'utf8');

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
    const replacementInnerXml = extractWordDocumentXmlInnerBody(convDocXml);

    // Replace paragraph containing token with paragraphs from converted rtf.
    mainDocXml = replaceParagraphContainingToken(mainDocXml, token, replacementInnerXml);
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

async function embedImagesIntoDocx(renderedDocxPath, tokenToImagePath) {
  const zip = new PizZip(fs.readFileSync(renderedDocxPath));
  const docXmlFile = zip.file('word/document.xml');
  const relsFile = zip.file('word/_rels/document.xml.rels');
  if (!docXmlFile || !relsFile) return { embedded: 0 };

  let docXml = docXmlFile.asText();
  let relsXml = relsFile.asText();
  let embedded = 0;

  for (const [token, imagePath] of Object.entries(tokenToImagePath || {})) {
    if (!imagePath || !fs.existsSync(imagePath)) continue;
    if (!docXml.includes(token)) continue;

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
    docXml = replaceParagraphContainingToken(docXml, token, paraXml);
    embedded += 1;
  }

  zip.file('word/document.xml', docXml);
  zip.file('word/_rels/document.xml.rels', relsXml);
  fs.writeFileSync(renderedDocxPath, zip.generate({ type: 'nodebuffer' }));
  return { embedded };
}

/**
 * One imaging record → one PDF buffer, hoặc null nếu không có template trên disk.
 */
async function renderRecordToPdf(record, segmentIndex, tempDir, ctx) {
  const { fileCopyHelper, templateSelector } = ctx;

  const extractDir = path.join(tempDir, `extract_${record.imagingResultId}_${segmentIndex}`);
  ensureDirectoryExists(extractDir);

  let zipPath = null;
  if (record.fileName) {
    zipPath = await fileCopyHelper.copyFileWithFallback(record.fileName);
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
  const imageFiles = [];
  const imageFilesSet = new Set();
  for (const filename of printedNames) {
    const wantRaw = String(filename).trim();
    if (!wantRaw) continue;

    const wantFile = path.basename(wantRaw);
    const wantNoExt = path.parse(wantFile).name.toLowerCase();

    let abs = extractedFiles.find((f) => {
      const fFile = path.basename(f);
      if (String(fFile).toLowerCase() === wantFile.toLowerCase()) return true;
      const fNoExt = path.parse(fFile).name.toLowerCase();
      return fNoExt === wantNoExt;
    });
    if (!abs || !fs.existsSync(abs)) {
      // Resolve từ disk theo basename (ưu tiên jpg/png trước)
      abs = fileCopyHelper.resolveMediaPathOrNull(wantFile);
      if ((!abs || !fs.existsSync(abs)) && wantNoExt) {
        abs = fileCopyHelper.resolveMediaPathOrNull(wantNoExt);
      }
    }

    if (abs && fs.existsSync(abs)) {
      const low = abs.toLowerCase();
      // Nếu resolve ra zip thì extract rồi thử match lại theo want
      if (low.endsWith('.zip')) {
        try {
          const extractedFromZip = extractImagesFromArchiveOrRaw(abs, extractDir);
          extractedFiles = extractedFiles.concat(extractedFromZip);
          abs = extractedFiles.find((f) => {
            const fFile = path.basename(f);
            if (String(fFile).toLowerCase() === wantFile.toLowerCase()) return true;
            const fNoExt = path.parse(fFile).name.toLowerCase();
            return fNoExt === wantNoExt;
          });
        } catch (ex) {
          logger.warn(
            `Failed to extract zip for ImagingResultId=${record.imagingResultId}, zip=${abs}: ${ex.message}`,
          );
        }
      }

      // Chỉ merge ảnh jpg/png (pdf-lib không render bmp/webp trực tiếp).
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

  logger.info(
    `Resolved ${imageFiles.length}/${printedNames.length} image(s) for ImagingResultId=${record.imagingResultId}`,
  );

  const templatePath = templateSelector.selectTemplate(record.templateFile);

  if (!templatePath) {
    logger.warn(
      `Skip ImagingResultId=${record.imagingResultId}: template missing or empty TemplateFile`,
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

  await convertWithLibreOffice('docx', stagedTemplate, tempDir);
  const templateDocxPath = path.join(tempDir, `${path.parse(stagedTemplate).name}.docx`);
  if (!fs.existsSync(templateDocxPath)) {
    throw new Error(`LibreOffice could not convert template to DOCX: ${stagedTemplate}`);
  }

  const resultRtf = decompressToString(record.resultData);
  const conclusionRtf = decompressToString(record.conclusionData);
  const suggestionRtf = decompressToString(record.suggestionData);

  const tmpPrefix = path.join(tempDir, `rtf_${record.imagingResultId}_${segmentIndex}`);

  // Fallback plain-text (used if RTF injection fails)
  const resultPlain = resultRtf ? rtfToPlainText(resultRtf, `${tmpPrefix}_result`) : '';
  const conclusionPlain = conclusionRtf
    ? rtfToPlainText(conclusionRtf, `${tmpPrefix}_conclusion`)
    : '';
  const suggestionPlain = suggestionRtf
    ? rtfToPlainText(suggestionRtf, `${tmpPrefix}_suggestion`)
    : '';

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
    resultToken: resultRtf ? '__RTF_RESULT__' : '',
    conclusionToken: conclusionRtf ? '__RTF_CONCLUSION__' : '',
    suggestionToken: suggestionRtf ? '__RTF_SUGGESTION__' : '',
    imageTokens,
  };

  const templateDocxBinary = fs.readFileSync(templateDocxPath, 'binary');
  const docZip = new PizZip(templateDocxBinary);
  const doc = new Docxtemplater(docZip, {
    delimiters: { start: '<<', end: '>>' },
    linebreaks: true,
    paragraphLoop: true,
    /** Thẻ trong .docx mà không có trong payload → chuỗi rỗng, không in "undefined". */
    nullGetter() {
      return '';
    },
  });

  doc.render(buildRenderPayload(record, tokens));

  const baseName = `rendered_${record.imagingResultId}_${segmentIndex}`;
  const renderedDocxPath = path.join(tempDir, `${baseName}.docx`);
  fs.writeFileSync(renderedDocxPath, doc.getZip().generate({ type: 'nodebuffer' }));

  const basePdfPath = path.join(tempDir, `${baseName}.pdf`);

  // Replace RTF tokens with converted RTF content (paragraph-level) to preserve formatting.
  try {
    const tokenToRtf = {};
    if (resultRtf) tokenToRtf.__RTF_RESULT__ = resultRtf;
    if (conclusionRtf) tokenToRtf.__RTF_CONCLUSION__ = conclusionRtf;
    if (suggestionRtf) tokenToRtf.__RTF_SUGGESTION__ = suggestionRtf;
    if (Object.keys(tokenToRtf).length) {
      await injectRtfIntoDocx(
        renderedDocxPath,
        tokenToRtf,
        path.join(tempDir, `rtf_inject_${record.imagingResultId}_${segmentIndex}`),
      );
    }

    await convertWithLibreOffice('pdf', renderedDocxPath, tempDir);
    if (!fs.existsSync(basePdfPath)) {
      throw new Error(`LibreOffice produced no PDF: ${basePdfPath}`);
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
    await convertWithLibreOffice('pdf', renderedDocxPath, tempDir);
    if (!fs.existsSync(basePdfPath)) {
      throw new Error(`LibreOffice produced no PDF after fallback: ${basePdfPath}`);
    }
  }

  const finalPdfPath = path.join(tempDir, `${baseName}_final.pdf`);
  // Prefer embedding into template placeholders (Image1/Image2). If embedding fails, fallback to append pages.
  let embeddedCount = 0;
  try {
    const emb = await embedImagesIntoDocx(renderedDocxPath, tokenToImagePath);
    embeddedCount = emb.embedded || 0;
  } catch (e) {
    logger.warn(`Embed images into docx failed, will append pages: ${e.message}`);
  }

  if (embeddedCount > 0) {
    // Re-convert docx (now with embedded images) to pdf, then no need to append image pages.
    await convertWithLibreOffice('pdf', renderedDocxPath, tempDir);
    if (fs.existsSync(basePdfPath)) {
      fs.copyFileSync(basePdfPath, finalPdfPath);
    } else {
      await mergeBasePdfWithImagePages(basePdfPath, imageFiles, finalPdfPath);
    }
  } else {
    await mergeBasePdfWithImagePages(basePdfPath, imageFiles, finalPdfPath);
  }

  return fs.readFileSync(finalPdfPath);
}

module.exports = {
  renderRecordToPdf,
  buildRenderPayload,
};
