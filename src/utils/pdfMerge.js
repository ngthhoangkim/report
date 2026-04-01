const fs = require('fs');
const { PDFDocument } = require('pdf-lib');
const logger = require('./logger');

function tryGetStreamSize(doc, obj) {
  if (!obj || !doc) return 0;
  try {
    const looked = doc.context.lookup(obj);
    // PDFArray-like
    if (looked && typeof looked.size === 'function' && typeof looked.get === 'function') {
      let sum = 0;
      for (let i = 0; i < looked.size(); i += 1) {
        sum += tryGetStreamSize(doc, looked.get(i));
      }
      return sum;
    }
    // PDFStream-like (duck typing)
    if (looked && looked.contents && typeof looked.contents.length === 'number') {
      return looked.contents.length;
    }
  } catch (_) {
    // ignore
  }
  return 0;
}

function pageLooksBlank(doc, page, minBytes = 20) {
  try {
    const contents = page?.node?.Contents?.();
    if (!contents) return true;
    const bytes = tryGetStreamSize(doc, contents);
    return bytes < minBytes;
  } catch (_) {
    return false;
  }
}

/**
 * Append one page per image (full page, centered) after base PDF — same role as xray merge in test script.
 */
async function mergeBasePdfWithImagePages(basePdfPath, imagePaths, outputPdfPath) {
  const baseBytes = fs.readFileSync(basePdfPath);
  const baseDoc = await PDFDocument.load(baseBytes);
  const outDoc = await PDFDocument.create();

  const copied = await outDoc.copyPages(baseDoc, baseDoc.getPageIndices());
  copied.forEach((page) => outDoc.addPage(page));

  for (const imagePath of imagePaths) {
    const ext = imagePath.toLowerCase();
    if (!['.jpg', '.jpeg', '.png'].some((e) => ext.endsWith(e))) continue;

    const imgBytes = fs.readFileSync(imagePath);
    const embedded = ext.endsWith('.png')
      ? await outDoc.embedPng(imgBytes)
      : await outDoc.embedJpg(imgBytes);

    const pageWidth = 595.28;
    const pageHeight = 841.89;
    const margin = 24;
    const page = outDoc.addPage([pageWidth, pageHeight]);

    const fitWidth = pageWidth - margin * 2;
    const fitHeight = pageHeight - margin * 2 - 20;
    const scale = Math.min(fitWidth / embedded.width, fitHeight / embedded.height);
    const drawW = embedded.width * scale;
    const drawH = embedded.height * scale;
    const x = (pageWidth - drawW) / 2;
    const y = (pageHeight - drawH) / 2 - 10;

    page.drawImage(embedded, { x, y, width: drawW, height: drawH });
  }

  const mergedBytes = await outDoc.save();
  fs.writeFileSync(outputPdfPath, mergedBytes);
}

/**
 * Concatenate multiple PDF buffers (one merged report per ItemNum).
 */
async function mergePdfBuffers(pdfBuffers) {
  const outDoc = await PDFDocument.create();
  let removedBlankPages = 0;
  for (const buf of pdfBuffers) {
    const src = await PDFDocument.load(buf);
    const keepIdx = [];
    for (let i = 0; i < src.getPageCount(); i += 1) {
      const p = src.getPage(i);
      if (pageLooksBlank(src, p)) {
        removedBlankPages += 1;
        continue;
      }
      keepIdx.push(i);
    }
    const pages = await outDoc.copyPages(src, keepIdx);
    pages.forEach((p) => outDoc.addPage(p));
  }
  if (removedBlankPages > 0) {
    logger.info(`Removed ${removedBlankPages} blank PDF page(s) during merge`);
  }
  return Buffer.from(await outDoc.save());
}

module.exports = { mergeBasePdfWithImagePages, mergePdfBuffers };
