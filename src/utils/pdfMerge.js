const fs = require('fs');
const { PDFDocument } = require('pdf-lib');

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
  for (const buf of pdfBuffers) {
    const src = await PDFDocument.load(buf);
    const pages = await outDoc.copyPages(src, src.getPageIndices());
    pages.forEach((p) => outDoc.addPage(p));
  }
  return Buffer.from(await outDoc.save());
}

module.exports = { mergeBasePdfWithImagePages, mergePdfBuffers };
