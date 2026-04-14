const { promisify } = require('util');
const bwipjs = require('bwip-js');

const toBuffer = promisify(bwipjs.toBuffer);

/** PNG 1×1 trong suốt (fallback khi không có chuỗi mã) */
const TINY_PNG = Buffer.from(
  'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg==',
  'base64',
);

/**
 * Ảnh PNG mã vạch Code128 (để chèn docxtemplater-image).
 * @param {string} text
 * @returns {Promise<Buffer>}
 */
async function renderBarcodeCode128Png(text) {
  const t = String(text == null ? '' : text).trim();
  if (!t) return TINY_PNG;
  return toBuffer({
    bcid: 'code128',
    text: t,
    scale: 2,
    height: 14,
    includetext: false,
    paddingwidth: 6,
    paddingheight: 4,
  });
}

module.exports = {
  renderBarcodeCode128Png,
  TINY_PNG,
};
