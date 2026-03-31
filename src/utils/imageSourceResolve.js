const fs = require('fs');
const path = require('path');

/** Thử các tên file trên disk khi DB chỉ lưu basename không đuôi. */
const NAME_SUFFIXES = ['', '.zip', '.ZIP', '.jpg', '.jpeg', '.JPG', '.JPEG', '.png', '.PNG', '.bmp', '.webp'];

/**
 * Ưu tiên tìm file ảnh jpg/png trước để merge vào PDF ngay.
 * Nếu chỉ có zip thì trả về zip (caller sẽ extract nếu cần).
 */
const IMAGE_NAME_SUFFIXES_PREFERRED = [
  '',
  '.jpg',
  '.jpeg',
  '.JPG',
  '.JPEG',
  '.png',
  '.PNG',
  '.bmp',
  '.webp',
  '.gif',
  '.tif',
  '.tiff',
  '.zip',
  '.ZIP',
];

/**
 * @returns {string|null} đường dẫn tuyệt đối tới file tồn tại
 */
function resolveExistingFileInDir(dir, baseName) {
  if (!dir || !baseName) return null;
  for (const suf of NAME_SUFFIXES) {
    const p = path.join(dir, String(baseName) + suf);
    if (fs.existsSync(p) && fs.statSync(p).isFile()) {
      return p;
    }
  }
  return null;
}

/**
 * Resolve file ưu tiên kiểu ảnh trước, chỉ fallback sang zip khi không thấy ảnh.
 * @returns {string|null}
 */
function resolveExistingImageFileInDir(dir, baseName) {
  if (!dir || !baseName) return null;
  for (const suf of IMAGE_NAME_SUFFIXES_PREFERRED) {
    const p = path.join(dir, String(baseName) + suf);
    if (fs.existsSync(p) && fs.statSync(p).isFile()) {
      return p;
    }
  }
  return null;
}

module.exports = {
  resolveExistingFileInDir,
  NAME_SUFFIXES,
  resolveExistingImageFileInDir,
};
