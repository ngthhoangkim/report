const fs = require('fs');
const path = require('path');
const AdmZip = require('adm-zip');

function ensureDirectoryExists(dirPath) {
  if (!fs.existsSync(dirPath)) {
    fs.mkdirSync(dirPath, { recursive: true });
  }
}

function readMagicHeader(filePath, len = 8) {
  const fd = fs.openSync(filePath, 'r');
  try {
    const buf = Buffer.alloc(len);
    fs.readSync(fd, buf, 0, len, 0);
    return buf;
  } finally {
    fs.closeSync(fd);
  }
}

function isZipMagic(buf) {
  return (
    buf.length >= 4 &&
    buf[0] === 0x50 &&
    buf[1] === 0x4b &&
    buf[2] === 0x03 &&
    buf[3] === 0x04
  );
}

function isJpegMagic(buf) {
  return buf.length >= 3 && buf[0] === 0xff && buf[1] === 0xd8 && buf[2] === 0xff;
}

function isPngMagic(buf) {
  return (
    buf.length >= 8 &&
    buf[0] === 0x89 &&
    buf[1] === 0x50 &&
    buf[2] === 0x4e &&
    buf[3] === 0x47
  );
}

/**
 * Extract image entries from ZIP to directory (same extensions as C# FileHelper).
 */
function extractImagesFromZip(zipPath, extractToDirectory) {
  if (!fs.existsSync(zipPath)) {
    throw new Error(`ZIP file not found: ${zipPath}`);
  }
  ensureDirectoryExists(extractToDirectory);
  const extractedFiles = [];
  const zip = new AdmZip(zipPath);
  const entries = zip.getEntries();
  for (const entry of entries) {
    if (entry.isDirectory) continue;
    const entryName = entry.entryName;
    const nameOnly = path.basename(entryName);
    const ext = path.extname(entryName).toLowerCase();
    if (!['.jpg', '.jpeg', '.png', '.bmp', '.gif', '.tiff'].includes(ext)) continue;
    const destinationPath = path.join(extractToDirectory, nameOnly);
    fs.writeFileSync(destinationPath, entry.getData());
    extractedFiles.push(destinationPath);
  }
  return extractedFiles;
}

/**
 * File không đuôi nhưng là ZIP (PK) → giải nén ảnh.
 * Một file JPEG/PNG đơn (không ZIP) → trả về một đường dẫn ảnh trong extractDir.
 */
function extractImagesFromArchiveOrRaw(filePath, extractToDirectory) {
  if (!fs.existsSync(filePath)) {
    throw new Error(`Media file not found: ${filePath}`);
  }
  ensureDirectoryExists(extractToDirectory);
  const buf = readMagicHeader(filePath, 16);
  if (buf.length && buf.every((b) => b === 0)) {
    throw new Error(
      'File is all zeros (invalid). Replace with real ZIP or JPEG/PNG export.',
    );
  }
  if (isZipMagic(buf)) {
    return extractImagesFromZip(filePath, extractToDirectory);
  }
  if (isJpegMagic(buf) || isPngMagic(buf)) {
    const base = path.basename(filePath);
    let ext = path.extname(base).toLowerCase();
    if (!ext) {
      ext = isPngMagic(buf) ? '.png' : '.jpg';
    }
    const destName = path.extname(base) ? base : base + ext;
    const dest = path.join(extractToDirectory, destName);
    fs.copyFileSync(filePath, dest);
    return [dest];
  }
  throw new Error(
    `Unsupported media (need ZIP or JPEG/PNG): ${filePath}`,
  );
}

function listFilesRecursively(dirPath) {
  if (!fs.existsSync(dirPath)) return [];
  const entries = fs.readdirSync(dirPath, { withFileTypes: true });
  const files = [];
  for (const entry of entries) {
    const fullPath = path.join(dirPath, entry.name);
    if (entry.isDirectory()) {
      files.push(...listFilesRecursively(fullPath));
    } else {
      files.push(fullPath);
    }
  }
  return files;
}

function cleanupDirectory(directoryPath) {
  if (fs.existsSync(directoryPath)) {
    try {
      fs.rmSync(directoryPath, { recursive: true, force: true });
    } catch (_) {
      /* ignore */
    }
  }
}

module.exports = {
  ensureDirectoryExists,
  extractImagesFromZip,
  extractImagesFromArchiveOrRaw,
  listFilesRecursively,
  cleanupDirectory,
};
