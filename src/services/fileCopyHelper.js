const fs = require('fs');
const path = require('path');
const { promisify } = require('util');
const copyFile = promisify(fs.copyFile);
const logger = require('../utils/logger');
const { ensureDirectoryExists } = require('../utils/fileHelper');
const { resolveExistingFileInDir, resolveExistingImageFileInDir } = require('../utils/imageSourceResolve');

/**
 * Mirrors MedicalReportServer FileCopyHelper — lock simplified to sequential copy per file name.
 */
class FileCopyHelper {
  constructor(sourceImageDir, fallbackImageDir, localImageDir) {
    this.sourceImageDir = sourceImageDir || '';
    this.fallbackImageDir = fallbackImageDir || '';
    this.localImageDir = localImageDir || '';
    this._queues = new Map();
    ensureDirectoryExists(this.localImageDir);
  }

  async copyFileWithFallback(fileName) {
    if (!fileName) return null;
    const key = fileName;
    const prev = this._queues.get(key) || Promise.resolve();
    const result = prev.then(() => this._copyOnce(fileName));
    this._queues.set(key, result.catch(() => {}));
    return result;
  }

  async _copyOnce(fileName) {
    const localPath = path.join(this.localImageDir, fileName);
    if (fs.existsSync(localPath)) {
      // Nếu cache local tồn tại nhưng bị "all zeros" (invalid), bỏ qua để lấy từ source.
      try {
        const st = fs.statSync(localPath);
        if (st.size > 0) {
          const fd = fs.openSync(localPath, 'r');
          try {
            const buf = Buffer.alloc(32);
            fs.readSync(fd, buf, 0, buf.length, 0);
            const isAllZeros = buf.length > 0 && buf.every((b) => b === 0);
            if (!isAllZeros) return localPath;
          } finally {
            fs.closeSync(fd);
          }
        }
      } catch (e) {
        logger.warn(`Local cached media exists but is unreadable, will fallback: ${localPath}: ${e.message}`);
      }
    }

    const resolvedSource =
      resolveExistingFileInDir(this.sourceImageDir, fileName) ||
      resolveExistingFileInDir(this.fallbackImageDir, fileName);
    if (resolvedSource && (await this._tryCopy(resolvedSource, localPath))) {
      logger.info(`Copied media from ${resolvedSource} → ${localPath}`);
      return localPath;
    }

    logger.warn(`File not found in source or fallback: ${fileName}`);
    return null;
  }

  /**
   * Tìm file media/ảnh đã có sẵn trên disk (cache local → img/source → fallback).
   * Dùng cho ảnh lẻ trong `img/` trùng tên với bảng pathology, không bắt buộc nằm trong ZIP.
   * @returns {string|null}
   */
  resolveMediaPathOrNull(fileName) {
    if (!fileName) return null;
    const base = String(fileName).trim();
    if (!base) return null;
    return (
      resolveExistingImageFileInDir(this.localImageDir, base) ||
      resolveExistingImageFileInDir(this.sourceImageDir, base) ||
      resolveExistingImageFileInDir(this.fallbackImageDir, base)
    );
  }

  async _tryCopy(sourcePath, destPath) {
    try {
      if (!fs.existsSync(sourcePath)) return false;
      if (fs.existsSync(destPath)) {
        const st = fs.statSync(sourcePath);
        const dt = fs.statSync(destPath);
        if (st.mtimeMs <= dt.mtimeMs) return true;
      }
      ensureDirectoryExists(path.dirname(destPath));
      await copyFile(sourcePath, destPath);
      return true;
    } catch (e) {
      logger.warn(`Failed to copy from ${sourcePath}: ${e.message}`);
      return false;
    }
  }
}

module.exports = { FileCopyHelper };
