const fs = require('fs');
const path = require('path');
const logger = require('../utils/logger');
const { ensureDirectoryExists, extractImagesFromArchiveOrRaw } = require('../utils/fileHelper');
const {
  getCnFilesByFileNumAndSessionId,
  guessStoredPathFromRow,
} = require('../repositories/cnFilesRepository');

/**
 * Copy + giải nén (ZIP / JPG đơn) mọi file trỏ bởi CN_FILES.FileName trong phiên (FileNum + SessionId).
 * Dùng khi media không nằm trong CN_ImagingResult.FileName (vd. ECG/PAP trong CN_FILES).
 *
 * Mật khẩu ZIP (ZipCrypto): đặt env CN_FILES_ZIP_PASSWORD. AES có thể không mở được (adm-zip).
 *
 * @returns {string[]} đường dẫn tuyệt đối tới file ảnh đã bung (jpg/png/...)
 */
function cnFilesZipPasswordFromEnv() {
  const v = process.env.CN_FILES_ZIP_PASSWORD;
  if (v == null || String(v).trim() === '') return undefined;
  return String(v);
}

async function collectCnFilesSessionMedia(fileCopyHelper, fileNum, sessionId, extractToDir) {
  ensureDirectoryExists(extractToDir);
  const zipPassword = cnFilesZipPasswordFromEnv();
  let rows;
  try {
    rows = await getCnFilesByFileNumAndSessionId(fileNum, sessionId);
  } catch (e) {
    logger.warn('CN_FILES query failed (session media)', {
      message: e.message,
      fileNum,
      sessionId,
    });
    return [];
  }
  if (!rows.length) {
    return [];
  }

  const out = [];
  const seen = new Set();

  for (const row of rows) {
    const claimed = guessStoredPathFromRow(row);
    if (!claimed) continue;
    const baseName = path.basename(String(claimed).replace(/\\/g, '/'));
    if (!baseName) continue;

    try {
      const localPath = await fileCopyHelper.copyFileWithFallback(baseName);
      if (!localPath || !fs.existsSync(localPath)) {
        logger.warn('CN_FILES: file not on disk after copy fallback', {
          baseName,
          docTitle: row.DocTitle,
          id: row.ID != null ? row.ID : row.Id,
        });
        continue;
      }
      // Optional tracker: allow caller to cleanup local cache files after job.
      if (typeof arguments[4] === 'function') {
        try {
          arguments[4](localPath);
        } catch (_) {
          // ignore
        }
      }
      const extracted = extractImagesFromArchiveOrRaw(localPath, extractToDir, zipPassword);
      for (const p of extracted) {
        const rp = path.resolve(p);
        if (!seen.has(rp)) {
          seen.add(rp);
          out.push(rp);
        }
      }
    } catch (e) {
      logger.warn('CN_FILES: skip media row', {
        baseName,
        message: e.message,
        id: row.ID != null ? row.ID : row.Id,
      });
    }
  }

  return out;
}

module.exports = { collectCnFilesSessionMedia, cnFilesZipPasswordFromEnv };
