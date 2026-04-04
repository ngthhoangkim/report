const db = require('../config/database');

/** Cột thường gặp trỏ tới file ZIP trên disk (máy chủ / thư mục chung với ảnh CDHA). */
const PATH_COLUMN_CANDIDATES = [
  'FileName',
  'FilePath',
  'Path',
  'LocalPath',
  'FullPath',
  'DocumentPath',
  'DocPath',
  'ZipPath',
  'PhysicalPath',
];

function normalizeKeyMap(row) {
  const map = new Map();
  for (const [k, v] of Object.entries(row || {})) {
    map.set(String(k).toLowerCase(), v);
  }
  return map;
}

/**
 * Lấy chuỗi đường dẫn file từ một dòng CN_FILES (không cần biết trước tên cột chính xác).
 * @returns {string|null}
 */
function guessStoredPathFromRow(row) {
  if (!row || typeof row !== 'object') return null;
  const lower = normalizeKeyMap(row);
  for (const col of PATH_COLUMN_CANDIDATES) {
    const v = lower.get(col.toLowerCase());
    if (v != null && String(v).trim() !== '') {
      return String(v).trim();
    }
  }
  return null;
}

/**
 * Bản ghi CN_FILES theo phiên khám (cùng ý nghĩa FileNum + SessionId như báo cáo CDHA).
 * Nếu bảng dùng tên cột khác (vd. SessionID), sửa query trong repo này.
 */
async function getCnFilesByFileNumAndSessionId(fileNum, sessionId) {
  return db.executeQuery(
    `
    SELECT *
    FROM dbo.CN_FILES
    WHERE FileNum = @fileNum
      AND SessionId = @sessionId
    `,
    { fileNum: String(fileNum), sessionId: Number(sessionId) },
  );
}

module.exports = {
  guessStoredPathFromRow,
  getCnFilesByFileNumAndSessionId,
  PATH_COLUMN_CANDIDATES,
};
