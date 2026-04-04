const db = require('../config/database');

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
 * Bản ghi CN_FILES theo FileNum + SessionId (cùng tham số với ViewImagingResult / báo cáo CDHA).
 *
 * CN_FILES có SubSessionId; join CR_SubSession → CR_Session → CR_Patient.FileNum.
 *
 * ViewImagingResult.SessionId ở một số cơ sở là CR_Session.Id, chỗ khác là CR_SubSession.Id.
 * Mặc định dùng (s.Id = @sessionId OR ss.Id = @sessionId) để khớp cả hai.
 *
 * Ép chỉ một nhánh: env CN_FILES_SESSION_ID_MATCHES=cr_session | subsession
 */
function buildSessionIdPredicate() {
  const mode = String(process.env.CN_FILES_SESSION_ID_MATCHES || '').toLowerCase();
  if (mode === 'subsession' || mode === 'cr_subsession') {
    return 'ss.Id = @sessionId';
  }
  if (mode === 'cr_session' || mode === 'session') {
    return 's.Id = @sessionId';
  }
  return '(s.Id = @sessionId OR ss.Id = @sessionId)';
}

function fileNumMatchSql(alias = 'pat') {
  return `LTRIM(RTRIM(CONVERT(VARCHAR(50), ${alias}.FileNum))) = LTRIM(RTRIM(@fileNum))`;
}

async function getCnFilesByFileNumAndSessionId(fileNum, sessionId) {
  const sessionPred = buildSessionIdPredicate();
  const filePred = fileNumMatchSql('pat');
  return db.executeQuery(
    `
    SELECT f.*
    FROM dbo.CN_FILES f
    INNER JOIN dbo.CR_SubSession ss ON ss.Id = f.SubSessionId
    INNER JOIN dbo.CR_Session s ON s.Id = ss.SessionId
    INNER JOIN dbo.CR_Patient pat ON pat.ContactId = s.PatientId
    WHERE ${filePred}
      AND ${sessionPred}
      AND f.DeletedDate IS NULL
      AND ss.DeletedDate IS NULL
    `,
    { fileNum: String(fileNum), sessionId: Number(sessionId) },
  );
}

/**
 * Chẩn đoán khi không có dòng: có CDHA không, CN_FILES theo CR_Session vs SubSession, mẫu file bệnh nhân.
 */
async function debugCnFilesLookup(fileNum, sessionId) {
  const params = { fileNum: String(fileNum), sessionId: Number(sessionId) };
  const filePred = fileNumMatchSql('pat');

  const imaging = await db.executeQuery(
    `
    SELECT COUNT(1) AS cnt
    FROM dbo.CN_ImagingResult r
    INNER JOIN dbo.ViewImagingResult v ON v.Id = r.Id
    WHERE r.DeletedDate IS NULL
      AND LTRIM(RTRIM(CONVERT(VARCHAR(50), v.FileNum))) = LTRIM(RTRIM(@fileNum))
      AND v.SessionId = @sessionId
    `,
    params,
  );

  const byCrSession = await db.executeQuery(
    `
    SELECT COUNT(1) AS cnt
    FROM dbo.CN_FILES f
    INNER JOIN dbo.CR_SubSession ss ON ss.Id = f.SubSessionId
    INNER JOIN dbo.CR_Session s ON s.Id = ss.SessionId
    INNER JOIN dbo.CR_Patient pat ON pat.ContactId = s.PatientId
    WHERE ${filePred}
      AND s.Id = @sessionId
      AND f.DeletedDate IS NULL
      AND ss.DeletedDate IS NULL
    `,
    params,
  );

  const bySubSession = await db.executeQuery(
    `
    SELECT COUNT(1) AS cnt
    FROM dbo.CN_FILES f
    INNER JOIN dbo.CR_SubSession ss ON ss.Id = f.SubSessionId
    INNER JOIN dbo.CR_Session s ON s.Id = ss.SessionId
    INNER JOIN dbo.CR_Patient pat ON pat.ContactId = s.PatientId
    WHERE ${filePred}
      AND ss.Id = @sessionId
      AND f.DeletedDate IS NULL
      AND ss.DeletedDate IS NULL
    `,
    params,
  );

  const patientTop = await db.executeQuery(
    `
    SELECT TOP 15
      f.ID,
      f.SubSessionId,
      ss.SessionId AS CrSessionId,
      f.DocTitle,
      f.FileName,
      f.DeletedDate,
      f.CreatedDate
    FROM dbo.CN_FILES f
    INNER JOIN dbo.CR_SubSession ss ON ss.Id = f.SubSessionId
    INNER JOIN dbo.CR_Session s ON s.Id = ss.SessionId
    INNER JOIN dbo.CR_Patient pat ON pat.ContactId = s.PatientId
    WHERE ${filePred}
    ORDER BY f.CreatedDate DESC
    `,
    { fileNum: String(fileNum) },
  );

  const ssProbe = await db.executeQuery(
    `SELECT Id, SessionId, DeletedDate FROM dbo.CR_SubSession WHERE Id = @sessionId`,
    { sessionId: Number(sessionId) },
  );

  const sProbe = await db.executeQuery(
    `SELECT Id, PatientId FROM dbo.CR_Session WHERE Id = @sessionId`,
    { sessionId: Number(sessionId) },
  );

  return {
    imagingRowCount: imaging[0]?.cnt ?? 0,
    cnFilesMatchCrSessionId: byCrSession[0]?.cnt ?? 0,
    cnFilesMatchSubSessionId: bySubSession[0]?.cnt ?? 0,
    crSubSessionWhereIdEqualsSessionParam: ssProbe,
    crSessionWhereIdEqualsSessionParam: sProbe,
    cnFilesTopForPatientFileNum: patientTop,
  };
}

module.exports = {
  guessStoredPathFromRow,
  getCnFilesByFileNumAndSessionId,
  debugCnFilesLookup,
  PATH_COLUMN_CANDIDATES,
};
