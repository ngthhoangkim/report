const db = require('../config/database');

/**
 * Imaging theo FileNum + SessionId (một phiên khám).
 */
const SQL_REPORT_LIST_BY_FILE_AND_SESSION = `
  SELECT
    v.FileNum,
    v.SessionId,
    v.PatientName,
    v.Dob,
    v.Sex AS Gender,
    v.Street AS Address,
    r.Conclusion,
    v.Doctor,
    r.DoctorId,
    v.RequestedDoctor,
    r.CreatedDate AS ngayKham,
    r.FileName,
    r.RequestId,
    r.Id AS ImagingResultId,
    r.SampleNumber,
    d.ResultData,
    d.ConclusionData,
    d.SuggestionData,
    r.TemplateFile,
    r.PathologyType
  FROM dbo.CN_ImagingResult r
  INNER JOIN dbo.CN_ImagingResultData d ON r.Id = d.ImagingResultId
  INNER JOIN dbo.ViewImagingResult v ON v.Id = r.Id
  WHERE v.FileNum = @fileNum
    AND v.SessionId = @sessionId
    AND r.DeletedDate IS NULL
  ORDER BY r.CreatedDate ASC, r.Id ASC
`;

function normalizeRecord(row) {
  const fileNum = row.FileNum != null ? String(row.FileNum) : '';
  const sessionId = row.SessionId != null ? Number(row.SessionId) : null;
  const sample = row.SampleNumber != null ? String(row.SampleNumber).trim() : '';
  const itemNumForPlaceholders = sample || fileNum;
  return {
    fileNum,
    sessionId,
    sampleNumber: sample,
    /** <<ItemNum>> / <<SampleNumber>> */
    itemNum: itemNumForPlaceholders,
    patientName: row.PatientName || '',
    dob: row.Dob,
    gender: row.Gender || '',
    address: row.Address || '',
    conclusion: row.Conclusion || '',
    doctor: row.Doctor || '',
    doctorId: row.DoctorId != null ? Number(row.DoctorId) : null,
    requestedDoctor: row.RequestedDoctor || '',
    ngayKham: row.ngayKham ?? row.NgayKham,
    fileName: row.FileName || '',
    requestId: row.RequestId != null ? Number(row.RequestId) : null,
    imagingResultId: row.ImagingResultId,
    resultData: row.ResultData,
    conclusionData: row.ConclusionData,
    suggestionData: row.SuggestionData,
    templateFile: row.TemplateFile || '',
    pathologyType: Number(row.PathologyType) || 0,
    doctorQualification: '',
  };
}

async function getReportDataListByFileNumAndSessionId(fileNum, sessionId) {
  const rows = await db.executeQuery(SQL_REPORT_LIST_BY_FILE_AND_SESSION, {
    fileNum,
    sessionId,
  });
  const records = rows.map(normalizeRecord);
  await enrichDoctorQualifications(records);
  return records;
}

async function enrichDoctorQualifications(records) {
  const ids = [
    ...new Set(
      records
        .map((r) => r.doctorId)
        .filter((id) => id != null && !Number.isNaN(id)),
    ),
  ];
  if (ids.length === 0) return;

  const params = {};
  const placeholders = ids.map((id, idx) => {
    const key = `id${idx}`;
    params[key] = id;
    return `@${key}`;
  });
  const sql = `
    SELECT ContactId AS Id, Qualification
    FROM ViewStaff
    WHERE ContactId IN (${placeholders.join(',')})
  `;
  const rows = await db.executeQuery(sql, params);
  const map = new Map();
  for (const row of rows) {
    map.set(Number(row.Id), row.Qualification ? String(row.Qualification) : '');
  }
  for (const r of records) {
    if (r.doctorId != null && map.has(r.doctorId)) {
      r.doctorQualification = map.get(r.doctorId) || '';
    }
  }
}

/**
 * Danh sách tên file ảnh cần đưa vào báo cáo, khớp tên khi giải nén ZIP (reportDocumentService).
 * Mặc định: chỉ Printed = 1. PRINTED_IMAGES_ONLY=false: mọi ảnh có cùng ResultId.
 */
async function getPrintedImageFilenames(resultId) {
  const strict = String(process.env.PRINTED_IMAGES_ONLY || 'true').toLowerCase() !== 'false';

  if (!strict) {
    const rows = await db.executeQuery(
      `SELECT Filename
       FROM CN_PathologyImage
       WHERE ResultId = @resultId
       ORDER BY CreatedDate`,
      { resultId },
    );
    return rows.map((r) => r.Filename).filter(Boolean);
  }

  const rows = await db.executeQuery(
    `SELECT Filename
     FROM CN_PathologyImage
     WHERE ResultId = @resultId AND Printed = 1
     ORDER BY CreatedDate`,
    { resultId },
  );
  return rows.map((r) => r.Filename).filter(Boolean);
}

const SQL_LATEST_PACS_PER_REQUEST_FOR_SESSION = `
WITH Req AS (
  SELECT DISTINCT r.RequestId
  FROM dbo.CN_ImagingResult r
  INNER JOIN dbo.ViewImagingResult v ON v.Id = r.Id
  WHERE v.FileNum = @fileNum
    AND v.SessionId = @sessionId
    AND r.DeletedDate IS NULL
    AND r.RequestId IS NOT NULL
),
Ranked AS (
  SELECT
    p.RequestId,
    p.Id,
    p.AccessCode,
    p.ViewURL,
    p.FileResultURL,
    p.CreatedDate,
    ROW_NUMBER() OVER (PARTITION BY p.RequestId ORDER BY p.Id DESC) AS rn
  FROM dbo.PACS_RequestInfo p
  INNER JOIN Req ON Req.RequestId = p.RequestId
)
SELECT
  RequestId,
  Id AS PacsRequestInfoId,
  AccessCode,
  ViewURL,
  FileResultURL,
  CreatedDate
FROM Ranked
WHERE rn = 1
ORDER BY RequestId;
`;

/**
 * Mỗi RequestId trong phiên: một dòng PACS_RequestInfo mới nhất (theo Id).
 */
async function getLatestPacsInfoForSession(fileNum, sessionId) {
  const rows = await db.executeQuery(SQL_LATEST_PACS_PER_REQUEST_FOR_SESSION, {
    fileNum,
    sessionId,
  });
  return rows.map((row) => ({
    requestId: row.RequestId != null ? Number(row.RequestId) : null,
    pacsRequestInfoId: row.PacsRequestInfoId != null ? Number(row.PacsRequestInfoId) : null,
    accessCode: row.AccessCode != null ? String(row.AccessCode) : '',
    viewUrl: row.ViewURL != null ? String(row.ViewURL).trim() : '',
    fileResultUrl: row.FileResultURL != null ? String(row.FileResultURL).trim() : '',
    createdDate: row.CreatedDate,
  })).filter((r) => r.requestId != null);
}

module.exports = {
  getReportDataListByFileNumAndSessionId,
  getLatestPacsInfoForSession,
  getPrintedImageFilenames,
  /**
   * Polling: lấy danh sách session có imaging result cập nhật mới hơn checkpoint.
   * @returns {Array<{fileNum:string,sessionId:number,imagingResultId:number,updatedAt:Date}>}
   */
  async getUpdatedSessionsSince(lastUpdatedAt, lastImagingResultId, limit = 200) {
    const rows = await db.executeQuery(
      `
      SELECT TOP (@limit)
        v.FileNum,
        v.SessionId,
        r.Id AS ImagingResultId,
        COALESCE(r.UpdatedDate, r.CreatedDate) AS UpdatedAt
      FROM dbo.CN_ImagingResult r
      INNER JOIN dbo.ViewImagingResult v ON v.Id = r.Id
      WHERE r.DeletedDate IS NULL
        AND (
          COALESCE(r.UpdatedDate, r.CreatedDate) > @lastUpdatedAt
          OR (
            COALESCE(r.UpdatedDate, r.CreatedDate) = @lastUpdatedAt
            AND r.Id > @lastImagingResultId
          )
        )
      ORDER BY COALESCE(r.UpdatedDate, r.CreatedDate) ASC, r.Id ASC
      `,
      {
        limit,
        lastUpdatedAt,
        lastImagingResultId,
      },
    );
    return rows.map((r) => ({
      fileNum: r.FileNum != null ? String(r.FileNum) : '',
      sessionId: r.SessionId != null ? Number(r.SessionId) : null,
      imagingResultId: r.ImagingResultId != null ? Number(r.ImagingResultId) : null,
      updatedAt: r.UpdatedAt,
    })).filter((r) => r.fileNum && r.sessionId != null && r.imagingResultId != null);
  },
};
