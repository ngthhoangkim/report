const db = require('../config/database');
const { vitalRowHasData } = require('../utils/prescriptionPayloadHelpers');
const { mergeSessionImagingDiagnoses } = require('../utils/imagingDiagnosisPlain');

/** Lớp JS: bỏ trùng rx.ID (giữ thứ tự) nếu SQL chưa lọc. Tắt: PRESCRIPTION_RX_DEDUPE_BY_ID=false */
function dedupeRxRowsById(rows) {
  const v = String(process.env.PRESCRIPTION_RX_DEDUPE_BY_ID || 'true').toLowerCase();
  if (v === 'false' || v === '0' || v === 'off') return rows || [];
  if (!rows || !rows.length) return rows || [];
  const seen = new Set();
  const out = [];
  for (const r of rows) {
    const id = r.ID;
    if (id != null && String(id).trim() !== '') {
      const k = String(id);
      if (seen.has(k)) continue;
      seen.add(k);
    }
    out.push(r);
  }
  return out;
}

function rxDedupeByIdEnabled() {
  const v = String(process.env.PRESCRIPTION_RX_DEDUPE_BY_ID || 'true').toLowerCase();
  return v !== 'false' && v !== '0' && v !== 'off';
}

const VITALS_SELECT = `
      ge.Pulse,
      ge.Temp,
      ge.Systolic,
      ge.Diastolic,
      ge.Resp,
      ge.CreatedDate`;

/**
 * Sinh hiệu: thử nhiều cách khớp SessionId / SubSessionId (tùy cấu hình CR_SubSession + CN_GeneralExam).
 */
async function fetchVitalsForSession(sessionId) {
  const sid = Number(sessionId);
  const attempts = [
    {
      key: 'CN_GeneralExam_via_ss.SessionId',
      sql: `
    SELECT TOP 1 ${VITALS_SELECT}
    FROM dbo.CN_GeneralExam ge WITH (NOLOCK)
    INNER JOIN dbo.CR_SubSession ss WITH (NOLOCK) ON ss.Id = ge.SubSessionId
    WHERE ss.SessionId = @sessionId
    ORDER BY ge.CreatedDate DESC`,
    },
    {
      key: 'CN_GeneralExam.SubSessionId_eq_param',
      sql: `
    SELECT TOP 1 ${VITALS_SELECT}
    FROM dbo.CN_GeneralExam ge WITH (NOLOCK)
    WHERE ge.SubSessionId = @sessionId
    ORDER BY ge.CreatedDate DESC`,
    },
    {
      key: 'CN_GeneralExam_via_ss.Id_eq_param',
      sql: `
    SELECT TOP 1 ${VITALS_SELECT}
    FROM dbo.CN_GeneralExam ge WITH (NOLOCK)
    INNER JOIN dbo.CR_SubSession ss WITH (NOLOCK) ON ss.Id = ge.SubSessionId
    WHERE ss.Id = @sessionId
    ORDER BY ge.CreatedDate DESC`,
    },
    {
      key: 'CN_GeneralExam.SessionId_eq_param',
      sql: `
    SELECT TOP 1 ${VITALS_SELECT}
    FROM dbo.CN_GeneralExam ge WITH (NOLOCK)
    WHERE ge.SessionId = @sessionId
    ORDER BY ge.CreatedDate DESC`,
    },
  ];

  for (const a of attempts) {
    try {
      const rows = await db.executeQuery(a.sql, { sessionId: sid }, { silent: true });
      const row = rows[0];
      if (vitalRowHasData(row)) return { vitals: row, vitalsSource: a.key };
    } catch (_) {
      /* bỏ qua nếu bảng/cột khác schema */
    }
  }
  return { vitals: null, vitalsSource: null };
}

/**
 * Mã hiển thị ở ô Barcode trên mẫu toa: CN_Prescription.ScriptNo (cùng đơn, nhiều dòng thuốc → cùng ScriptNo).
 *
 * Ưu tiên khớp theo SubSessionId (nếu có) để tránh trộn nhiều toa trong cùng SessionId.
 * Fallback: khớp theo CR_SubSession.SessionId = sessionId (trường hợp chỉ biết session).
 */
async function tryFetchPrescriptionScriptNoBarcode({ patientId, sessionId, subSessionId }) {
  const pid = Number(patientId);
  const sid = sessionId != null ? Number(sessionId) : null;
  const ssid = subSessionId != null ? Number(subSessionId) : null;
  if (!patientId || Number.isNaN(pid)) return '';

  const attempts = [];

  if (ssid != null && Number.isFinite(ssid)) {
    attempts.push({
      sql: `
      SELECT TOP 1 pr.ScriptNo
      FROM dbo.CN_Prescription pr WITH (NOLOCK)
      WHERE pr.PatientID = @patientId
        AND pr.SubSessionId = @subSessionId
        AND pr.DeletedDate IS NULL
        AND pr.ScriptNo IS NOT NULL
        AND LTRIM(RTRIM(pr.ScriptNo)) <> ''
      ORDER BY pr.CreatedDate DESC`,
      params: { patientId: pid, subSessionId: ssid },
    });
  }

  if (sid != null && Number.isFinite(sid)) {
    attempts.push({
      sql: `
      SELECT TOP 1 pr.ScriptNo
      FROM dbo.CN_Prescription pr WITH (NOLOCK)
      INNER JOIN dbo.CR_SubSession ss WITH (NOLOCK) ON ss.Id = pr.SubSessionId
      WHERE pr.PatientID = @patientId
        AND ss.SessionId = @sessionId
        AND pr.DeletedDate IS NULL
        AND pr.ScriptNo IS NOT NULL
        AND LTRIM(RTRIM(pr.ScriptNo)) <> ''
      ORDER BY pr.CreatedDate DESC`,
      params: { patientId: pid, sessionId: sid },
    });
  }

  for (const a of attempts) {
    try {
      const rows = await db.executeQuery(a.sql, a.params, { silent: true });
      const v = rows[0]?.ScriptNo;
      if (v != null && String(v).trim() !== '') return String(v).trim();
    } catch (_) {
      /* schema khác nhau */
    }
  }
  return '';
}

/** Cột có thể chứa mã vạch BN trên PersonView (dự phòng khi không có ScriptNo đơn thuốc). */
const PATIENT_BARCODE_COLUMNS = [
  'Barcode',
  'PatientBarcode',
  'BarCode',
  'MaVach',
  'MaVachBN',
  'ScanCode',
  'Code128',
];

async function tryFetchPatientBarcode(fileNum) {
  const fn = String(fileNum);
  const inList = PATIENT_BARCODE_COLUMNS.map((c) => `'${c.replace(/'/g, "''")}'`).join(', ');
  let existing = [];
  try {
    existing = await db.executeQuery(
      `
      SELECT COLUMN_NAME
      FROM INFORMATION_SCHEMA.COLUMNS
      WHERE TABLE_SCHEMA = 'dbo'
        AND TABLE_NAME = 'PersonView'
        AND COLUMN_NAME IN (${inList})
      `,
    );
  } catch (_) {
    return '';
  }
  const names = new Set(existing.map((r) => r.COLUMN_NAME));
  for (const col of PATIENT_BARCODE_COLUMNS) {
    if (!names.has(col)) continue;
    const rows = await db.executeQuery(
      `
      SELECT TOP 1 [${col}] AS BarcodeValue
      FROM dbo.PersonView WITH (NOLOCK)
      WHERE FileNum = @fileNum
      `,
      { fileNum: fn },
    );
    const v = rows[0]?.BarcodeValue;
    if (v != null && String(v).trim() !== '') return String(v).trim();
  }
  return '';
}

/**
 * Lấy danh sách SubSessionId trong một phiên có đơn thuốc (ViewRX).
 * Trả về theo thứ tự thời gian kê (CreatedDate) để merge PDF ổn định.
 */
async function getDistinctPrescriptionSubSessionIds(fileNum, sessionId) {
  const fn = String(fileNum);
  const sid = Number(sessionId);
  const rows = await db.executeQuery(
    `
    SELECT
      rx.SubSessionId,
      MIN(rx.CreatedDate) AS FirstAt
    FROM dbo.ViewRX rx WITH (NOLOCK)
    INNER JOIN dbo.PersonView p WITH (NOLOCK) ON p.ContactId = rx.PatientID
    WHERE rx.DeletedDate IS NULL
      AND p.FileNum = @fileNum
      AND rx.SessionId = @sessionId
      AND rx.SubSessionId IS NOT NULL
    GROUP BY rx.SubSessionId
    ORDER BY FirstAt ASC, rx.SubSessionId ASC
    `,
    { fileNum: fn, sessionId: sid },
  );
  return rows
    .map((r) => (r.SubSessionId != null ? Number(r.SubSessionId) : null))
    .filter((x) => x != null && Number.isFinite(x));
}

/**
 * Lấy danh sách ScriptNo trong một phiên có đơn thuốc.
 * Nguồn: join ViewRX(ID) ↔ CN_Prescription(ID) để map ScriptNo đúng với dòng thuốc.
 */
async function getDistinctPrescriptionScriptNos(fileNum, sessionId) {
  const fn = String(fileNum);
  const sid = Number(sessionId);
  const rows = await db.executeQuery(
    `
    SELECT
      pr.ScriptNo,
      MIN(pr.CreatedDate) AS FirstAt
    FROM dbo.ViewRX rx WITH (NOLOCK)
    INNER JOIN dbo.PersonView p WITH (NOLOCK) ON p.ContactId = rx.PatientID
    INNER JOIN dbo.CN_Prescription pr WITH (NOLOCK) ON pr.ID = rx.ID
    WHERE rx.DeletedDate IS NULL
      AND pr.DeletedDate IS NULL
      AND p.FileNum = @fileNum
      AND rx.SessionId = @sessionId
      AND pr.ScriptNo IS NOT NULL
      AND LTRIM(RTRIM(pr.ScriptNo)) <> ''
    GROUP BY pr.ScriptNo
    ORDER BY FirstAt ASC, pr.ScriptNo ASC
    `,
    { fileNum: fn, sessionId: sid },
  );
  return rows.map((r) => String(r.ScriptNo).trim()).filter(Boolean);
}

/**
 * Dữ liệu cho mẫu ToaThuoc (FileNum + SessionId).
 * Nếu có `subSessionId` → lọc dữ liệu toa (rx + chẩn đoán + barcode ScriptNo) theo SubSessionId.
 */
async function getPrescriptionReportContext(fileNum, sessionId, subSessionId = null) {
  const fn = String(fileNum);
  const sid = Number(sessionId);
  const ssid = subSessionId != null ? Number(subSessionId) : null;

  const patients = await db.executeQuery(
    `
    SELECT TOP 1
      p.ContactId,
      p.FileNum,
      p.FullName,
      p.Sex,
      p.Dob,
      p.AddressNo,
      p.Street,
      p.Ward,
      p.District,
      p.City
    FROM dbo.PersonView p WITH (NOLOCK)
    WHERE p.FileNum = @fileNum
    `,
    { fileNum: fn },
  );
  const patient = patients[0] || null;

  const vitalsKey = ssid != null && Number.isFinite(ssid) ? ssid : sid;
  const { vitals, vitalsSource } = await fetchVitalsForSession(vitalsKey);
  let patientBarcode = await tryFetchPrescriptionScriptNoBarcode({
    patientId: patient?.ContactId,
    sessionId: sid,
    subSessionId: ssid,
  });
  if (!patientBarcode) patientBarcode = await tryFetchPatientBarcode(fn);

  let dxRows = [];
  if (ssid != null && Number.isFinite(ssid)) {
    dxRows = await db.executeQuery(
      `
      SELECT TOP 3 Notes, ICDCode1, ICDCode2, CreatedDate
      FROM dbo.ViewCurrentDiagnosis WITH (NOLOCK)
      WHERE SubSessionId = @subSessionId
      ORDER BY CreatedDate DESC
      `,
      { subSessionId: ssid },
      { silent: true },
    );
  }
  if (!dxRows.length) {
    dxRows = await db.executeQuery(
      `
      SELECT TOP 3 Notes, ICDCode1, ICDCode2, CreatedDate
      FROM dbo.ViewCurrentDiagnosis WITH (NOLOCK)
      WHERE SessionId = @sessionId
      ORDER BY CreatedDate DESC
      `,
      { sessionId: sid },
      { silent: true },
    );
  }

  /** Chẩn đoán giống báo cáo CDHA: Conclusion / ConclusionData từng CN_ImagingResult trong phiên */
  let diagnosisFromImagingReport = '';
  try {
    const imgDxRows = await db.executeQuery(
      `
      SELECT r.Conclusion, d.ConclusionData
      FROM dbo.CN_ImagingResult r WITH (NOLOCK)
      INNER JOIN dbo.CN_ImagingResultData d WITH (NOLOCK) ON r.Id = d.ImagingResultId
      INNER JOIN dbo.ViewImagingResult v WITH (NOLOCK) ON v.Id = r.Id
      WHERE v.FileNum = @fileNum
        AND v.SessionId = @sessionId
        AND r.DeletedDate IS NULL
      ORDER BY r.CreatedDate ASC, r.Id ASC
      `,
      { fileNum: fn, sessionId: sid },
      { silent: true },
    );
    diagnosisFromImagingReport = mergeSessionImagingDiagnoses(imgDxRows);
  } catch (_) {
    diagnosisFromImagingReport = '';
  }

  // Một dòng PDF = một rx.ID (không trùng). ROW_NUMBER ở SQL + dedupe JS dự phòng. Dòng in: nhân đoạn mẫu __RX_C* trong docx.
  const rxSqlDeduped = `
    WITH RawRx AS (
      SELECT
        rx.ID,
        rx.ITEM,
        rx.Property,
        rx.DOSE,
        rx.UnitUsage,
        rx.FREQUENCY,
        rx.INSTRUCTIONS,
        rx.QUANTITY,
        rx.REPEATS,
        rx.BeginDate,
        rx.FinishDate,
        rx.DocName,
        rx.UNITNAME
      FROM dbo.ViewRX rx WITH (NOLOCK)
      INNER JOIN dbo.PersonView p WITH (NOLOCK) ON p.ContactId = rx.PatientID
      WHERE rx.DeletedDate IS NULL
        AND p.FileNum = @fileNum
        AND ${
          ssid != null && Number.isFinite(ssid)
            ? 'rx.SubSessionId = @subSessionId'
            : 'rx.SessionId = @sessionId'
        }
    ),
    Ranked AS (
      SELECT
        ID, ITEM, Property, DOSE, UnitUsage, FREQUENCY, INSTRUCTIONS, QUANTITY, REPEATS,
        BeginDate, FinishDate, DocName, UNITNAME,
        ROW_NUMBER() OVER (PARTITION BY ID ORDER BY BeginDate ASC, ID ASC) AS rn
      FROM RawRx
      WHERE ID IS NOT NULL
    )
    SELECT
      ID, ITEM, Property, DOSE, UnitUsage, FREQUENCY, INSTRUCTIONS, QUANTITY, REPEATS,
      BeginDate, FinishDate, DocName, UNITNAME
    FROM (
      SELECT ID, ITEM, Property, DOSE, UnitUsage, FREQUENCY, INSTRUCTIONS, QUANTITY, REPEATS,
        BeginDate, FinishDate, DocName, UNITNAME
      FROM Ranked
      WHERE rn = 1
      UNION ALL
      SELECT ID, ITEM, Property, DOSE, UnitUsage, FREQUENCY, INSTRUCTIONS, QUANTITY, REPEATS,
        BeginDate, FinishDate, DocName, UNITNAME
      FROM RawRx
      WHERE ID IS NULL
    ) u
    ORDER BY u.BeginDate ASC, u.ID ASC
  `;

  const rxSqlPlain = `
    SELECT
      rx.ID,
      rx.ITEM,
      rx.Property,
      rx.DOSE,
      rx.UnitUsage,
      rx.FREQUENCY,
      rx.INSTRUCTIONS,
      rx.QUANTITY,
      rx.REPEATS,
      rx.BeginDate,
      rx.FinishDate,
      rx.DocName,
      rx.UNITNAME
    FROM dbo.ViewRX rx WITH (NOLOCK)
    INNER JOIN dbo.PersonView p WITH (NOLOCK) ON p.ContactId = rx.PatientID
    WHERE rx.DeletedDate IS NULL
      AND p.FileNum = @fileNum
      AND ${
        ssid != null && Number.isFinite(ssid)
          ? 'rx.SubSessionId = @subSessionId'
          : 'rx.SessionId = @sessionId'
      }
    ORDER BY rx.BeginDate ASC, rx.ID ASC
  `;

  let rxRows;
  if (rxDedupeByIdEnabled()) {
    try {
      rxRows = await db.executeQuery(
        rxSqlDeduped,
        { fileNum: fn, sessionId: sid, subSessionId: ssid },
        { silent: true },
      );
    } catch (_) {
      rxRows = await db.executeQuery(rxSqlPlain, { fileNum: fn, sessionId: sid, subSessionId: ssid });
    }
  } else {
    rxRows = await db.executeQuery(rxSqlPlain, { fileNum: fn, sessionId: sid, subSessionId: ssid });
  }

  const rxLines = rxDedupeByIdEnabled() ? dedupeRxRowsById(rxRows) : rxRows || [];

  return {
    patient,
    vitals,
    vitalsSource,
    patientBarcode,
    diagnoses: dxRows,
    diagnosisFromImagingReport,
    rxLines,
    subSessionId: ssid,
  };
}

/**
 * Context theo ScriptNo (dùng để tách nhiều toa trong cùng SessionId).
 * - Lọc dòng thuốc theo ScriptNo qua join ViewRX(ID) ↔ CN_Prescription(ID)
 * - SubSessionId lấy theo ViewRX để kéo sinh hiệu/chẩn đoán chính xác (nếu có)
 */
async function getPrescriptionReportContextByScriptNo(fileNum, sessionId, scriptNo) {
  const fn = String(fileNum);
  const sid = Number(sessionId);
  const sn = String(scriptNo || '').trim();
  if (!sn) throw new Error('Thiếu ScriptNo');

  const patients = await db.executeQuery(
    `
    SELECT TOP 1
      p.ContactId,
      p.FileNum,
      p.FullName,
      p.Sex,
      p.Dob,
      p.AddressNo,
      p.Street,
      p.Ward,
      p.District,
      p.City
    FROM dbo.PersonView p WITH (NOLOCK)
    WHERE p.FileNum = @fileNum
    `,
    { fileNum: fn },
  );
  const patient = patients[0] || null;

  // Resolve SubSessionId cho ScriptNo để lấy sinh hiệu/chẩn đoán (nếu tồn tại).
  const subRows = await db.executeQuery(
    `
    SELECT TOP 1 rx.SubSessionId
    FROM dbo.ViewRX rx WITH (NOLOCK)
    INNER JOIN dbo.PersonView p WITH (NOLOCK) ON p.ContactId = rx.PatientID
    INNER JOIN dbo.CN_Prescription pr WITH (NOLOCK) ON pr.ID = rx.ID
    WHERE rx.DeletedDate IS NULL
      AND pr.DeletedDate IS NULL
      AND p.FileNum = @fileNum
      AND rx.SessionId = @sessionId
      AND pr.ScriptNo = @scriptNo
    ORDER BY rx.CreatedDate ASC, rx.ID ASC
    `,
    { fileNum: fn, sessionId: sid, scriptNo: sn },
    { silent: true },
  );
  const ssid = subRows[0]?.SubSessionId != null ? Number(subRows[0].SubSessionId) : null;

  const vitalsKey = ssid != null && Number.isFinite(ssid) ? ssid : sid;
  const { vitals, vitalsSource } = await fetchVitalsForSession(vitalsKey);

  // Barcode: dùng chính ScriptNo (ổn định theo toa); fallback patient barcode nếu ScriptNo rỗng.
  let patientBarcode = sn;
  if (!patientBarcode) patientBarcode = await tryFetchPatientBarcode(fn);

  let dxRows = [];
  if (ssid != null && Number.isFinite(ssid)) {
    dxRows = await db.executeQuery(
      `
      SELECT TOP 3 Notes, ICDCode1, ICDCode2, CreatedDate
      FROM dbo.ViewCurrentDiagnosis WITH (NOLOCK)
      WHERE SubSessionId = @subSessionId
      ORDER BY CreatedDate DESC
      `,
      { subSessionId: ssid },
      { silent: true },
    );
  }
  if (!dxRows.length) {
    dxRows = await db.executeQuery(
      `
      SELECT TOP 3 Notes, ICDCode1, ICDCode2, CreatedDate
      FROM dbo.ViewCurrentDiagnosis WITH (NOLOCK)
      WHERE SessionId = @sessionId
      ORDER BY CreatedDate DESC
      `,
      { sessionId: sid },
      { silent: true },
    );
  }

  let diagnosisFromImagingReport = '';
  try {
    const imgDxRows = await db.executeQuery(
      `
      SELECT r.Conclusion, d.ConclusionData
      FROM dbo.CN_ImagingResult r WITH (NOLOCK)
      INNER JOIN dbo.CN_ImagingResultData d WITH (NOLOCK) ON r.Id = d.ImagingResultId
      INNER JOIN dbo.ViewImagingResult v WITH (NOLOCK) ON v.Id = r.Id
      WHERE v.FileNum = @fileNum
        AND v.SessionId = @sessionId
        AND r.DeletedDate IS NULL
      ORDER BY r.CreatedDate ASC, r.Id ASC
      `,
      { fileNum: fn, sessionId: sid },
      { silent: true },
    );
    diagnosisFromImagingReport = mergeSessionImagingDiagnoses(imgDxRows);
  } catch (_) {
    diagnosisFromImagingReport = '';
  }

  // Rx lines theo ScriptNo
  const rxSql = `
    SELECT
      rx.ID,
      rx.ITEM,
      rx.Property,
      rx.DOSE,
      rx.UnitUsage,
      rx.FREQUENCY,
      rx.INSTRUCTIONS,
      rx.QUANTITY,
      rx.REPEATS,
      rx.BeginDate,
      rx.FinishDate,
      rx.DocName,
      rx.UNITNAME
    FROM dbo.ViewRX rx WITH (NOLOCK)
    INNER JOIN dbo.PersonView p WITH (NOLOCK) ON p.ContactId = rx.PatientID
    INNER JOIN dbo.CN_Prescription pr WITH (NOLOCK) ON pr.ID = rx.ID
    WHERE rx.DeletedDate IS NULL
      AND pr.DeletedDate IS NULL
      AND p.FileNum = @fileNum
      AND rx.SessionId = @sessionId
      AND pr.ScriptNo = @scriptNo
    ORDER BY rx.BeginDate ASC, rx.ID ASC
  `;
  const rxRows = await db.executeQuery(rxSql, { fileNum: fn, sessionId: sid, scriptNo: sn });
  const rxLines = rxDedupeByIdEnabled() ? dedupeRxRowsById(rxRows) : rxRows || [];

  return {
    patient,
    vitals,
    vitalsSource,
    patientBarcode,
    diagnoses: dxRows,
    diagnosisFromImagingReport,
    rxLines,
    subSessionId: ssid,
    scriptNo: sn,
  };
}

/**
 * Gỡ (FileNum, SessionId) cho đơn thuốc: truyền một hoặc cả hai.
 * - Chỉ SessionId: lấy FileNum từ ViewRX (cùng phiên).
 * - Chỉ FileNum: lấy phiên mới nhất có ViewRX (BeginDate/Id).
 * - Cả hai: kiểm tra còn ít nhất một dòng ViewRX.
 */
async function resolveFileNumSessionIdForPrescription({ fileNum, sessionId }) {
  const fnRaw = fileNum != null && String(fileNum).trim() !== '' ? String(fileNum).trim() : null;
  const sidRaw =
    sessionId != null && String(sessionId).trim() !== '' ? String(sessionId).trim() : null;

  if (!fnRaw && !sidRaw) {
    throw new Error('Thiếu fileNum và sessionId — cần ít nhất một trong hai.');
  }

  if (fnRaw && sidRaw) {
    const sid = Number(sidRaw);
    if (!Number.isFinite(sid)) throw new Error(`sessionId không hợp lệ: ${sidRaw}`);
    const check = await db.executeQuery(
      `
      SELECT TOP 1 1 AS ok
      FROM dbo.ViewRX rx WITH (NOLOCK)
      INNER JOIN dbo.PersonView p WITH (NOLOCK) ON p.ContactId = rx.PatientID
      WHERE rx.DeletedDate IS NULL
        AND p.FileNum = @fileNum
        AND rx.SessionId = @sessionId
      `,
      { fileNum: fnRaw, sessionId: sid },
    );
    if (!check.length) {
      throw new Error(`Không có ViewRX cho FileNum=${fnRaw}, SessionId=${sid}`);
    }
    return { fileNum: fnRaw, sessionId: sid };
  }

  if (sidRaw) {
    const sid = Number(sidRaw);
    if (!Number.isFinite(sid)) throw new Error(`sessionId không hợp lệ: ${sidRaw}`);
    // Avoid heavy join on large ViewRX/PersonView when only SessionId is known.
    // Step 1: pick one PatientID in this session (ViewRX should be indexed by SessionId).
    const rx = await db.executeQuery(
      `
      SELECT TOP 1 rx.PatientID
      FROM dbo.ViewRX rx WITH (NOLOCK)
      WHERE rx.DeletedDate IS NULL
        AND rx.SessionId = @sessionId
      ORDER BY rx.ID DESC
      `,
      { sessionId: sid },
    );
    const pid = rx[0]?.PatientID;
    if (pid == null) {
      throw new Error(`Không có ViewRX cho SessionId=${sid}`);
    }
    // Step 2: resolve FileNum by ContactId (PatientID)
    const pv = await db.executeQuery(
      `
      SELECT TOP 1 p.FileNum
      FROM dbo.PersonView p WITH (NOLOCK)
      WHERE p.ContactId = @contactId
      `,
      { contactId: pid },
    );
    const fileNumResolved = pv[0]?.FileNum;
    if (fileNumResolved == null || String(fileNumResolved).trim() === '') {
      throw new Error(`Không tìm thấy FileNum từ PersonView cho SessionId=${sid} (ContactId=${pid})`);
    }
    return { fileNum: String(fileNumResolved).trim(), sessionId: sid };
  }

  const rows = await db.executeQuery(
    `
    SELECT TOP 1 rx.SessionId
    FROM dbo.ViewRX rx WITH (NOLOCK)
    INNER JOIN dbo.PersonView p WITH (NOLOCK) ON p.ContactId = rx.PatientID
    WHERE rx.DeletedDate IS NULL
      AND p.FileNum = @fileNum
    ORDER BY rx.BeginDate DESC, rx.ID DESC
    `,
    { fileNum: fnRaw },
  );
  if (!rows[0] || rows[0].SessionId == null) {
    throw new Error(`Không có ViewRX cho FileNum=${fnRaw}`);
  }
  return { fileNum: fnRaw, sessionId: Number(rows[0].SessionId) };
}

/**
 * Polling: dòng ViewRX mới hơn checkpoint (CreatedDate + ID).
 * @returns {Promise<Array<{fileNum:string,sessionId:number,prescriptionRowId:number,updatedAt:Date}>>}
 */
async function getUpdatedPrescriptionRowsSince(lastUpdatedAt, lastPrescriptionRowId, limit = 200) {
  const rows = await db.executeQuery(
    `
    SELECT TOP (@limit)
      LTRIM(RTRIM(CONVERT(VARCHAR(50), p.FileNum))) AS FileNum,
      rx.SessionId,
      rx.ID AS PrescriptionRowId,
      rx.CreatedDate AS UpdatedAt
    FROM dbo.ViewRX rx WITH (NOLOCK)
    INNER JOIN dbo.PersonView p WITH (NOLOCK) ON p.ContactId = rx.PatientID
    WHERE rx.DeletedDate IS NULL
      AND (
        rx.CreatedDate > @lastUpdatedAt
        OR (
          rx.CreatedDate = @lastUpdatedAt
          AND rx.ID > @lastPrescriptionRowId
        )
      )
    ORDER BY rx.CreatedDate ASC, rx.ID ASC
    `,
    {
      limit,
      lastUpdatedAt,
      lastPrescriptionRowId: Number(lastPrescriptionRowId || 0),
    },
  );
  return rows
    .map((r) => ({
      fileNum: r.FileNum != null ? String(r.FileNum).trim() : '',
      sessionId: r.SessionId != null ? Number(r.SessionId) : null,
      prescriptionRowId: r.PrescriptionRowId != null ? Number(r.PrescriptionRowId) : null,
      updatedAt: r.UpdatedAt,
    }))
    .filter(
      (r) =>
        r.fileNum &&
        r.sessionId != null &&
        !Number.isNaN(r.sessionId) &&
        r.prescriptionRowId != null &&
        !Number.isNaN(r.prescriptionRowId),
    );
}

/**
 * Backfill: lấy danh sách phiên có đơn thuốc trong [from, to) theo ViewRX.CreatedDate.
 * Trả về distinct (fileNum, sessionId) theo thứ tự thời gian để backfill ổn định.
 */
async function getDistinctPrescriptionSessionsCreatedBetween(from, to) {
  const rows = await db.executeQuery(
    `
    SELECT
      LTRIM(RTRIM(CONVERT(VARCHAR(50), p.FileNum))) AS FileNum,
      rx.SessionId,
      MIN(rx.CreatedDate) AS FirstAt
    FROM dbo.ViewRX rx WITH (NOLOCK)
    INNER JOIN dbo.PersonView p WITH (NOLOCK) ON p.ContactId = rx.PatientID
    WHERE rx.DeletedDate IS NULL
      AND rx.CreatedDate >= @from
      AND rx.CreatedDate < @to
    GROUP BY p.FileNum, rx.SessionId
    ORDER BY FirstAt ASC, rx.SessionId ASC
    `,
    { from, to },
  );
  return rows
    .map((r) => ({
      fileNum: r.FileNum != null ? String(r.FileNum).trim() : '',
      sessionId: r.SessionId != null ? Number(r.SessionId) : null,
    }))
    .filter((r) => r.fileNum && r.sessionId != null && !Number.isNaN(r.sessionId));
}

/** Watermark mới nhất ViewRX (sau backfill / seed checkpoint). */
async function getLatestPrescriptionWatermark() {
  const rows = await db.executeQuery(
    `
    SELECT TOP 1 rx.ID AS PrescriptionRowId, rx.CreatedDate AS UpdatedAt
    FROM dbo.ViewRX rx WITH (NOLOCK)
    WHERE rx.DeletedDate IS NULL
    ORDER BY rx.CreatedDate DESC, rx.ID DESC
    `,
    {},
  );
  if (!rows.length) return null;
  const id = rows[0].PrescriptionRowId != null ? Number(rows[0].PrescriptionRowId) : null;
  if (id == null || Number.isNaN(id)) return null;
  return { lastPrescriptionRowId: id, lastUpdatedAt: rows[0].UpdatedAt };
}

module.exports = {
  getPrescriptionReportContext,
  getDistinctPrescriptionSubSessionIds,
  getDistinctPrescriptionScriptNos,
  getPrescriptionReportContextByScriptNo,
  resolveFileNumSessionIdForPrescription,
  getUpdatedPrescriptionRowsSince,
  getDistinctPrescriptionSessionsCreatedBetween,
  getLatestPrescriptionWatermark,
};
