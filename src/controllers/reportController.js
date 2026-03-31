const db = require('../config/database');
const { generatePdfByFileNumAndSessionId } = require('../services/reportGeneratorService');

async function getReportData(req, res) {
  const { filenum, sessionId } = req.query;
  if (!filenum) {
    return res.status(400).json({ error: 'filenum is required' });
  }
  try {
    // 1. Lấy danh sách Id kết quả xét nghiệm từ ViewPathologyResult
    let idQuery = `SELECT Id FROM ViewPathologyResult WHERE FileNum = @filenum`;
    const params = { filenum };
    if (sessionId) {
      idQuery += ' AND SessionId = @sessionId';
      params.sessionId = sessionId;
    }
    const idsResult = await db.executeQuery(idQuery, params);
    const ids = idsResult && idsResult.length > 0 ? idsResult.map(row => row.Id) : [];

    // 2. Lấy chi tiết kết quả xét nghiệm
    let examResults = [];
    if (ids.length > 0) {
      let valueQuery = '';
      let valueParams = {};
      if (ids.length === 1) {
        valueQuery = `SELECT * FROM CN_PathologyResultValue WHERE ResultId = @id`;
        valueParams = { id: ids[0] };
      } else {
        const idParams = ids.map((_, idx) => `@id${idx}`).join(',');
        valueQuery = `SELECT * FROM CN_PathologyResultValue WHERE ResultId IN (${idParams})`;
        ids.forEach((id, idx) => { valueParams[`id${idx}`] = id; });
      }
      examResults = await db.executeQuery(valueQuery, valueParams);
    }

    // 3. Lấy kết quả hình ảnh CT từ CN_ImagingResult và CN_ImagingResultData
    let ctResults = [];
    let ctQuery = `SELECT r.*, d.*
      FROM CN_ImagingResult r
      LEFT JOIN CN_ImagingResultData d ON r.ImagingResultId = d.ImagingResultId
      WHERE r.FileNum = @filenum`;
    const ctParams = { filenum };
    if (sessionId) {
      ctQuery += ' AND r.SessionId = @sessionId';
      ctParams.sessionId = sessionId;
    }
    ctResults = await db.executeQuery(ctQuery, ctParams);

    res.json({
      examResults,
      ctResults
    });
  } catch (error) {
    console.error('Error fetching report data:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
}

// API: Lấy kết quả xét nghiệm và kết quả hình ảnh CT
async function getPatientResults(req, res) {
  const { filenum, sessionId } = req.query;
  if (!filenum) {
    return res.status(400).json({ error: 'filenum is required' });
  }
  try {
    // 1. Lấy danh sách Id kết quả xét nghiệm từ ViewPathologyResult
    let idQuery = `SELECT Id FROM ViewPathologyResult WHERE FileNum = @filenum`;
    const params = { filenum };
    if (sessionId) {
      idQuery += ' AND SessionId = @sessionId';
      params.sessionId = sessionId;
    }
    const idsResult = await db.executeQuery(idQuery, params);
    const ids = idsResult && idsResult.length > 0 ? idsResult.map(row => row.Id) : [];

    // 2. Lấy chi tiết kết quả xét nghiệm
    let examResults = [];
    if (ids.length > 0) {
      let valueQuery = '';
      let valueParams = {};
      if (ids.length === 1) {
        valueQuery = `SELECT * FROM CN_PathologyResultValue WHERE ResultId = @id`;
        valueParams = { id: ids[0] };
      } else {
        const idParams = ids.map((_, idx) => `@id${idx}`).join(',');
        valueQuery = `SELECT * FROM CN_PathologyResultValue WHERE ResultId IN (${idParams})`;
        ids.forEach((id, idx) => { valueParams[`id${idx}`] = id; });
      }
      examResults = await db.executeQuery(valueQuery, valueParams);
    }

    // 3. Lấy kết quả hình ảnh CT
    let ctResults = [];
    let ctQuery = `SELECT 
      ItemNum, FileNum, PatientName, Dob, Gender, Address, 
      Conclusion, Doctor, RequestedDoctor, ngayKham,
      FileName, v.ImagingResultId, ResultData, ConclusionData, 
      SuggestionData, TemplateFile, PathologyType
      FROM viewImageFileName v
      JOIN CN_ImagingResultData ird
        ON v.ImagingResultId = ird.ImagingResultId
      WHERE v.FileNum = @filenum`;
    const ctParams = { filenum };
    if (sessionId) {
      ctQuery += ' AND v.SessionId = @sessionId';
      ctParams.sessionId = sessionId;
    }
    ctResults = await db.executeQuery(ctQuery, ctParams);

    res.json({
      examResults,
      ctResults
    });
  } catch (error) {
    console.error('Error fetching patient results:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
}

// API: Lấy kết quả hình ảnh theo sessionId + fileNum (+ type optional)
async function getXrayResultBySessionAndFileNum(req, res) {
  const { sessionId, filenum, type } = req.query;

  if (!sessionId || !filenum) {
    return res.status(400).json({
      error: 'sessionId and filenum are required',
    });
  }

  try {
    if (type !== undefined && (Number.isNaN(Number(type)) || String(type).trim() === '')) {
      return res.status(400).json({
        error: 'type must be a valid number when provided',
      });
    }

    const query = `
      SELECT
        v.Id AS ImagingResultId,
        v.SessionId,
        v.FileNum,
        v.PatientName,
        v.Dob,
        v.Sex AS Gender,
        v.Street AS Address,
        v.ServiceName,
        v.RequestedDoctor,
        v.Doctor,
        v.CreatedDate,
        r.TemplateFile,
        r.PathologyType,
        r.ResultName,
        r.FileName,
        d.ResultData,
        d.ConclusionData,
        d.SuggestionData
      FROM ViewImagingResult v
      LEFT JOIN CN_ImagingResult r ON v.Id = r.Id
      LEFT JOIN CN_ImagingResultData d ON v.Id = d.ImagingResultId
      WHERE v.SessionId = @sessionId
        AND v.FileNum = @filenum
        AND (@type IS NULL OR r.PathologyType = @type)
      ORDER BY v.CreatedDate DESC
    `;

    const typeValue = type !== undefined ? Number(type) : null;
    const results = await db.executeQuery(query, { sessionId, filenum, type: typeValue });
    if (!results || results.length === 0) {
      return res.status(404).json({
        error: 'No imaging result found for the provided filters',
      });
    }

    return res.json({
      count: results.length,
      filters: {
        sessionId: Number(sessionId),
        filenum: String(filenum),
        type: typeValue,
      },
      results: results.map((row) => ({
        patient: {
          sessionId: row.SessionId,
          fileNum: row.FileNum,
          patientName: row.PatientName,
          dob: row.Dob,
          gender: row.Gender,
          address: row.Address,
          doctor: row.Doctor,
          requestedDoctor: row.RequestedDoctor,
        },
        imaging: {
          imagingResultId: row.ImagingResultId,
          pathologyType: row.PathologyType,
          serviceName: row.ServiceName,
          resultName: row.ResultName,
          sourceFileName: row.FileName,
          createdDate: row.CreatedDate,
        },
        template: {
          templateFile: row.TemplateFile ?? null,
        },
        reportData: {
          resultData: row.ResultData,
          conclusionData: row.ConclusionData,
          suggestionData: row.SuggestionData,
        },
      })),
    });
  } catch (error) {
    console.error('Error fetching imaging results:', error);
    return res.status(500).json({ error: 'Internal server error' });
  }
}

/**
 * POST /api/report/generate
 * Body: { fileNum: string, sessionId: number, resultFileName?: string }
 * — một phiên (SessionId); tên PDF = FileName kết quả (dòng mới nhất trong phiên), hoặc resultFileName.
 */
async function generateReport(req, res) {
  const fileNum = req.body?.fileNum ?? req.body?.FileNum ?? req.body?.filenum;
  const sessionIdRaw = req.body?.sessionId ?? req.body?.SessionId;
  const resultFileName = req.body?.resultFileName ?? req.body?.ResultFileName;

  if (fileNum === undefined || fileNum === null || String(fileNum).trim() === '') {
    return res.status(400).json({ error: 'fileNum is required' });
  }
  if (sessionIdRaw === undefined || sessionIdRaw === null || sessionIdRaw === '') {
    return res.status(400).json({ error: 'sessionId is required' });
  }
  const sessionId = Number(sessionIdRaw);
  if (Number.isNaN(sessionId)) {
    return res.status(400).json({ error: 'sessionId must be a number' });
  }

  try {
    const result = await generatePdfByFileNumAndSessionId(
      String(fileNum).trim(),
      sessionId,
      { resultFileName },
    );
    return res.json(result);
  } catch (error) {
    console.error('Error generating report:', error);
    if (error.code === 'NO_RECORDS' || error.code === 'NO_SEGMENTS') {
      return res.status(404).json({ success: false, error: error.message });
    }
    return res.status(500).json({ success: false, error: error.message || 'Internal server error' });
  }
}

module.exports = {
  getReportData,
  getPatientResults,
  getXrayResultBySessionAndFileNum,
  generateReport,
};
