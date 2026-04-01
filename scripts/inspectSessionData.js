/**
 * Kiểm tra dữ liệu báo cáo + giải nén gzip (ResultData, ConclusionData, SuggestionData).
 *
 *   node scripts/inspectSessionData.js 15058178 523620
 */
require('dotenv').config();

const db = require('../src/config/database');
const { decompressToString } = require('../src/utils/gzipHelper');
const { rtfToPlainText } = require('../src/utils/rtfToPlain');
const fs = require('fs');
const path = require('path');
const os = require('os');

const SQL = `
  SELECT
    r.Id AS ImagingResultId,
    r.TemplateFile,
    r.PathologyType,
    r.Conclusion,
    r.FileName,
    d.ResultData,
    d.ConclusionData,
    d.SuggestionData
  FROM dbo.CN_ImagingResult r
  INNER JOIN dbo.CN_ImagingResultData d ON r.Id = d.ImagingResultId
  INNER JOIN dbo.ViewImagingResult v ON v.Id = r.Id
  WHERE v.FileNum = @fileNum
    AND v.SessionId = @sessionId
    AND r.DeletedDate IS NULL
  ORDER BY r.CreatedDate ASC, r.Id ASC
`;

function preview(str, max = 400) {
  if (str == null) return '(null)';
  const s = String(str).replace(/\r/g, '\n');
  if (s.length <= max) return s;
  return `${s.slice(0, max)}… [+${s.length - max} chars]`;
}

function bufInfo(buf) {
  if (buf == null) return { bytes: 0, note: 'null' };
  const b = Buffer.isBuffer(buf) ? buf : Buffer.from(buf);
  return { bytes: b.length, note: b.length ? `first4=${b.subarray(0, 4).toString('hex')}` : 'empty' };
}

async function main() {
  const fileNum = process.argv[2] || '15058178';
  const sessionId = Number(process.argv[3] || '523620');
  if (Number.isNaN(sessionId)) {
    console.error('Invalid sessionId');
    process.exit(1);
  }

  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'inspect_rtf_'));

  await db.initializePool();
  try {
    const rows = await db.executeQuery(SQL, { fileNum: String(fileNum), sessionId });
    console.log(JSON.stringify({ fileNum, sessionId, rowCount: rows.length }, null, 2));

    for (let i = 0; i < rows.length; i += 1) {
      const row = rows[i];
      console.log('\n--- Row', i + 1, '---');
      console.log('ImagingResultId:', row.ImagingResultId);
      console.log('PathologyType:', row.PathologyType);
      console.log('TemplateFile:', row.TemplateFile || '(empty)');
      console.log('FileName (ZIP media):', row.FileName || '(empty)');
      console.log('Conclusion (cột SQL, plain):', preview(row.Conclusion, 300));

      const fields = ['ResultData', 'ConclusionData', 'SuggestionData'];
      for (const f of fields) {
        const raw = row[f];
        console.log(`\n[${f}]`, bufInfo(raw));
        const rtf = decompressToString(raw);
        if (!rtf || !String(rtf).trim()) {
          console.log('  decompressed: (empty or null)');
          continue;
        }
        console.log('  decompressed length:', rtf.length, 'startsWith:', JSON.stringify(rtf.slice(0, 40)));
        try {
          const plain = rtfToPlainText(rtf, path.join(tmpDir, `${f}_${row.ImagingResultId}`));
          console.log('  plain text preview:', preview(plain, 500));
        } catch (e) {
          console.log('  plain extract failed:', e.message);
        }
      }
    }

    if (rows.length === 0) {
      console.log('\nNo rows — kiểm tra FileNum / SessionId hoặc DeletedDate.');
    }
  } finally {
    await db.closePool();
    try {
      fs.rmSync(tmpDir, { recursive: true });
    } catch (_) {}
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
