/**
 * Find "heavy" imaging sessions (FileNum + SessionId) to test report generation.
 *
 * It ranks sessions by:
 * - segmentCount: number of CN_ImagingResult rows in the session
 * - printedImageCount: number of printed images (CN_PathologyImage.Printed=1) across those segments
 * - totalImageCount: total images across those segments
 *
 * Usage:
 *   node scripts/findHeavySession.js
 *   node scripts/findHeavySession.js --top=20
 *   node scripts/findHeavySession.js --days=30   (only sessions with r.CreatedDate within last N days)
 *   node scripts/findHeavySession.js --from=2023-01-01 --to=2026-12-31
 */
require('dotenv').config();

const db = require('../src/config/database');
const logger = require('../src/utils/logger');

function parseArgs(argv) {
  const out = { top: 10, days: null, from: null, to: null };
  for (const a of argv) {
    if (a.startsWith('--top=')) out.top = Number(a.slice('--top='.length));
    if (a.startsWith('--days=')) out.days = Number(a.slice('--days='.length));
    if (a.startsWith('--from=')) out.from = String(a.slice('--from='.length)).trim();
    if (a.startsWith('--to=')) out.to = String(a.slice('--to='.length)).trim();
  }
  if (!Number.isFinite(out.top) || out.top <= 0) out.top = 10;
  out.top = Math.max(1, Math.min(100, out.top));
  if (!Number.isFinite(out.days) || out.days <= 0) out.days = null;
  if (out.from && Number.isNaN(new Date(out.from).getTime())) out.from = null;
  if (out.to && Number.isNaN(new Date(out.to).getTime())) out.to = null;
  return out;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  await db.initializePool();

  let whereDate = '';
  const params = { top: args.top };
  if (args.days) {
    whereDate = 'AND r.CreatedDate >= DATEADD(day, -@days, GETDATE())';
    params.days = args.days;
  } else {
    if (args.from) {
      whereDate += ' AND r.CreatedDate >= @from';
      params.from = args.from;
    }
    if (args.to) {
      // inclusive end date
      whereDate += ' AND r.CreatedDate < DATEADD(day, 1, @to)';
      params.to = args.to;
    }
  }

  const sql = `
    ;WITH Img AS (
      SELECT
        ResultId,
        COUNT(1) AS TotalImageCount,
        SUM(CASE WHEN Printed = 1 THEN 1 ELSE 0 END) AS PrintedImageCount
      FROM dbo.CN_PathologyImage
      GROUP BY ResultId
    )
    SELECT TOP (@top)
      v.FileNum,
      v.SessionId,
      COUNT(1) AS SegmentCount,
      SUM(ISNULL(i.PrintedImageCount, 0)) AS PrintedImageCount,
      SUM(ISNULL(i.TotalImageCount, 0)) AS TotalImageCount,
      MIN(r.CreatedDate) AS FirstCreatedDate,
      MAX(r.CreatedDate) AS LastCreatedDate
    FROM dbo.CN_ImagingResult r
    INNER JOIN dbo.ViewImagingResult v ON v.Id = r.Id
    LEFT JOIN Img i ON i.ResultId = r.Id
    WHERE r.DeletedDate IS NULL
      ${whereDate}
    GROUP BY v.FileNum, v.SessionId
    ORDER BY
      SUM(ISNULL(i.PrintedImageCount, 0)) DESC,
      COUNT(1) DESC,
      MAX(r.CreatedDate) DESC
  `;

  const rows = await db.executeQuery(sql, params);
  logger.info('Heavy sessions', {
    top: args.top,
    days: args.days,
    from: args.from,
    to: args.to,
    count: rows.length,
  });

  // eslint-disable-next-line no-console
  console.log(
    rows.map((r) => ({
      fileNum: String(r.FileNum),
      sessionId: Number(r.SessionId),
      segmentCount: Number(r.SegmentCount),
      printedImageCount: Number(r.PrintedImageCount),
      totalImageCount: Number(r.TotalImageCount),
      firstCreatedDate: r.FirstCreatedDate,
      lastCreatedDate: r.LastCreatedDate,
    })),
  );
}

main()
  .then(() => db.closePool())
  .catch(async (e) => {
    logger.error('findHeavySession failed', { message: e?.message || String(e), stack: e?.stack || null });
    try {
      await db.closePool();
    } catch (_) {
      // ignore
    }
    process.exitCode = 1;
  });

