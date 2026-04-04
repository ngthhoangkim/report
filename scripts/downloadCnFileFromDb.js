/**
 * Tìm dbo.CN_FILES theo FileNum + SessionId, copy file ZIP về thư mục output (không giải nén).
 *
 * File trên disk: PATHS_SOURCE_IMAGE_DIR → PATHS_FALLBACK_IMAGE_DIR → PATHS_LOCAL_IMAGE_DIR
 * (cùng kiểu với worker CDHA; cần mount UNC / chạy trên máy có file thật).
 *
 * Usage:
 *   node scripts/downloadCnFileFromDb.js <fileNum> <sessionId> [--out=./output/cn_files_zip]
 *   node scripts/downloadCnFileFromDb.js --fileNum=26003528 --sessionId=844466
 *   npm run download-cn-file -- 26003528 844466 --out=./output/cn_files_zip
 *
 * Cần `.env` kết nối SQL giống app.
 */
require('dotenv').config();

const fs = require('fs');
const path = require('path');
const db = require('../src/config/database');
const { getPaths } = require('../src/config/paths');
const { ensureDirectoryExists } = require('../src/utils/fileHelper');
const { resolveExistingFileInDir } = require('../src/utils/imageSourceResolve');
const {
  guessStoredPathFromRow,
  getCnFilesByFileNumAndSessionId,
  PATH_COLUMN_CANDIDATES,
} = require('../src/repositories/cnFilesRepository');

function parseArgs(argv) {
  const out = {};
  for (const a of argv) {
    if (a === '--help' || a === '-h') out.help = true;
    else if (a.startsWith('--fileNum=')) out.fileNum = a.slice('--fileNum='.length);
    else if (a.startsWith('--filenum=')) out.fileNum = a.slice('--filenum='.length);
    else if (a.startsWith('--sessionId=')) out.sessionId = a.slice('--sessionId='.length);
    else if (a.startsWith('--sessionid=')) out.sessionId = a.slice('--sessionid='.length);
    else if (a.startsWith('--out=')) out.out = a.slice('--out='.length);
    else if (!a.startsWith('--') && out.fileNum == null) out.fileNum = a;
    else if (!a.startsWith('--') && out.sessionId == null) out.sessionId = a;
  }
  return out;
}

function usageAndExit(code = 1) {
  // eslint-disable-next-line no-console
  console.log(
    [
      'CN_FILES: copy ZIP theo FileNum + SessionId (chỉ tải file, không giải nén).',
      '',
      '  node scripts/downloadCnFileFromDb.js <fileNum> <sessionId> [--out=dir]',
      '  node scripts/downloadCnFileFromDb.js --fileNum=... --sessionId=...',
      '',
      'Mặc định --out: ./output/cn_files_zip',
      'PATHS_*: PATHS_SOURCE_IMAGE_DIR, PATHS_FALLBACK_IMAGE_DIR, PATHS_LOCAL_IMAGE_DIR',
    ].join('\n'),
  );
  process.exit(code);
}

function summarizeRow(row) {
  const o = {};
  for (const [k, v] of Object.entries(row)) {
    if (Buffer.isBuffer(v)) o[k] = `<Buffer ${v.length} bytes>`;
    else if (v instanceof Date) o[k] = v.toISOString();
    else o[k] = v;
  }
  return o;
}

function isProbablyAbsolutePath(p) {
  if (!p || typeof p !== 'string') return false;
  if (path.isAbsolute(p)) return true;
  if (/^[A-Za-z]:[\\/]/.test(p)) return true;
  return p.startsWith('\\\\');
}

function resolveCnFileOnDisk(claimedPath, paths) {
  if (!claimedPath) return null;
  const raw = String(claimedPath).trim();
  if (!raw) return null;

  if (isProbablyAbsolutePath(raw) && fs.existsSync(raw) && fs.statSync(raw).isFile()) {
    return path.resolve(raw);
  }

  const base = path.basename(raw.replace(/\\/g, '/'));
  const ordered = [paths.sourceImageDir, paths.fallbackImageDir, paths.localImageDir];
  for (const dir of ordered) {
    const found = resolveExistingFileInDir(dir, base);
    if (found) return path.resolve(found);
  }
  return null;
}

function uniqueDestPath(outDir, baseName, rowId) {
  let dest = path.join(outDir, baseName);
  if (!fs.existsSync(dest)) return dest;
  const ext = path.extname(baseName);
  const stem = path.basename(baseName, ext);
  const suffix = rowId != null ? `_${rowId}` : `_${Date.now()}`;
  return path.join(outDir, `${stem}${suffix}${ext}`);
}

async function main() {
  const parsed = parseArgs(process.argv.slice(2));
  if (parsed.help) usageAndExit(0);

  const { fileNum, sessionId } = parsed;
  if (!fileNum || sessionId == null || String(sessionId).trim() === '') {
    usageAndExit(1);
  }

  const sid = Number(sessionId);
  if (Number.isNaN(sid)) {
    // eslint-disable-next-line no-console
    console.error('sessionId phải là số', { sessionId });
    process.exit(1);
  }

  const paths = getPaths();
  const outDir = parsed.out
    ? path.resolve(process.cwd(), parsed.out)
    : path.join(process.cwd(), 'output', 'cn_files_zip');

  try {
    await db.initializePool();

    const rows = await getCnFilesByFileNumAndSessionId(String(fileNum), sid);
    // eslint-disable-next-line no-console
    console.log(
      `CN_FILES: ${rows.length} row(s) FileNum=${fileNum} SessionId=${sid} (path columns: ${PATH_COLUMN_CANDIDATES.join(', ')})`,
    );

    if (rows.length === 0) {
      process.exit(2);
    }

    ensureDirectoryExists(outDir);
    let copied = 0;

    for (const row of rows) {
      // eslint-disable-next-line no-console
      console.log('Row:', JSON.stringify(summarizeRow(row)));

      const claimed = guessStoredPathFromRow(row);
      if (!claimed) {
        // eslint-disable-next-line no-console
        console.error('Không đoán được cột đường dẫn file. Keys:', Object.keys(row).join(', '));
        continue;
      }

      const src = resolveCnFileOnDisk(claimed, paths);
      if (!src) {
        // eslint-disable-next-line no-console
        console.error('Không thấy file trên disk:', {
          claimed,
          sourceImageDir: paths.sourceImageDir,
          fallbackImageDir: paths.fallbackImageDir,
          localImageDir: paths.localImageDir,
        });
        continue;
      }

      const baseName = path.basename(src);
      const rowId = row.Id != null ? row.Id : row.id;
      const destZip = uniqueDestPath(outDir, baseName, rowId);
      fs.copyFileSync(src, destZip);
      copied += 1;
      // eslint-disable-next-line no-console
      console.log('Copied:', { from: src, to: destZip });
    }

    if (copied === 0) {
      process.exit(3);
    }
  } finally {
    await db.closePool();
  }
}

main().catch((e) => {
  // eslint-disable-next-line no-console
  console.error(e);
  process.exit(1);
});
