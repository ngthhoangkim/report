const fs = require('fs');
const path = require('path');
const os = require('os');
const crypto = require('crypto');
const { execFile } = require('child_process');
const { promisify } = require('util');
const { getLibreOfficeBinary } = require('./libreOffice');
const logger = require('./logger');

const execFileAsync = promisify(execFile);

function ensureDir(dir) {
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
}

function isWindows() {
  return process.platform === 'win32';
}

function normalizeMode(mode) {
  const m = String(mode).toLowerCase().trim();
  if (!['pdf', 'docx'].includes(m)) throw new Error(`Unsupported convert mode: ${mode}`);
  return m;
}

function shortErr(e, max = 220) {
  const s = (e?.message || String(e || '')).replace(/\s+/g, ' ').trim();
  if (s.length <= max) return s;
  return `${s.slice(0, max - 3)}...`;
}

async function convertWithLibreOffice(mode, inputPath, outDir) {
  const bin = getLibreOfficeBinary();
  const args = ['--headless', '--convert-to', mode, '--outdir', outDir, inputPath];
  await execFileAsync(bin, args, { maxBuffer: 50 * 1024 * 1024 });
}

/**
 * Convert using Microsoft Word (COM automation) on Windows.
 * - input: .doc/.docx/.rtf
 * - output: .docx or .pdf
 */
async function convertWithWord(mode, inputPath, outDir) {
  const m = normalizeMode(mode);
  ensureDir(outDir);

  const absInput = path.resolve(inputPath);
  if (!fs.existsSync(absInput)) {
    throw new Error(`Word conversion input not found: ${absInput}`);
  }
  const st = fs.statSync(absInput);
  if (!st.isFile() || st.size <= 0) {
    throw new Error(`Word conversion input is empty/invalid: ${absInput} size=${st.size}`);
  }

  const base = path.parse(absInput).name;
  const outPath = path.join(outDir, `${base}.${m}`);

  // Word SaveAs2 format codes:
  // - wdFormatXMLDocument = 16 (.docx)
  // - wdFormatPDF = 17 (.pdf)
  const format = m === 'docx' ? 16 : 17;

  const ps = `
$ErrorActionPreference = "Stop"
$inputPath = "${absInput.replace(/"/g, '""')}"
$outputPath = "${outPath.replace(/"/g, '""')}"
$format = ${format}

$word = $null
$doc = $null
try {
  $word = New-Object -ComObject Word.Application
  $word.Visible = $false
  $word.DisplayAlerts = 0
  $doc = $word.Documents.Open($inputPath, $false, $true)
  $doc.SaveAs2($outputPath, $format)
} finally {
  if ($doc -ne $null) { try { $doc.Close($false) } catch {} }
  if ($word -ne $null) { try { $word.Quit() } catch {} }
}
`;

  // Write script to temp and run powershell
  const tmpDir = path.join(os.tmpdir(), `word_convert_${crypto.randomBytes(6).toString('hex')}`);
  ensureDir(tmpDir);
  const ps1 = path.join(tmpDir, 'convert.ps1');
  fs.writeFileSync(ps1, ps, 'utf8');

  const psExe = process.env.POWERSHELL_EXE || 'powershell.exe';
  try {
    await execFileAsync(
      psExe,
      ['-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', ps1],
      { maxBuffer: 50 * 1024 * 1024 },
    );
  } catch (e) {
    throw e;
  }

  if (!fs.existsSync(outPath)) {
    throw new Error(`Word conversion produced no output: ${outPath}`);
  }
}

/**
 * Unified conversion:
 * - On Windows: prefer Word when USE_WORD=true (default true).
 * - On macOS/Linux: use LibreOffice/soffice.
 */
async function convertWithOffice(mode, inputPath, outDir) {
  const m = normalizeMode(mode);
  const useWord =
    isWindows() &&
    String(process.env.USE_WORD || 'true').toLowerCase().trim() !== 'false';

  if (useWord) {
    try {
      return await convertWithWord(m, inputPath, outDir);
    } catch (e) {
      logger.warn('Word convert failed; fallback to LibreOffice', {
        mode: m,
        input: path.basename(String(inputPath || '')),
        reason: shortErr(e),
      });
      try {
        return await convertWithLibreOffice(m, inputPath, outDir);
      } catch (e2) {
        logger.error('LibreOffice fallback also failed', {
          mode: m,
          input: path.basename(String(inputPath || '')),
          wordReason: shortErr(e),
          loReason: shortErr(e2),
        });
        throw e2;
      }
    }
  }
  return convertWithLibreOffice(m, inputPath, outDir);
}

module.exports = {
  convertWithOffice,
  convertWithLibreOffice,
  convertWithWord,
};

