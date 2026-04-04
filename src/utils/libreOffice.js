const fs = require('fs');
const { execSync } = require('child_process');

const MAC_SOFFICE = '/Applications/LibreOffice.app/Contents/MacOS/soffice';
const LINUX_CANDIDATES = [
  '/usr/bin/soffice',
  '/usr/lib/libreoffice/program/soffice',
  '/snap/bin/libreoffice',
];

/**
 * Đường dẫn soffice: SOFFICE_PATH → macOS app bundle → Linux thường gặp → `which soffice` → 'soffice'.
 */
function getLibreOfficeBinary() {
  const envPath = process.env.SOFFICE_PATH;
  if (envPath && fs.existsSync(envPath)) {
    return envPath;
  }
  if (process.platform === 'darwin' && fs.existsSync(MAC_SOFFICE)) {
    return MAC_SOFFICE;
  }
  for (const p of LINUX_CANDIDATES) {
    if (fs.existsSync(p)) {
      return p;
    }
  }
  try {
    const out = execSync('which soffice', { encoding: 'utf8' }).trim();
    if (out) {
      return out.split('\n')[0];
    }
  } catch (_) {
    /* ignore */
  }
  return 'soffice';
}

/**
 * Biến môi trường cho mọi lần spawn `soffice`.
 * PYTHONHOME / PYTHONPATH của hệ thống thường làm Python nhúng trong LibreOffice lỗi và in ra stderr:
 * "Could not find platform independent libraries <prefix>" (lặp vài lần mỗi job).
 */
function getLibreOfficeSpawnEnv() {
  const env = { ...process.env };
  delete env.PYTHONHOME;
  delete env.PYTHONPATH;
  delete env.PYTHONSAFEPATH;
  return env;
}

module.exports = {
  getLibreOfficeBinary,
  getLibreOfficeSpawnEnv,
  MAC_SOFFICE,
};
