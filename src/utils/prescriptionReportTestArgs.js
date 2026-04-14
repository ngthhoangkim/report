/**
 * CLI / Vitest: parse tham số giống scripts/testGenerateReport.js.
 * Vitest: truyền thêm sau `--`, ví dụ:
 *   npx vitest run test/prescriptionReport.vitest.spec.js -- --sessionId=30338
 */
function parsePrescriptionTestArgs(argv) {
  const out = { fileNum: null, sessionId: null, upload: false, local: false };
  for (const a of argv) {
    if (a === '--upload') out.upload = true;
    else if (a === '--local') out.local = true;
    else if (a.startsWith('--fileNum=')) out.fileNum = a.slice('--fileNum='.length);
    else if (a.startsWith('--filenum=')) out.fileNum = a.slice('--filenum='.length);
    else if (a.startsWith('--sessionId=')) out.sessionId = a.slice('--sessionId='.length);
    else if (a.startsWith('--sessionid=')) out.sessionId = a.slice('--sessionid='.length);
    else if (!a.startsWith('--') && out.fileNum == null) out.fileNum = a;
    else if (!a.startsWith('--') && out.sessionId == null) out.sessionId = a;
  }
  return out;
}

function getPrescriptionCliArgv() {
  const inVitest = process.env.VITEST === 'true' || process.env.VITEST_WORKER_ID !== undefined;
  if (inVitest) {
    const idx = process.argv.indexOf('--');
    return idx >= 0 ? process.argv.slice(idx + 1) : [];
  }
  return process.argv.slice(2);
}

module.exports = {
  parsePrescriptionTestArgs,
  getPrescriptionCliArgv,
};
