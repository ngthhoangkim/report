/**
 * Chạy một vòng polling toa thuốc (không cần PRESCRIPTION_AUTOMATION_ENABLED).
 * Dùng kiểm tra tay hoặc cron riêng.
 *
 *   node scripts/runPrescriptionAutomationOnce.js
 */
require('dotenv').config({ path: require('path').join(__dirname, '..', '.env') });
const db = require('../src/config/database');
const { runPrescriptionAutomationOnce } = require('../src/services/prescriptionAutomationService');

(async () => {
  await db.initializePool();
  try {
    await runPrescriptionAutomationOnce();
  } finally {
    await db.closePool();
  }
})().catch((e) => {
  console.error(e);
  process.exit(1);
});
