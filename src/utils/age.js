/**
 * Mirrors ReportService.CalcAge(dob, refDate) — age at examination date (ngayKham).
 */
function calcAge(dob, refDate) {
  if (!dob) return '';
  const birth = new Date(dob);
  if (Number.isNaN(birth.getTime())) return '';
  const reference = refDate ? new Date(refDate) : new Date();
  if (Number.isNaN(reference.getTime())) return '';
  let age = reference.getFullYear() - birth.getFullYear();
  const m = reference.getMonth() - birth.getMonth();
  if (m < 0 || (m === 0 && reference.getDate() < birth.getDate())) {
    age -= 1;
  }
  return age < 0 ? '' : String(age);
}

module.exports = { calcAge };
