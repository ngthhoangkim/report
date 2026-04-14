/**
 * Helpers cho payload mẫu ToaThuoc (docxtemplater).
 */

function str(v) {
  if (v === undefined || v == null) return '';
  return String(v);
}

function escapeXmlText(s) {
  return str(s)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

/**
 * Một dòng hiển thị cho một dòng ViewRX (1 thuốc).
 */
function formatRxDisplayLine(r, index) {
  const name = str(r.ITEM);
  const prop = str(r.Property);
  const dose = str(r.DOSE);
  const uu = str(r.UnitUsage);
  const qty = r.QUANTITY != null && r.QUANTITY !== '' ? str(r.QUANTITY) : '';
  const un = str(r.UNITNAME).trim();
  const line = prop ? `${name} (${prop})` : name;
  const dosePart = [dose, uu].filter(Boolean).join(' ');
  const qtyPart = [qty, un].filter(Boolean).join(' ');
  let out = `${index + 1}. ${line}`;
  if (dosePart) out += ` — ${dosePart}`;
  if (qtyPart) out += ` — ${qtyPart}`;
  return out;
}

/** Cột tổng / số lượng kê (kèm đơn vị) */
function formatRxTotalCell(r) {
  const q = r.QUANTITY != null && r.QUANTITY !== '' ? str(r.QUANTITY).trim() : '';
  const u = str(r.UNITNAME).trim();
  if (q && u) return `${q} ${u}`;
  return q || u || '';
}

/** Cột tên thuốc + hoạt chất */
function formatRxNameCell(r) {
  const name = str(r.ITEM).trim();
  const prop = str(r.Property).trim();
  if (!name && !prop) return '';
  if (prop) return `${name} (${prop})`;
  return name;
}

/**
 * Cách uống: liều/lần; nếu FREQUENCY là số → "Ngày X lần"; thêm FREQUENCY dạng chữ + INSTRUCTIONS (sáng/chiều…).
 */
function formatRxScheduleCell(r) {
  const parts = [];
  const dose = [str(r.DOSE), str(r.UnitUsage).trim()].filter(Boolean).join(' ');
  if (dose) parts.push(`Liều/lần: ${dose}`);

  const fRaw = str(r.FREQUENCY).trim();
  if (fRaw) {
    if (/^\d+(\.\d+)?$/.test(fRaw)) {
      parts.push(`Ngày ${fRaw} lần`);
    } else {
      parts.push(fRaw);
    }
  }

  const ins = str(r.INSTRUCTIONS).trim();
  if (ins) parts.push(ins);

  return parts.join(' · ');
}

/** @alias formatRxScheduleCell */
function formatRxHowCell(r) {
  return formatRxScheduleCell(r);
}

/** Dòng 1 khối thuốc (mẫu: STT/ + tổng SL + tên + Liều/lần) */
function formatRxItemLine1(index, r) {
  const n = index + 1;
  const tot = formatRxTotalCell(r);
  const item = str(r.ITEM).trim();
  const prop = str(r.Property).trim();
  const dosePart = [str(r.DOSE), str(r.UnitUsage).trim()].filter(Boolean).join(' ');
  const namePart = prop ? `${item} (${prop})` : item;
  const lieu = dosePart ? `Liều/lần: ${dosePart}` : '';
  return [`${n}/`, tot, namePart, lieu].filter(Boolean).join('  ');
}

/** Dòng <<Items>> khi <<Quantity>> là ô riêng (không lặp tổng SL) */
function formatRxItemLine1Split(index, r) {
  const n = index + 1;
  const item = str(r.ITEM).trim();
  const prop = str(r.Property).trim();
  const dosePart = [str(r.DOSE), str(r.UnitUsage).trim()].filter(Boolean).join(' ');
  const namePart = prop ? `${item} (${prop})` : item;
  const lieu = dosePart ? `Liều/lần: ${dosePart}` : '';
  return [`${n}/`, namePart, lieu].filter(Boolean).join('  ');
}

/** Dòng 2: tần suất + lời dặn ngắn (vd: 2 lần · sáng chiều) */
function formatRxItemLine2(r) {
  const f = str(r.FREQUENCY).trim();
  const ins = str(r.INSTRUCTIONS).trim();
  const left = /^\d+(\.\d+)?$/.test(f) ? `${f} lần` : f;
  if (left && ins) return `${left} · ${ins}`;
  if (left) return left;
  return ins;
}

/** Dòng 3: câu đầy đủ kiểu mẫu toa (Ngày uống X lần/ngày, mỗi lần …) */
function formatRxItemLine3(r) {
  const f = str(r.FREQUENCY).trim();
  const fn = /^\d+(\.\d+)?$/.test(f) ? f : (f || '—');
  const dose = [str(r.DOSE), str(r.UnitUsage).trim()].filter(Boolean).join(' ') || '—';
  const ins = str(r.INSTRUCTIONS).trim() || '—';
  return `Ngày uống ${fn} lần/ngày, mỗi lần ${dose} (${ins})`;
}

/** Ô <<PerD>> mẫu toa: tần suất/ngày */
function formatRxPerDLine(r) {
  const f = str(r.FREQUENCY).trim();
  if (!f) return '';
  if (/^\d+(\.\d+)?$/.test(f)) return `${f} lần/ngày`;
  return f;
}

/**
 * Ô <<DPL>> sau “, mỗi lần”: chỉ khi có DOSE; null/rỗng thì để trống (không gán REPEATS hay chỉ UnitUsage).
 */
function formatRxDplCell(r) {
  const d = str(r.DOSE).trim();
  if (!d) return '';
  const u = str(r.UnitUsage).trim();
  return u ? `${d} ${u}` : d;
}

/** Ô <<Note>> trên mẫu toa = đúng cột INSTRUCTIONS (ViewRX), không ghép tần suất / câu “Ngày uống…” */
function formatRxNoteBlock(r) {
  return str(r.INSTRUCTIONS).trim();
}

/** Trường "Số lượng" tổng hợp: có cả đơn vị (gói, vỉ, …) từ ViewRX.UNITNAME */
function formatSummaryQuantity(rxLines) {
  const rows = rxLines || [];
  if (!rows.length) return '';
  const parts = rows
    .map((r) => {
      const q = r.QUANTITY != null && r.QUANTITY !== '' ? str(r.QUANTITY).trim() : '';
      const u = str(r.UNITNAME).trim();
      if (!q && !u) return '';
      if (q && u) return `${q} ${u}`;
      return q || u;
    })
    .filter(Boolean);
  if (parts.length) return [...new Set(parts)].join('; ');
  return str(rows.length);
}

function formatBirthYear(dob) {
  if (!dob) return '';
  const d = new Date(dob);
  if (Number.isNaN(d.getTime())) return '';
  return String(d.getFullYear());
}

function vitalRowHasData(row) {
  if (!row) return false;
  return (
    row.Pulse != null ||
    row.Temp != null ||
    row.Systolic != null ||
    row.Diastolic != null ||
    row.Resp != null
  );
}

module.exports = {
  str,
  escapeXmlText,
  formatRxDisplayLine,
  formatRxTotalCell,
  formatRxNameCell,
  formatRxHowCell,
  formatRxScheduleCell,
  formatRxItemLine1,
  formatRxItemLine1Split,
  formatRxItemLine2,
  formatRxItemLine3,
  formatRxPerDLine,
  formatRxDplCell,
  formatRxNoteBlock,
  formatSummaryQuantity,
  formatBirthYear,
  vitalRowHasData,
};
