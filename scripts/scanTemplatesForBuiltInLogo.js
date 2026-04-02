/**
 * Quét thư mục template: gợi ý mẫu nào có sẵn ảnh/logo đầu trang (nên tắt chèn logo.jpg).
 *
 * - .docx: đọc word/document.xml — nếu ngay đoạn đầu <w:body> có <w:drawing / blip / pict → "có ảnh sớm".
 * - .doc: không đọc được cấu trúc (OLE) — báo "không tự quét".
 *
 * Chạy:
 *   node scripts/scanTemplatesForBuiltInLogo.js
 *   node scripts/scanTemplatesForBuiltInLogo.js /đường/dẫn/Templates
 */
require('dotenv').config();
const fs = require('fs');
const path = require('path');
const PizZip = require('pizzip');

const argvDir = process.argv[2];
const templatesDir =
  argvDir ||
  process.env.PATHS_TEMPLATES ||
  path.join(process.cwd(), 'Templates');

const PREFIX_LEN = 18000;

function walkFiles(dir, acc = []) {
  if (!fs.existsSync(dir)) return acc;
  const st = fs.statSync(dir);
  if (!st.isDirectory()) return acc;
  for (const name of fs.readdirSync(dir)) {
    if (name.startsWith('.') && name !== '.') continue;
    const full = path.join(dir, name);
    const s = fs.statSync(full);
    if (s.isDirectory()) walkFiles(full, acc);
    else if (/\.(doc|docx)$/i.test(name)) acc.push(full);
  }
  return acc;
}

function analyzeDocx(absPath) {
  try {
    const zip = new PizZip(fs.readFileSync(absPath));
    const docFile = zip.file('word/document.xml');
    if (!docFile) return { ok: false, reason: 'no word/document.xml' };
    const xml = docFile.asText();
    const bodyMatch = xml.match(/<w:body\b[^>]*>/);
    const start = bodyMatch ? bodyMatch.index + bodyMatch[0].length : 0;
    const prefix = xml.slice(start, start + PREFIX_LEN);
    const hasDrawing =
      prefix.includes('<w:drawing') ||
      prefix.includes('pic:pic') ||
      prefix.includes('<a:blip') ||
      prefix.includes('<v:imagedata') ||
      prefix.includes('<w:pict');
    return {
      ok: true,
      earlyImage: hasDrawing,
      hint: hasDrawing
        ? 'likely_skip_logo'
        : 'no_early_drawing_in_prefix',
    };
  } catch (e) {
    return { ok: false, reason: e.message };
  }
}

function main() {
  console.log('Templates dir:', path.resolve(templatesDir));
  const files = walkFiles(templatesDir).sort();
  if (!files.length) {
    console.log('No .doc/.docx files found.');
    process.exit(0);
  }

  const skipLogoNames = [];

  for (const abs of files) {
    const rel = path.relative(templatesDir, abs);
    const ext = path.extname(abs).toLowerCase();
    if (ext === '.docx') {
      const r = analyzeDocx(abs);
      if (!r.ok) {
        console.log(`${rel}\tdocx\tERR\t${r.reason}`);
        continue;
      }
      console.log(
        `${rel}\tdocx\t${r.earlyImage ? 'EARLY_IMAGE' : 'no_early_image'}\t${r.hint}`,
      );
      if (r.earlyImage) skipLogoNames.push(path.basename(abs));
    } else {
      console.log(`${rel}\tdoc\tUNKNOWN\tcannot_scan_legacy_doc`);
    }
  }

  console.log('\n--- Gợi ý .env (basename, kiểm tra tay thêm .doc) ---');
  if (skipLogoNames.length) {
    console.log(`SKIP_LOGO_TEMPLATE_NAMES=${skipLogoNames.join(',')}`);
  } else {
    console.log(
      '# (không có .docx nào phát hiện ảnh sớm — hoặc chỉ có .doc, cần mở Word kiểm tra)',
    );
  }
  console.log('\nLưu ý: MRI.doc đã được code bỏ logo tự động; heuristic chỉ mang tính gợi ý.');
}

main();
