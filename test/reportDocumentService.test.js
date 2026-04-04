/**
 * Unit test (logic strip token / nhận diện BS chỉ định).
 * Chạy: npm test  (Node 18+)
 *
 * Test generate PDF thật theo hồ sơ: npm run test:report -- <fileNum> <sessionId>
 */
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('fs');
const os = require('os');
const path = require('path');
const PizZip = require('pizzip');

const {
  stripImagePlaceholderTokensInDocXml,
  stripImagePlaceholderTokensInDocxPath,
  paragraphLooksLikeReferDoctorLabel,
  classifyReferDoctorParagraphJc,
} = require('../src/services/reportDocumentService');

test('stripImagePlaceholderTokensInDocXml removes paragraph containing __IMG_1__', () => {
  const docXml = `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">
  <w:body>
    <w:p><w:r><w:t>Before</w:t></w:r></w:p>
    <w:p><w:r><w:t>__IMG_1__</w:t></w:r></w:p>
    <w:p><w:r><w:t>After</w:t></w:r></w:p>
  </w:body>
</w:document>`;
  const out = stripImagePlaceholderTokensInDocXml(docXml, ['__IMG_1__']);
  assert.match(out, /Before/);
  assert.match(out, /After/);
  assert.doesNotMatch(out, /__IMG_1__/);
});

test('stripImagePlaceholderTokensInDocXml handles token split across w:t runs', () => {
  const docXml = `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">
  <w:body>
    <w:p>
      <w:r><w:t>__IMG_</w:t></w:r>
      <w:r><w:t>1__</w:t></w:r>
    </w:p>
  </w:body>
</w:document>`;
  const out = stripImagePlaceholderTokensInDocXml(docXml, ['__IMG_1__']);
  assert.doesNotMatch(out, /__IMG_/);
});

test('stripImagePlaceholderTokensInDocxPath rewrites docx on disk', () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'arg-strip-'));
  const docxPath = path.join(tmp, 't.docx');
  const inner = `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">
  <w:body>
    <w:p><w:r><w:t>__IMG_2__</w:t></w:r></w:p>
  </w:body>
</w:document>`;
  const zip = new PizZip();
  zip.file('word/document.xml', inner);
  fs.writeFileSync(docxPath, zip.generate({ type: 'nodebuffer' }));

  stripImagePlaceholderTokensInDocxPath(docxPath, ['__IMG_2__']);

  const z2 = new PizZip(fs.readFileSync(docxPath));
  const xml = z2.file('word/document.xml').asText();
  assert.doesNotMatch(xml, /__IMG_2__/);
  fs.rmSync(tmp, { recursive: true, force: true });
});

test('paragraphLooksLikeReferDoctorLabel: BS Chỉ định / Bác sĩ chỉ định', () => {
  assert.equal(
    paragraphLooksLikeReferDoctorLabel('BS Chỉ định: PHAN TRUNG HIẾU', []),
    true,
  );
  assert.equal(
    paragraphLooksLikeReferDoctorLabel('Bác sĩ chỉ định: NGUYỄN VĂN A', []),
    true,
  );
  assert.equal(
    paragraphLooksLikeReferDoctorLabel('Kết quả siêu âm bình thường', []),
    false,
  );
});

test('classifyReferDoctorParagraphJc respects REPORT_ALIGN_REFER_DOCTOR_LINES', () => {
  const prev = process.env.REPORT_ALIGN_REFER_DOCTOR_LINES;
  try {
    process.env.REPORT_ALIGN_REFER_DOCTOR_LINES = 'false';
    assert.equal(
      classifyReferDoctorParagraphJc('BS Chỉ định: X', []),
      null,
    );
    process.env.REPORT_ALIGN_REFER_DOCTOR_LINES = 'true';
    assert.equal(classifyReferDoctorParagraphJc('BS Chỉ định: X', []), 'right');
  } finally {
    if (prev === undefined) delete process.env.REPORT_ALIGN_REFER_DOCTOR_LINES;
    else process.env.REPORT_ALIGN_REFER_DOCTOR_LINES = prev;
  }
});
