const fs = require('fs');

/**
 * Ghi file .rtf cho Word / LibreOffice convert.
 *
 * Mặc định UTF-8 không BOM — giống C# ReportService.LoadRtf (Encoding.UTF8.GetBytes không thêm BOM);
 * file bắt đầu bằng {\rtf. BOM trước {\rtf có thể làm LO/Word parse sai.
 *
 * RTF_WRITE_ENCODING:
 * - utf8 (mặc định): UTF-8 không BOM
 * - utf8_bom: UTF-8 + BOM (theo RTF_WRITE_UTF8_BOM, mặc định bật khi mode này)
 * - utf16le: UTF-16 LE + BOM
 */
function writeRtfFileForOffice(rtfPath, rtfText) {
  const body = rtfText == null ? '' : String(rtfText);
  const mode = String(process.env.RTF_WRITE_ENCODING || 'utf8').toLowerCase().trim();

  if (mode === 'utf16le' || mode === 'utf-16le' || mode === 'utf16') {
    const bom = Buffer.from([0xff, 0xfe]);
    const buf = Buffer.from(body, 'utf16le');
    fs.writeFileSync(rtfPath, Buffer.concat([bom, buf]));
    return;
  }

  const bomOn =
    mode === 'utf8_bom' &&
    String(process.env.RTF_WRITE_UTF8_BOM || 'true').toLowerCase() !== 'false';
  const content = bomOn ? `\uFEFF${body}` : body;
  fs.writeFileSync(rtfPath, content, 'utf8');
}

module.exports = { writeRtfFileForOffice };
