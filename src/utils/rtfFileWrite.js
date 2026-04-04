const fs = require('fs');

/**
 * Ghi file .rtf cho Word / LibreOffice convert.
 *
 * RTF_WRITE_ENCODING:
 * - utf8_bom (mặc định): UTF-8, BOM theo RTF_WRITE_UTF8_BOM (mặc định bật)
 * - utf8: UTF-8 không BOM
 * - utf16le: UTF-16 LE + BOM — Word trên Windows đôi khi xử lý Unicode ổn định hơn UTF-8 cho RTF dài
 */
function writeRtfFileForOffice(rtfPath, rtfText) {
  const body = rtfText == null ? '' : String(rtfText);
  const mode = String(process.env.RTF_WRITE_ENCODING || 'utf8_bom').toLowerCase().trim();

  if (mode === 'utf16le' || mode === 'utf-16le' || mode === 'utf16') {
    const bom = Buffer.from([0xff, 0xfe]);
    const buf = Buffer.from(body, 'utf16le');
    fs.writeFileSync(rtfPath, Buffer.concat([bom, buf]));
    return;
  }

  const bomOn =
    mode === 'utf8'
      ? false
      : String(process.env.RTF_WRITE_UTF8_BOM || 'true').toLowerCase() !== 'false';
  const content = bomOn ? `\uFEFF${body}` : body;
  fs.writeFileSync(rtfPath, content, 'utf8');
}

module.exports = { writeRtfFileForOffice };
