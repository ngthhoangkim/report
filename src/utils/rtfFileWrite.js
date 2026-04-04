const fs = require('fs');

/**
 * Ghi file .rtf cho Word / LibreOffice convert.
 * BOM UTF-8 (mặc định bật) giúp Word trên Windows nhận đúng Unicode khi RTF có ký tự > U+007F.
 */
function writeRtfFileForOffice(rtfPath, rtfText) {
  const bomOn =
    String(process.env.RTF_WRITE_UTF8_BOM || 'true').toLowerCase() !== 'false';
  const body = rtfText == null ? '' : String(rtfText);
  const content = bomOn ? `\uFEFF${body}` : body;
  fs.writeFileSync(rtfPath, content, 'utf8');
}

module.exports = { writeRtfFileForOffice };
