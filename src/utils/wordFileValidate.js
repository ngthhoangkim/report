const fs = require('fs');
const path = require('path');

/**
 * File .doc legacy: magic D0 CF 11 E0 (OLE Compound).
 * File .docx: PK\x03\x04 (ZIP).
 */
function validateWordTemplateFile(absPath) {
  const st = fs.statSync(absPath);
  if (st.size === 0) {
    throw new Error(`Template rỗng (0 byte): ${path.basename(absPath)}`);
  }

  const buf = Buffer.alloc(512);
  const fd = fs.openSync(absPath, 'r');
  try {
    const n = fs.readSync(fd, buf, 0, 512, 0);
    const slice = buf.subarray(0, n);
    if (slice.length && slice.every((b) => b === 0)) {
      throw new Error(
        `Template không hợp lệ (toàn byte 0 — có thể copy lỗi / placeholder). ` +
          `Hãy copy lại file Word thật: ${path.basename(absPath)}`,
      );
    }
    const isOle =
      slice.length >= 8 &&
      slice[0] === 0xd0 &&
      slice[1] === 0xcf &&
      slice[2] === 0x11 &&
      slice[3] === 0xe0;
    const isZip =
      slice.length >= 4 && slice[0] === 0x50 && slice[1] === 0x4b && slice[2] === 0x03 && slice[3] === 0x04;
    if (!isOle && !isZip) {
      throw new Error(
        `Template không phải .doc (OLE) hay .docx (ZIP): ${path.basename(absPath)}`,
      );
    }
  } finally {
    fs.closeSync(fd);
  }
}

module.exports = { validateWordTemplateFile };
