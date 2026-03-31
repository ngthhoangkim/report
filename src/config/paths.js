const path = require('path');

/**
 * Same keys as MedicalReportServer appsettings Paths:* (HA-agnostic).
 */
function getPaths() {
  const root = process.cwd();
  return {
    templates: process.env.PATHS_TEMPLATES || path.join(root, 'template'),
    output: process.env.PATHS_OUTPUT || path.join(root, 'output', 'CDHA'),
    /** Mặc định `./img` — chỗ đặt file theo `FileName` / ảnh lẻ trùng `CN_PathologyImage.Filename`. */
    sourceImageDir:
      process.env.PATHS_SOURCE_IMAGE_DIR !== undefined &&
      process.env.PATHS_SOURCE_IMAGE_DIR !== ''
        ? process.env.PATHS_SOURCE_IMAGE_DIR
        : path.join(root, 'img'),
    fallbackImageDir: process.env.PATHS_FALLBACK_IMAGE_DIR || path.join(root, 'documents2'),
    localImageDir: process.env.PATHS_LOCAL_IMAGE_DIR || path.join(root, 'Documents'),
  };
}

module.exports = { getPaths };
