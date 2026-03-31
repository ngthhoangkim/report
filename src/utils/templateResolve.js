const fs = require('fs');
const path = require('path');

function resolveTemplateInDir(templateDir, templateFileName) {
  if (!templateFileName) return { sourcePath: null, fileName: null };
  const direct = path.join(templateDir, templateFileName);
  if (fs.existsSync(direct)) {
    return { sourcePath: direct, fileName: templateFileName };
  }
  const ext = path.extname(templateFileName).toLowerCase();
  const base = path.parse(templateFileName).name;
  if (ext === '.doc') {
    const altName = `${base}.docx`;
    const altPath = path.join(templateDir, altName);
    if (fs.existsSync(altPath)) {
      return { sourcePath: altPath, fileName: altName };
    }
  } else if (ext === '.docx') {
    const altName = `${base}.doc`;
    const altPath = path.join(templateDir, altName);
    if (fs.existsSync(altPath)) {
      return { sourcePath: altPath, fileName: altName };
    }
  }
  return { sourcePath: null, fileName: null };
}

module.exports = { resolveTemplateInDir };
