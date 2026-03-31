const fs = require('fs');
const path = require('path');
const logger = require('../utils/logger');

/** Giới hạn số ảnh theo tên file template (không fallback template). */
const TemplateImageLimits = {
  'UltraSoundResultTemplate 1H.doc': 1,
  'UltraSoundResultTemplate.doc': 2,
  'NoiSoi 9H.doc': 9,
  'NoiSoiMoi.doc': 4,
  'SoiCTC.doc': 2,
  'XrayResultTemplate.doc': 1,
};

/**
 * Chỉ dùng đúng file trong TemplateFile (CN_ImagingResult), có trên disk trong PATHS_TEMPLATES.
 * Không fallback sang template mặc định; không đổi .doc ↔ .docx.
 */
class TemplateSelector {
  constructor(templateBasePath) {
    this.templateBasePath = templateBasePath;
  }

  /**
   * @returns {string|null} đường dẫn tuyệt đối hoặc null
   */
  selectTemplate(templateFile) {
    logger.info(`Selecting template. TemplateFile=${templateFile}`);

    if (!templateFile || !String(templateFile).trim()) {
      logger.warn('No TemplateFile in database — skip segment');
      return null;
    }

    const templateName = path.basename(String(templateFile).trim());
    const templatePath = path.join(this.templateBasePath, templateName);

    if (fs.existsSync(templatePath)) {
      logger.info(`Using template: ${templateName}`);
      return templatePath;
    }

    logger.warn(`Template file not found: ${templatePath}`);
    return null;
  }

  getImageLimit(templatePath) {
    const templateName = path.basename(templatePath);
    if (Object.prototype.hasOwnProperty.call(TemplateImageLimits, templateName)) {
      return TemplateImageLimits[templateName];
    }
    return Number.MAX_SAFE_INTEGER;
  }
}

module.exports = { TemplateSelector };
