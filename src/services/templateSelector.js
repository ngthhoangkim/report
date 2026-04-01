const fs = require('fs');
const path = require('path');
const logger = require('../utils/logger');
const { PathologyType } = require('../constants/pathologyTypes');

/** Giới hạn số ảnh theo tên file template (fallback templates — .doc). */
const TemplateImageLimits = {
  'UltraSoundResultTemplate 1H.doc': 1,
  'UltraSoundResultTemplate.doc': 2,
  'NoiSoi 9H.doc': 9,
  'NoiSoiMoi.doc': 4,
  'SoiCTC.doc': 2,
  'XrayResultTemplate.doc': 1,
};

/**
 * Chọn template: ưu tiên TemplateFile (CN_ImagingResult) nếu có trên disk;
 * nếu không — fallback theo PathologyType (giống MedicalReportServer.TemplateSelector).
 * Nội soi: không fallback, bắt buộc file template từ DB tồn tại trên disk.
 */
class TemplateSelector {
  constructor(templateBasePath) {
    this.templateBasePath = templateBasePath;
  }

  /**
   * @param {string|null|undefined} templateFile - TemplateFile từ DB
   * @param {number} [pathologyType=0] - PathologyType (0–4)
   * @param {number} [imageCount=0] - số ảnh đã resolve (cho fallback siêu âm)
   * @returns {string|null} đường dẫn tuyệt đối hoặc null
   */
  selectTemplate(templateFile, pathologyType = 0, imageCount = 0) {
    logger.info(
      `Selecting template. TemplateFile=${templateFile}, PathologyType=${pathologyType}, ImageCount=${imageCount}`,
    );

    const base = this.templateBasePath;
    const baseExists = fs.existsSync(base);
    logger.info(`Template base path: ${base} (exists: ${baseExists})`);

    // Priority 1: TemplateFile từ DB
    if (templateFile && String(templateFile).trim()) {
      const templateName = path.basename(String(templateFile).trim());
      const templatePath = path.join(base, templateName);

      if (fs.existsSync(templatePath)) {
        logger.info(`Using template from database: ${templateName}`);
        return templatePath;
      }

      logger.warn(`Template from database not found locally: ${templatePath}`);

      if (baseExists) {
        try {
          const files = fs.readdirSync(base);
          logger.info(`Files in template base path: ${files.join(', ')}`);
        } catch (ex) {
          logger.warn(`Failed to list files in template base path: ${ex.message}`);
        }
      }
      logger.info('Falling back to default template by PathologyType');
    } else {
      logger.warn('No TemplateFile in database — falling back to PathologyType');
    }

    // Priority 2: Fallback theo PathologyType (NoiSoi: null — must have TemplateFile)
    const fallback = this.selectFallbackByPathology(pathologyType, imageCount);
    logger.info(`Fallback template selected: ${fallback}`);
    return fallback;
  }

  /**
   * @returns {string|null}
   */
  selectFallbackByPathology(pathologyType, imageCount) {
    switch (pathologyType) {
      case PathologyType.SieuAm:
        return this.selectUltraSoundFallback(imageCount);
      case PathologyType.XRay:
        return this.selectXrayFallback();
      case PathologyType.NoiSoi:
        logger.warn('NoiSoi requires TemplateFile on disk — no default template');
        return null;
      default:
        return null;
    }
  }

  selectUltraSoundFallback(imageCount) {
    const names =
      imageCount <= 1
        ? ['UltraSoundResultTemplate 1H.doc']
        : ['UltraSoundResultTemplate.doc'];

    for (const name of names) {
      const templatePath = path.join(this.templateBasePath, name);
      if (fs.existsSync(templatePath)) {
        logger.info(`Using UltraSound template: ${name} for ${imageCount} image(s)`);
        return templatePath;
      }
    }
    logger.warn(`UltraSound template not found. Tried: ${names.join(', ')}`);
    return null;
  }

  selectXrayFallback() {
    const names = ['XrayResultTemplate.doc'];
    for (const name of names) {
      const templatePath = path.join(this.templateBasePath, name);
      if (fs.existsSync(templatePath)) {
        logger.info(`Using X-Ray template: ${name}`);
        return templatePath;
      }
    }
    logger.warn(`X-Ray template not found. Tried: ${names.join(', ')}`);
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
