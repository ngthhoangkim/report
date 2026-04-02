/** Mirrors MedicalReportServer.Core.Enums.PathologyType */
const PathologyType = {
  Unknown: 0,
  Standard: 1,
  XRay: 2,
  NoiSoi: 3,
  SieuAm: 4,
  /** Một số cơ sở map MRI vào giá trị 7 trong CN_ImagingResult.PathologyType */
  Mri: 7,
};

function isSupportedPathologyType(pathologyType) {
  return (
    pathologyType === PathologyType.SieuAm ||
    pathologyType === PathologyType.XRay ||
    pathologyType === PathologyType.NoiSoi
  );
}

module.exports = { PathologyType, isSupportedPathologyType };
