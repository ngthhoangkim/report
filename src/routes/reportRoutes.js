const express = require('express');
const reportController = require('../controllers/reportController');

const router = express.Router();

router.get('/report', reportController.getReportData);
router.get('/xray-result', reportController.getXrayResultBySessionAndFileNum);
router.post('/report/generate', reportController.generateReport);

module.exports = router;
