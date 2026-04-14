require('dotenv').config();
const express = require('express');
const db = require('./config/database');
const reportRoutes = require('./routes/reportRoutes');
const { startAutomation } = require('./services/automationService');
const { startPrescriptionAutomation } = require('./services/prescriptionAutomationService');
const logger = require('./utils/logger');

const app = express();
const port = process.env.PORT || 3000;

app.use(express.json());

// Initialize Database
db.initializePool().catch(err => {
    console.error('Failed to initialize database pool', err);
    process.exit(1);
});

// Routes
app.use('/api', reportRoutes);

// Root endpoint
app.get('/', (req, res) => {
  res.send('Medical Report Generator API is running.');
});

app.listen(port, () => {
  console.log(`Server is running on http://localhost:${port}`);
});

// Start automation worker (polling) — keeps API intact for manual testing.
const automation = startAutomation();
const prescriptionAutomation = startPrescriptionAutomation();

process.on('SIGINT', async () => {
    try {
      automation.stop();
    } catch (_) {
      // ignore
    }
    try {
      prescriptionAutomation.stop();
    } catch (_) {
      // ignore
    }
    await db.closePool();
    process.exit(0);
});

process.on('SIGTERM', async () => {
  try {
    automation.stop();
  } catch (_) {
    // ignore
  }
  try {
    prescriptionAutomation.stop();
  } catch (_) {
    // ignore
  }
  try {
    await db.closePool();
  } catch (e) {
    logger.warn(`DB close on SIGTERM failed: ${e.message}`);
  }
  process.exit(0);
});

