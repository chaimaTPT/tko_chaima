import express from 'express';
import path from 'path';
import { fileURLToPath } from 'url';
import { executeSQL } from './databricks.js';
import { chatWithKA } from './llm.js';
import { triggerPipeline, listJobRuns } from './jobs.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const app = express();
const PORT = process.env.PORT || 8000;

app.use(express.json());
app.use(express.static(path.join(__dirname, '../client/dist')));

// ─── Dashboard KPIs ───────────────────────────────────────────────
app.get('/api/dashboard', async (req, res) => {
  try {
    const queries = {
      totalSuppliers: `SELECT COUNT(DISTINCT supplier_id) as count FROM opm_catalog.supplier_hub.suppliers_cleaned`,
      approvedCount: `SELECT COUNT(*) as count FROM opm_catalog.supplier_hub.gold_suppliers`,
      escalatedCount: `SELECT COUNT(*) as count FROM opm_catalog.supplier_hub.escalation_queue WHERE review_status = 'PENDING'`,
      byCategory: `SELECT standardized_category as category, COUNT(*) as count FROM opm_catalog.supplier_hub.suppliers_cleaned GROUP BY standardized_category ORDER BY count DESC`,
      byPrefecture: `SELECT prefecture_jp as prefecture, COUNT(*) as count FROM opm_catalog.supplier_hub.suppliers_cleaned WHERE prefecture_jp IS NOT NULL AND prefecture_jp != '' GROUP BY prefecture_jp ORDER BY count DESC LIMIT 15`,
      avgReliability: `SELECT ROUND(AVG(reliability_score), 2) as score FROM opm_catalog.supplier_hub.suppliers_cleaned`,
      avgLeadTime: `SELECT ROUND(AVG(lead_time_days), 1) as days FROM opm_catalog.supplier_hub.suppliers_cleaned`,
      certCoverage: `SELECT
        ROUND(SUM(CASE WHEN certification IS NOT NULL AND certification != '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as pct
        FROM opm_catalog.supplier_hub.suppliers_cleaned`,
      riskBreakdown: `SELECT risk_level, COUNT(*) as count FROM opm_catalog.supplier_hub.escalation_queue GROUP BY risk_level ORDER BY CASE risk_level WHEN 'HIGH' THEN 1 WHEN 'MEDIUM' THEN 2 ELSE 3 END`,
      duplicatesDetected: `SELECT COUNT(*) as count FROM opm_catalog.supplier_hub.duplicate_candidates WHERE TRIM(is_duplicate) = 'YES'`,
    };

    const entries = Object.entries(queries);
    const responses = await Promise.all(
      entries.map(([key, sql]) => executeSQL(sql).then(data => [key, data]).catch(err => {
        console.error(`Dashboard query '${key}' failed:`, err.message?.substring(0, 200));
        return [key, []];
      }))
    );

    const results = {};
    for (const [key, data] of responses) {
      results[key] = data;
    }

    res.json({
      totalSuppliers: results.totalSuppliers?.[0]?.count ?? 0,
      approvedCount: results.approvedCount?.[0]?.count ?? 0,
      escalatedCount: results.escalatedCount?.[0]?.count ?? 0,
      byCategory: results.byCategory ?? [],
      byPrefecture: results.byPrefecture ?? [],
      avgReliability: results.avgReliability?.[0]?.score ?? 0,
      avgLeadTime: results.avgLeadTime?.[0]?.days ?? 0,
      certCoverage: results.certCoverage?.[0]?.pct ?? 0,
      riskBreakdown: results.riskBreakdown ?? [],
      duplicatesDetected: results.duplicatesDetected?.[0]?.count ?? 0,
    });
  } catch (error) {
    console.error('Dashboard error:', error);
    res.status(500).json({ error: error.message });
  }
});

// ─── Escalation Queue ─────────────────────────────────────────────
app.get('/api/escalations', async (req, res) => {
  try {
    const riskFilter = req.query.risk_level;
    let sql = `SELECT supplier_id, supplier_name, category, risk_level, issues, recommendation, review_status FROM opm_catalog.supplier_hub.escalation_queue`;
    if (riskFilter && ['HIGH', 'MEDIUM', 'LOW'].includes(riskFilter.toUpperCase())) {
      sql += ` WHERE risk_level = '${riskFilter.toUpperCase()}'`;
    }
    sql += ` ORDER BY CASE risk_level WHEN 'HIGH' THEN 1 WHEN 'MEDIUM' THEN 2 ELSE 3 END, supplier_name ASC`;
    const data = await executeSQL(sql);
    res.json(data);
  } catch (error) {
    console.error('Escalations error:', error);
    res.status(500).json({ error: error.message });
  }
});

app.post('/api/escalations/:supplierId/review', async (req, res) => {
  try {
    const { supplierId } = req.params;
    const { action } = req.body; // 'APPROVED' or 'REJECTED'
    if (!['APPROVED', 'REJECTED'].includes(action)) {
      return res.status(400).json({ error: 'Invalid action. Must be APPROVED or REJECTED.' });
    }
    const sql = `UPDATE opm_catalog.supplier_hub.escalation_queue SET review_status = '${action}', reviewer = 'app_user' WHERE supplier_id = '${supplierId}'`;
    await executeSQL(sql);
    res.json({ success: true, supplier_id: supplierId, action });
  } catch (error) {
    console.error('Review error:', error);
    res.status(500).json({ error: error.message });
  }
});

// ─── Chat with Knowledge Assistant ───────────────────────────────
app.post('/api/chat', async (req, res) => {
  try {
    const { messages } = req.body;
    const response = await chatWithKA(messages);
    res.json(response);
  } catch (error) {
    console.error('Chat error:', error);
    res.status(500).json({ error: error.message });
  }
});

// ─── Pipeline / Jobs ──────────────────────────────────────────────
app.post('/api/trigger-pipeline', async (req, res) => {
  try {
    const result = await triggerPipeline();
    res.json(result);
  } catch (error) {
    console.error('Pipeline trigger error:', error);
    res.status(500).json({ error: error.message });
  }
});

app.get('/api/job-runs', async (req, res) => {
  try {
    const runs = await listJobRuns();
    res.json(runs);
  } catch (error) {
    console.error('Job runs error:', error);
    res.status(500).json({ error: error.message });
  }
});

// ─── Health check ─────────────────────────────────────────────────
app.get('/api/health', (req, res) => {
  res.json({ status: 'healthy', timestamp: new Date().toISOString() });
});

// ─── SPA fallback ─────────────────────────────────────────────────
app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, '../client/dist/index.html'));
});

app.listen(PORT, () => {
  console.log(`Pernod Ricard Supplier Hub running on port ${PORT}`);
});
