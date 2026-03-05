# Demo Script - Pernod Ricard Japan Supplier Intelligence

## What We Built

**Pernod Ricard Japan Supplier Intelligence Hub** — an end-to-end AI-powered supplier data management platform built entirely on Databricks.

### The Problem

Pernod Ricard Japan manages 500+ suppliers across Japan and Asia. Supplier data arrives from multiple sources in inconsistent formats: mixed Japanese and English names (山田ガラス工業株式会社 vs Yamada Glass Industries), non-standard categories (ガラスびん, Glass Bottles, glass bottle), messy addresses, and different currency notations. Manually standardizing and validating this data is time-consuming and error-prone.

### What the Platform Does

The platform processes supplier data through 4 layers, all running on Databricks:

**Layer 1 — AI SQL Cleaning.** Raw bilingual supplier data is ingested and cleaned using Databricks AI SQL Functions (`ai_query()`). The AI standardizes 500 suppliers into 10 product categories, parses free-text Japanese/English addresses into structured fields (prefecture, city, postal code), and detects 444 duplicate pairs across languages — matching `札幌フィルター工業` to `Sapporo Filter Industries` automatically.

**Layer 2 — Knowledge Assistant.** Three procurement policy documents (supplier qualification policy, compliance checklist, supplier scorecard methodology) are indexed using Vector Search. A Knowledge Assistant built with Agent Bricks provides RAG-grounded answers about certification requirements (ISO 22000, FSSC 22000, HACCP), pricing limits, audit schedules, and Japan-specific compliance (食品衛生法, 酒税法).

**Layer 3 — Agent Processing.** Rule-based anomaly detection flags suppliers for price anomalies, excessive lead times, missing certifications, low reliability, and missing audits. The system auto-approves 118 clean suppliers to a Gold table and escalates 382 suppliers with issues to a review queue, categorized by risk level (HIGH/MEDIUM/LOW).

**Layer 4 — Business Surfaces.** Four ways to interact with the data:
- **Genie Space** — natural language SQL queries over supplier tables
- **Knowledge Assistant** — policy Q&A with document citations
- **Supervisor Agent** — multi-agent orchestration routing questions to both the KA and Genie
- **Custom Agent** — code-based LangGraph agent (MLflow 3) combining Vector Search RAG with 5 UC Function tools

### What the App Does

The Databricks App (React/Node.js) provides a unified interface for procurement managers:

- **Dashboard** — Real-time KPIs: total supplier count (500), auto-approved vs escalated breakdown (118/382), certification coverage (74.4%), average reliability scores, lead time metrics, risk distribution chart, suppliers by category and by prefecture. Includes a visual architecture diagram showing the data flow from raw data through AI processing to gold tables.

- **Upload & Process** — One-click trigger for the full supplier processing pipeline (ingest → AI cleaning → duplicate detection → anomaly flagging → gold tables). Shows pipeline step visualization and job run history with status tracking.

- **Knowledge Assistant** — Chat interface powered by the Agent Bricks Knowledge Assistant endpoint with Vector Search RAG. Answers questions about procurement policies grounded in actual company documents, with source citations (supplier_policy.md, compliance_checklist.txt, scorecard_methodology.md).

- **Review Queue** — Filterable, searchable table of all 382 escalated suppliers. Shows supplier ID, name, category, risk level, and specific issues (PRICE_ANOMALY, MISSING_CERTIFICATION, LOW_RELIABILITY, LEAD_TIME_ANOMALY). Managers can approve or reject each supplier directly from the app, writing decisions back to Delta tables.

### Key Databricks Features Demonstrated

- **AI SQL Functions** — `ai_query()` for data standardization at scale
- **Agent Bricks** — No-code Knowledge Assistant and Supervisor Agent
- **Unity Catalog Functions** — 5 SQL functions registered as AI agent tools, callable from Playground
- **Vector Search** — Managed embedding index over procurement docs for RAG
- **MLflow 3 + LangGraph** — Custom code-based agent with ResponsesAgent pattern
- **Genie Space** — Natural language SQL exploration
- **Databricks Apps** — Full-stack web application with SP OAuth authentication
- **Serverless compute** — Everything runs on serverless (no clusters)

---

## Prerequisites
- Workspace: https://fevm-serverless-opm.cloud.databricks.com/
- All agents deployed and endpoints READY

## Agent Endpoints

| Agent | Endpoint | Tile ID |
|-------|----------|---------|
| Knowledge Assistant | `ka-94303de4-endpoint` | `94303de4-1113-4490-9c6e-48d6f8ccedc8` |
| Supervisor | `mas-f1bf4d08-endpoint` | `f1bf4d08-d05c-4e21-9267-a298006e3478` |
| Custom Agent (MLflow 3) | `supplier-intelligence-agent` | N/A |
| Genie Space | N/A (space ID: `01f118869d191f1f97ee38c28c8b2c4c`) | N/A |

## Demo Flow

### Act 1: The Problem (2 min)
Open the SQL Editor and show the raw data:

```sql
-- Show the messy raw supplier data
SELECT supplier_name, category, address, certification, currency
FROM opm_catalog.supplier_hub.raw_suppliers
LIMIT 20;
```

Point out:
- Japanese and English names mixed (`山田ガラス工業株式会社` vs `Yamada Glass Industries Co., Ltd.`)
- Inconsistent categories (`ガラスびん`, `Glass Bottles`, `glass bottle`, `ボトル`)
- Messy addresses (mix of formats)
- Different currency notations (`JPY`, `円`, `yen`)

### Act 2: AI SQL Cleaning (3 min)
Show the cleaned data:

```sql
-- Categories standardized by AI
SELECT standardized_category, COUNT(*) as cnt
FROM opm_catalog.supplier_hub.suppliers_cleaned
GROUP BY standardized_category
ORDER BY cnt DESC;

-- AI detected duplicates across languages
SELECT name_a, name_b, is_duplicate
FROM opm_catalog.supplier_hub.duplicate_candidates
WHERE TRIM(is_duplicate) = 'YES'
LIMIT 10;
```

Point out how AI matched `札幌フィルター工業` = `Sapporo Filter Industries`.

### Act 3: UC Functions as Tools (3 min)
Go to: **SQL Editor** and test the UC functions directly:

```sql
-- 1. Get a full supplier overview with delivery metrics
SELECT * FROM opm_catalog.supplier_hub.get_supplier_overview('SUP-0001');

-- 2. List all non-compliant/escalated suppliers by risk
SELECT * FROM opm_catalog.supplier_hub.list_non_compliant_suppliers() LIMIT 10;

-- 3. Spend breakdown by category
SELECT * FROM opm_catalog.supplier_hub.get_spend_by_category();

-- 4. Audit delivery quality for a specific supplier
SELECT * FROM opm_catalog.supplier_hub.validate_delivery_quality('SUP-0001');

-- 5. Portfolio risk summary
SELECT * FROM opm_catalog.supplier_hub.get_supplier_risk_summary();
```

Then show these same functions in **AI/ML > Playground** as tools — the AI agent can call them automatically.

### Act 4: Knowledge Assistant - Playground (3 min)
Go to: **AI/ML > Playground**

Select endpoint: `ka-94303de4-endpoint`

Ask these questions:

1. **"What certifications are mandatory for food-contact suppliers?"**
   - Expected: ISO 22000, FSSC 22000, or HACCP (mandatory), plus ISO 9001

2. **"What are the acceptable price ranges for glass bottles?"**
   - Expected: Min 80 JPY, Max 5,000 JPY per policy

3. **"What is the audit frequency for Tier 1 suppliers?"**
   - Expected: Annual audit required

4. **"What are the Japan-specific compliance requirements?"**
   - Expected: Food Safety Act (食品衛生法), Liquor Tax Act (酒税法), Container & Packaging Recycling Act

### Act 5: Genie Space (3 min)
Go to: **SQL > Genie** or URL: `https://fevm-serverless-opm.cloud.databricks.com/genie/rooms/01f118869d191f1f97ee38c28c8b2c4c`

Ask these questions:

1. **"How many suppliers were auto-approved vs escalated?"**
   - Expected: ~118 approved, ~382 escalated

2. **"Show me the top 5 glass bottle suppliers by reliability"**
   - Expected: Table with supplier names and reliability scores

3. **"What is the average lead time by category?"**
   - Expected: Breakdown by all 10 categories

4. **"Which suppliers in the escalation queue have HIGH risk level?"**
   - Expected: List of high-risk suppliers with issues

### Act 6: Custom Agent with Tools (3 min)
Go to: **AI/ML > Playground**

Select endpoint: `supplier-intelligence-agent`

This agent combines Vector Search RAG + UC Function tools in a single LangGraph agent built with MLflow 3.

Ask these questions:

1. **"What certifications are required and how many suppliers are missing them?"**
   - Should use vector search for policy, then call list_non_compliant_suppliers()

2. **"Give me a full overview of supplier SUP-0001"**
   - Should call get_supplier_overview('SUP-0001')

3. **"What is our total spend by category?"**
   - Should call get_spend_by_category()

4. **"According to policy, what lead time is acceptable for glass bottles? And validate the delivery quality for SUP-0010."**
   - Should use vector search for policy, then call validate_delivery_quality('SUP-0010')

5. **"What is our risk exposure across the portfolio?"**
   - Should call get_supplier_risk_summary()

### Act 7: Supervisor Agent - Playground (3 min)
Go to: **AI/ML > Playground**

Select endpoint: `mas-f1bf4d08-endpoint`

Ask these combined questions:

1. **"What certifications are required and which suppliers are missing them?"**
   - Should route to BOTH policy + data agents

2. **"Are our packaging suppliers compliant with lead time requirements?"**
   - Should check policy (30 days max) then query data

3. **"Show me the supplier health summary"**
   - Should query data for overview stats

### Act 8: The App (2 min)
Open: `https://pernod-supplier-hub-7474649123061285.aws.databricksapps.com`

Walk through:
1. **Dashboard tab** - KPI overview with charts
2. **Upload & Process tab** - Show pipeline trigger
3. **Knowledge Assistant tab** - Chat with the AI
4. **Review Queue tab** - Show escalated suppliers, approve/reject

## Key Talking Points

- **Layered architecture**: Raw -> AI SQL Cleaning -> Agent Processing -> Business Surfaces
- **Three agent patterns**: Agent Bricks KA (no-code RAG), Agent Bricks Supervisor (multi-agent), Custom Agent (code-based MLflow 3 + LangGraph)
- **UC Functions as tools**: SQL functions registered in Unity Catalog, callable by AI agents in Playground
- **Vector Search**: Document chunks embedded and indexed for semantic retrieval
- **Bilingual AI**: Handles Japanese/English automatically
- **Policy-grounded**: All decisions traced back to company procurement policies
- **Single platform**: Data, AI, governance all in Databricks — no stitching tools together
