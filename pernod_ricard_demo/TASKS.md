# TASKS - Pernod Ricard Supplier Demo

## Layer 1: Data Generation & AI SQL Cleaning
- [x] Generate synthetic supplier data (Japanese/English bilingual) - 500 suppliers, 8 products, 2000 deliveries
- [x] Create Bronze raw_suppliers table
- [x] Create AI SQL cleaning notebook - ai_query() for category standardization, address parsing, duplicate detection
- [x] Create Silver cleaned_suppliers table - 10 standardized categories, 444 duplicate pairs detected

## Layer 2: Knowledge Assistant (Agent Bricks)
- [x] Generate sample procurement policy documents - 3 docs (policy, compliance, scorecard)
- [x] Upload docs to Unity Catalog Volume - /Volumes/opm_catalog/supplier_hub/procurement_docs/
- [x] Create Knowledge Assistant via Agent Bricks
  - Tile ID: 94303de4-1113-4490-9c6e-48d6f8ccedc8
  - Endpoint: ka-94303de4-endpoint (PROVISIONING)
  - Indexing: 6 files

## Layer 3: Multi-Agent System
- [x] Create Anomaly detection (rule-based) - flagged 382 suppliers
- [x] Supervisor writes: 118 auto-approved, 382 escalated
- [x] Gold validated_suppliers table - 118 records
- [x] Escalation_queue table - 382 records

## Layer 4: Business Surfaces
- [x] Create Genie Space - ID: 01f118869d191f1f97ee38c28c8b2c4c (5 tables, WORKING)
- [x] Create Supervisor Agent via Agent Bricks
  - Tile ID: f1bf4d08-d05c-4e21-9267-a298006e3478
  - Endpoint: mas-f1bf4d08-endpoint (UPDATING)
  - Sub-agents: procurement_policy (KA) + supplier_data (Genie)
- [x] Build Databricks App (React/Node.js)
- [x] Deploy App - https://pernod-supplier-hub-7474649123061285.aws.databricksapps.com
- [x] Write Demo Script - DEMO_SCRIPT.md

## UC Functions (Tools for AI Playground)
- [x] Fix get_supplier_overview(supplier_id) - supplier profile + delivery metrics
- [x] Fix list_non_compliant_suppliers() - escalation queue sorted by risk
- [x] Fix get_spend_by_category() - spend breakdown across 10 categories
- [x] Fix validate_delivery_quality(supplier_id) - delivery audit with fill rate, lead time variance
- [x] Create get_supplier_risk_summary() - portfolio risk distribution (replaced broken get_schema_mapping)

## Custom Agent (Agent Framework + MLflow 3)
- [x] Create procurement_docs_chunked table (15 chunks from 3 docs)
- [x] Create Vector Search endpoint: pernod-supplier-vs (ONLINE)
- [x] Create Vector Search index: opm_catalog.supplier_hub.procurement_docs_index (SYNCING)
- [x] Build custom agent with LangGraph + ResponsesAgent (agent.py)
  - Tools: VectorSearchRetrieverTool + 5 UC Functions
  - LLM: databricks-claude-sonnet-4-5
- [x] Log model to MLflow: opm_catalog.agents.supplier_intelligence_agent v1
- [x] Deploy serving endpoint: supplier-intelligence-agent (READY)

## Databricks Bundle
- [x] Create databricks.yml with all jobs
- [x] Deploy bundle to workspace
- [x] Run full pipeline - COMPLETED

## URLs
- App: https://pernod-supplier-hub-7474649123061285.aws.databricksapps.com
- Genie Space: https://fevm-serverless-opm.cloud.databricks.com/genie/rooms/01f118869d191f1f97ee38c28c8b2c4c
- KA Playground: Use endpoint ka-94303de4-endpoint in AI/ML > Playground
- Supervisor Playground: Use endpoint mas-f1bf4d08-endpoint in AI/ML > Playground
- Custom Agent: Use endpoint supplier-intelligence-agent in AI/ML > Playground
- Pipeline Job: https://fevm-serverless-opm.cloud.databricks.com/?o=7474649123061285#job/1038497799598839
- Agents Page: https://fevm-serverless-opm.cloud.databricks.com/ml/bricks
- MLflow Experiment: https://fevm-serverless-opm.cloud.databricks.com/ml/experiments/125660485765145
- Model: https://fevm-serverless-opm.cloud.databricks.com/explore/data/models/opm_catalog/agents/supplier_intelligence_agent
