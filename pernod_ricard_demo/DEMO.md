# AI-Powered Supplier Data Standardization - Pernod Ricard Japan

## Brand Guidelines
- **Company**: Pernod Ricard Japan (subsidiary of Pernod Ricard Group)
- **Primary Colors**: Deep Navy (#1B2A4A), Gold (#C5A55A), White (#FFFFFF)
- **Accent Colors**: Warm Red (#B83232), Light Gray (#F5F5F5)
- **Font Style**: Clean, professional, modern sans-serif
- **Tone**: Enterprise-grade, sophisticated, data-driven

## Use Case Summary
Pernod Ricard Japan manages hundreds of suppliers across Japan and Asia for raw materials, packaging, and logistics. Supplier data arrives in inconsistent formats across Japanese and English, with different naming conventions, units, and missing fields. The procurement team wastes significant time manually cleaning and reconciling data.

## Architecture

### Layer 1: Data Ingestion & AI SQL Cleaning
- Raw supplier CSV/JSON files ingested into Bronze tables
- Databricks AI SQL Functions (`ai_generate_text`) standardize:
  - Product categories (Japanese -> English standardized taxonomy)
  - Free-text addresses -> structured fields (prefecture, city, postal code)
  - Duplicate detection via fuzzy matching

### Layer 2: Knowledge Assistant
- Loaded with internal procurement documents:
  - Supplier policies
  - Japan-specific compliance (JFSA, food safety)
  - Certification standards (ISO 9001, ISO 22000, FSSC 22000)
  - Historical scorecards
- Answers procurement team questions in natural language

### Layer 3: Multi-Agent Processing
- **Deduplication Agent**: Merges duplicate supplier records into golden records
- **Compliance Agent**: Checks each supplier against Knowledge Assistant rules
- **Anomaly Agent**: Flags unusual prices, lead times, missing certifications
- **Supervisor Agent**: Orchestrates all three, auto-approves or escalates, writes to Gold table

### Layer 4: Business Surfaces
- **Genie Space**: Natural language queries for procurement managers
- **Databricks App**: Upload trigger, Knowledge Assistant chat, review queue, KPI dashboard

## Workspace Configuration
- **Workspace**: https://fevm-serverless-opm.cloud.databricks.com/
- **CLI Profile**: fe-vm-serverless-opm
- **Catalog**: opm_catalog
- **Schemas**: supplier_hub (main), pernod_ricard_demo (staging), agents (agent models)
- **LLM Endpoint**: databricks-claude-sonnet-4-5
- **Warehouse**: Serverless Starter Warehouse (6457618d1c009dd6)

## Key Personas
- **Procurement Manager**: Uses App and Genie Space daily
- **Data Steward**: Reviews flagged records in the queue
- **Procurement Director**: Monitors KPI dashboard
