-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Layer 1: AI-Powered Supplier Data Cleaning
-- MAGIC
-- MAGIC This notebook uses **Databricks AI SQL Functions** to automatically:
-- MAGIC 1. Standardize product categories (JP/EN -> unified taxonomy)
-- MAGIC 2. Extract structured address fields from free-text
-- MAGIC 3. Detect potential duplicate suppliers
-- MAGIC 4. Normalize currency and units

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 1: Inspect Raw Data

-- COMMAND ----------

SELECT * FROM opm_catalog.supplier_hub.raw_suppliers LIMIT 20

-- COMMAND ----------

SELECT category, COUNT(*) as cnt
FROM opm_catalog.supplier_hub.raw_suppliers
GROUP BY category
ORDER BY cnt DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 2: Standardize Categories using AI

-- COMMAND ----------

CREATE OR REPLACE TABLE opm_catalog.supplier_hub.suppliers_category_standardized AS
SELECT
  *,
  ai_query(
    'databricks-claude-sonnet-4-5',
    CONCAT(
      'Classify this supplier category into exactly one of these standard categories: ',
      'Glass Bottles, Packaging, Labels & Printing, Corks & Caps, Raw Materials, ',
      'Logistics & Transport, Quality Inspection, Brewing Equipment, Fruit Processing, Water Management. ',
      'The input may be in Japanese or English. Return ONLY the standard category name in English, nothing else. ',
      'Input category: ', category
    )
  ) AS standardized_category
FROM opm_catalog.supplier_hub.raw_suppliers

-- COMMAND ----------

-- Verify standardization
SELECT standardized_category, COUNT(*) as cnt
FROM opm_catalog.supplier_hub.suppliers_category_standardized
GROUP BY standardized_category
ORDER BY cnt DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 3: Extract Structured Address Fields using AI

-- COMMAND ----------

CREATE OR REPLACE TABLE opm_catalog.supplier_hub.suppliers_address_parsed AS
SELECT
  s.*,
  ai_query(
    'databricks-claude-sonnet-4-5',
    CONCAT(
      'Parse this Japanese/English address into a JSON object with these exact keys: ',
      '"postal_code", "prefecture", "city", "street_address", "prefecture_en". ',
      'If a field cannot be determined, use null. For prefecture_en, always provide the English name. ',
      'Return ONLY valid JSON, no other text. ',
      'Address: ', s.address
    )
  ) AS parsed_address_json
FROM opm_catalog.supplier_hub.suppliers_category_standardized s

-- COMMAND ----------

-- Extract individual fields from parsed JSON
CREATE OR REPLACE TABLE opm_catalog.supplier_hub.suppliers_cleaned AS
SELECT
  supplier_id,
  supplier_name,
  standardized_category,
  address AS original_address,
  COALESCE(GET_JSON_OBJECT(parsed_address_json, '$.postal_code'), '') AS postal_code,
  COALESCE(GET_JSON_OBJECT(parsed_address_json, '$.prefecture'), prefecture) AS prefecture_jp,
  COALESCE(GET_JSON_OBJECT(parsed_address_json, '$.prefecture_en'), '') AS prefecture_en,
  COALESCE(GET_JSON_OBJECT(parsed_address_json, '$.city'), '') AS city,
  COALESCE(GET_JSON_OBJECT(parsed_address_json, '$.street_address'), '') AS street_address,
  phone,
  contact_person,
  certification,
  unit_price,
  CASE
    WHEN currency IN ('円', 'yen', 'JPY') THEN 'JPY'
    WHEN currency IN ('USD', '$') THEN 'USD'
    WHEN currency IN ('CNY', '元') THEN 'CNY'
    ELSE UPPER(currency)
  END AS currency_standardized,
  lead_time_days,
  reliability_score,
  last_audit_date,
  source_system,
  ingestion_date
FROM opm_catalog.supplier_hub.suppliers_address_parsed

-- COMMAND ----------

SELECT * FROM opm_catalog.supplier_hub.suppliers_cleaned LIMIT 20

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 4: Detect Potential Duplicates using AI

-- COMMAND ----------

CREATE OR REPLACE TABLE opm_catalog.supplier_hub.duplicate_candidates AS
WITH supplier_pairs AS (
  SELECT
    a.supplier_id AS id_a,
    b.supplier_id AS id_b,
    a.supplier_name AS name_a,
    b.supplier_name AS name_b,
    a.standardized_category AS cat_a,
    b.standardized_category AS cat_b,
    a.prefecture_en AS pref_a,
    b.prefecture_en AS pref_b
  FROM opm_catalog.supplier_hub.suppliers_cleaned a
  JOIN opm_catalog.supplier_hub.suppliers_cleaned b
    ON a.supplier_id < b.supplier_id
    AND a.standardized_category = b.standardized_category
)
SELECT
  id_a, id_b, name_a, name_b, cat_a, pref_a, pref_b,
  ai_query(
    'databricks-claude-sonnet-4-5',
    CONCAT(
      'Are these two supplier names likely the same company? Consider Japanese/English variations, ',
      'abbreviations like (株) vs 株式会社 vs Co. Ltd., and minor spelling differences. ',
      'Reply with ONLY "YES" or "NO". ',
      'Name 1: ', name_a, ' | Name 2: ', name_b
    )
  ) AS is_duplicate
FROM supplier_pairs

-- COMMAND ----------

-- Show detected duplicates
SELECT * FROM opm_catalog.supplier_hub.duplicate_candidates
WHERE TRIM(is_duplicate) = 'YES'
ORDER BY name_a

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 5: Summary Statistics

-- COMMAND ----------

SELECT
  COUNT(*) AS total_suppliers,
  COUNT(DISTINCT standardized_category) AS unique_categories,
  SUM(CASE WHEN certification IS NOT NULL THEN 1 ELSE 0 END) AS certified_suppliers,
  SUM(CASE WHEN certification IS NULL THEN 1 ELSE 0 END) AS missing_certification,
  ROUND(AVG(reliability_score), 2) AS avg_reliability,
  ROUND(AVG(lead_time_days), 1) AS avg_lead_time_days
FROM opm_catalog.supplier_hub.suppliers_cleaned

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Data cleaning complete. Proceed to Layer 2 (Knowledge Assistant) and Layer 3 (Agent Processing).
