"""
Layer 3: Multi-Agent Supplier Processing System

Three specialized agents orchestrated by a supervisor:
1. Deduplication Agent - merges duplicate supplier records
2. Compliance Agent - checks against procurement policies
3. Anomaly Agent - flags unusual prices, lead times, missing certs

The Supervisor orchestrates all three, auto-approves or escalates,
and writes validated records to the Gold table.
"""
import os
import json
from databricks.sdk import WorkspaceClient
from databricks.connect import DatabricksSession

PROFILE = "fe-vm-serverless-opm"
CATALOG = "opm_catalog"
SCHEMA = "supplier_hub"
LLM_ENDPOINT = "databricks-claude-sonnet-4-5"


def get_spark():
    if os.environ.get("DATABRICKS_RUNTIME_VERSION"):
        from pyspark.sql import SparkSession
        return SparkSession.builder.getOrCreate()
    else:
        return (DatabricksSession.builder
                .serverless(True)
                .profile(PROFILE)
                .getOrCreate())


def get_workspace_client():
    if os.environ.get("DATABRICKS_RUNTIME_VERSION"):
        return WorkspaceClient()
    return WorkspaceClient(profile=PROFILE)


def call_llm(w, prompt, max_tokens=2000):
    """Call the LLM endpoint."""
    response = w.serving_endpoints.query(
        name=LLM_ENDPOINT,
        messages=[{"role": "user", "content": prompt}],
        max_tokens=max_tokens,
        temperature=0.1,
    )
    return response.choices[0].message.content


def run_deduplication_agent(spark, w):
    """Agent 1: Identify and merge duplicate suppliers into golden records."""
    print("\n=== DEDUPLICATION AGENT ===")

    dupes_df = spark.sql(f"""
        SELECT * FROM {CATALOG}.{SCHEMA}.duplicate_candidates
        WHERE TRIM(is_duplicate) = 'YES'
    """)

    dupes = dupes_df.collect()
    if not dupes:
        print("No duplicates found.")
        return []

    print(f"Found {len(dupes)} duplicate pairs to process")
    golden_records = []

    for row in dupes:
        # Get full records for both suppliers
        supplier_a = spark.sql(f"""
            SELECT * FROM {CATALOG}.{SCHEMA}.suppliers_cleaned
            WHERE supplier_id = '{row.id_a}'
        """).collect()
        supplier_b = spark.sql(f"""
            SELECT * FROM {CATALOG}.{SCHEMA}.suppliers_cleaned
            WHERE supplier_id = '{row.id_b}'
        """).collect()

        if not supplier_a or not supplier_b:
            continue

        a = supplier_a[0].asDict()
        b = supplier_b[0].asDict()

        prompt = f"""You are a data steward merging duplicate supplier records.
Merge these two records into one golden record. Choose the most complete and accurate value for each field.
Keep the supplier_id from record A. Return ONLY a JSON object with the merged fields.

Record A: {json.dumps(a, default=str)}
Record B: {json.dumps(b, default=str)}

Return a JSON with keys: supplier_id, supplier_name, standardized_category, postal_code, prefecture_en, city, street_address, phone, contact_person, certification, unit_price, currency_standardized, lead_time_days, reliability_score, last_audit_date, merged_from (array of original IDs).
"""
        try:
            result = call_llm(w, prompt)
            # Extract JSON from response
            json_start = result.find("{")
            json_end = result.rfind("}") + 1
            if json_start >= 0 and json_end > json_start:
                merged = json.loads(result[json_start:json_end])
                golden_records.append(merged)
                print(f"  Merged: {row.name_a} + {row.name_b} -> {merged.get('supplier_name', 'N/A')}")
        except Exception as e:
            print(f"  Error merging {row.id_a}/{row.id_b}: {e}")

    print(f"Created {len(golden_records)} golden records")
    return golden_records


def run_compliance_agent(spark, w):
    """Agent 2: Check each supplier against compliance rules."""
    print("\n=== COMPLIANCE AGENT ===")

    suppliers = spark.sql(f"""
        SELECT * FROM {CATALOG}.{SCHEMA}.suppliers_cleaned
    """).collect()

    print(f"Checking {len(suppliers)} suppliers for compliance")
    results = []

    for sup in suppliers:
        s = sup.asDict()
        prompt = f"""You are a procurement compliance officer for Pernod Ricard Japan.
Check this supplier against these rules and return a JSON assessment:

RULES:
- Food-contact suppliers MUST have ISO 22000, FSSC 22000, or HACCP
- All suppliers MUST have ISO 9001
- Reliability score must be >= 0.65
- Last audit must be within 12 months for critical suppliers
- Missing certification is a red flag

SUPPLIER:
- Name: {s.get('supplier_name')}
- Category: {s.get('standardized_category')}
- Certification: {s.get('certification', 'NONE')}
- Reliability: {s.get('reliability_score', 'N/A')}
- Last Audit: {s.get('last_audit_date', 'NONE')}

Return ONLY a JSON with keys:
- supplier_id: "{s.get('supplier_id')}"
- compliant: true/false
- issues: array of issue strings (empty if compliant)
- risk_level: "LOW", "MEDIUM", or "HIGH"
- recommendation: brief recommendation string
"""
        try:
            result = call_llm(w, prompt, max_tokens=500)
            json_start = result.find("{")
            json_end = result.rfind("}") + 1
            if json_start >= 0 and json_end > json_start:
                assessment = json.loads(result[json_start:json_end])
                assessment["supplier_id"] = s.get("supplier_id")
                results.append(assessment)
                if not assessment.get("compliant", True):
                    print(f"  Non-compliant: {s.get('supplier_name')} - {assessment.get('risk_level')} risk")
        except Exception as e:
            print(f"  Error checking {s.get('supplier_id')}: {e}")

    compliant = sum(1 for r in results if r.get("compliant"))
    print(f"Results: {compliant}/{len(results)} compliant")
    return results


def run_anomaly_agent(spark, w):
    """Agent 3: Flag anomalies in pricing, lead times, and certifications."""
    print("\n=== ANOMALY AGENT ===")

    # Price anomaly thresholds by category (from policy doc)
    price_limits = {
        "Glass Bottles": 5000, "Packaging": 3000, "Labels & Printing": 1500,
        "Corks & Caps": 2000, "Raw Materials": 50000, "Logistics & Transport": 100000,
        "Quality Inspection": 200000, "Brewing Equipment": 500000,
        "Fruit Processing": 30000, "Water Management": 50000,
    }
    lead_time_limits = {
        "Glass Bottles": 45, "Packaging": 30, "Labels & Printing": 21,
        "Corks & Caps": 30, "Raw Materials": 60, "Logistics & Transport": 14,
        "Quality Inspection": 30, "Brewing Equipment": 60,
        "Fruit Processing": 45, "Water Management": 30,
    }

    suppliers = spark.sql(f"""
        SELECT * FROM {CATALOG}.{SCHEMA}.suppliers_cleaned
    """).collect()

    anomalies = []
    for sup in suppliers:
        s = sup.asDict()
        flags = []
        cat = s.get("standardized_category", "")

        # Price check
        price = s.get("unit_price")
        max_price = price_limits.get(cat, 50000)
        if price and price > max_price * 3:
            flags.append(f"PRICE_ANOMALY: {price} JPY exceeds 3x category max ({max_price * 3})")

        # Lead time check
        lead = s.get("lead_time_days")
        max_lead = lead_time_limits.get(cat, 60)
        if lead and lead > max_lead * 2:
            flags.append(f"LEAD_TIME_ANOMALY: {lead} days exceeds 2x category max ({max_lead * 2})")

        # Missing certification
        cert = s.get("certification")
        if not cert or cert == "None":
            flags.append("MISSING_CERTIFICATION: No certification on record")

        # Low reliability
        rel = s.get("reliability_score")
        if rel is not None and rel < 0.65:
            flags.append(f"LOW_RELIABILITY: Score {rel} below minimum 0.65")

        # Missing audit
        audit = s.get("last_audit_date")
        if not audit or audit == "None":
            flags.append("MISSING_AUDIT: No audit date on record")

        if flags:
            anomalies.append({
                "supplier_id": s.get("supplier_id"),
                "supplier_name": s.get("supplier_name"),
                "category": cat,
                "anomaly_flags": flags,
                "anomaly_count": len(flags),
                "severity": "HIGH" if len(flags) >= 3 else ("MEDIUM" if len(flags) >= 2 else "LOW"),
            })

    print(f"Flagged {len(anomalies)} suppliers with anomalies")
    high = sum(1 for a in anomalies if a["severity"] == "HIGH")
    med = sum(1 for a in anomalies if a["severity"] == "MEDIUM")
    low = sum(1 for a in anomalies if a["severity"] == "LOW")
    print(f"  HIGH: {high}, MEDIUM: {med}, LOW: {low}")
    return anomalies


def supervisor_orchestrate(spark, w, golden_records, compliance_results, anomalies):
    """Supervisor: Combine agent results, decide auto-approve vs escalate."""
    print("\n=== SUPERVISOR AGENT ===")

    # Build lookup maps
    compliance_map = {r["supplier_id"]: r for r in compliance_results}
    anomaly_map = {a["supplier_id"]: a for a in anomalies}
    merged_ids = set()
    for gr in golden_records:
        for mid in gr.get("merged_from", []):
            merged_ids.add(mid)

    suppliers = spark.sql(f"""
        SELECT * FROM {CATALOG}.{SCHEMA}.suppliers_cleaned
    """).collect()

    approved = []
    escalated = []

    for sup in suppliers:
        s = sup.asDict()
        sid = s["supplier_id"]

        # Skip if merged into a golden record
        if sid in merged_ids:
            continue

        comp = compliance_map.get(sid, {})
        anom = anomaly_map.get(sid, {})

        is_compliant = comp.get("compliant", True)
        risk = comp.get("risk_level", "LOW")
        anomaly_count = anom.get("anomaly_count", 0)
        severity = anom.get("severity", "LOW")

        # Decision logic
        if is_compliant and anomaly_count == 0:
            s["status"] = "AUTO_APPROVED"
            s["risk_level"] = "LOW"
            s["issues"] = []
            approved.append(s)
        elif risk == "HIGH" or severity == "HIGH" or anomaly_count >= 3:
            s["status"] = "ESCALATED"
            s["risk_level"] = risk
            s["issues"] = comp.get("issues", []) + anom.get("anomaly_flags", [])
            s["recommendation"] = comp.get("recommendation", "Review required")
            escalated.append(s)
        elif not is_compliant or anomaly_count > 0:
            s["status"] = "ESCALATED"
            s["risk_level"] = risk if risk != "LOW" else "MEDIUM"
            s["issues"] = comp.get("issues", []) + anom.get("anomaly_flags", [])
            s["recommendation"] = comp.get("recommendation", "Review required")
            escalated.append(s)
        else:
            s["status"] = "AUTO_APPROVED"
            s["risk_level"] = "LOW"
            s["issues"] = []
            approved.append(s)

    # Add golden records as approved
    for gr in golden_records:
        gr["status"] = "AUTO_APPROVED"
        gr["risk_level"] = "LOW"
        gr["issues"] = []
        approved.append(gr)

    total = len(approved) + len(escalated)
    print(f"\nSupervisor Decision Summary:")
    print(f"  Total processed: {total}")
    print(f"  Auto-approved: {len(approved)} ({len(approved)/max(total,1)*100:.0f}%)")
    print(f"  Escalated: {len(escalated)} ({len(escalated)/max(total,1)*100:.0f}%)")

    # Write Gold table (approved suppliers)
    write_gold_table(spark, approved)

    # Write escalation queue
    write_escalation_queue(spark, escalated)

    return approved, escalated


def write_gold_table(spark, records):
    """Write validated supplier records to Gold table."""
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

    if not records:
        print("No records to write to Gold table")
        return

    schema = StructType([
        StructField("supplier_id", StringType()),
        StructField("supplier_name", StringType()),
        StructField("category", StringType()),
        StructField("prefecture", StringType()),
        StructField("city", StringType()),
        StructField("phone", StringType()),
        StructField("contact_person", StringType()),
        StructField("certification", StringType()),
        StructField("unit_price", DoubleType()),
        StructField("currency", StringType()),
        StructField("lead_time_days", IntegerType()),
        StructField("reliability_score", DoubleType()),
        StructField("status", StringType()),
        StructField("risk_level", StringType()),
    ])

    rows = []
    for r in records:
        rows.append((
            str(r.get("supplier_id", "")),
            str(r.get("supplier_name", "")),
            str(r.get("standardized_category", r.get("category", ""))),
            str(r.get("prefecture_en", r.get("prefecture", ""))),
            str(r.get("city", "")),
            str(r.get("phone") or ""),
            str(r.get("contact_person") or ""),
            str(r.get("certification") or ""),
            float(r.get("unit_price", 0) or 0),
            str(r.get("currency_standardized", r.get("currency", "JPY"))),
            int(r.get("lead_time_days", 0) or 0),
            float(r.get("reliability_score", 0) or 0),
            str(r.get("status", "")),
            str(r.get("risk_level", "")),
        ))

    df = spark.createDataFrame(rows, schema=schema)
    table = f"{CATALOG}.{SCHEMA}.gold_suppliers"
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)
    print(f"Wrote {len(rows)} records to {table}")


def write_escalation_queue(spark, records):
    """Write escalated records to the review queue."""
    from pyspark.sql.types import StructType, StructField, StringType

    if not records:
        print("No records to escalate")
        return

    schema = StructType([
        StructField("supplier_id", StringType()),
        StructField("supplier_name", StringType()),
        StructField("category", StringType()),
        StructField("risk_level", StringType()),
        StructField("issues", StringType()),
        StructField("recommendation", StringType()),
        StructField("review_status", StringType()),
        StructField("reviewer", StringType()),
    ])

    rows = []
    for r in records:
        rows.append((
            str(r.get("supplier_id", "")),
            str(r.get("supplier_name", "")),
            str(r.get("standardized_category", r.get("category", ""))),
            str(r.get("risk_level", "")),
            json.dumps(r.get("issues", []), ensure_ascii=False),
            str(r.get("recommendation", "")),
            "PENDING",
            "",
        ))

    df = spark.createDataFrame(rows, schema=schema)
    table = f"{CATALOG}.{SCHEMA}.escalation_queue"
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)
    print(f"Wrote {len(rows)} records to {table}")


def main():
    print("=" * 60)
    print("PERNOD RICARD JAPAN - SUPPLIER PROCESSING PIPELINE")
    print("=" * 60)

    spark = get_spark()
    w = get_workspace_client()
    print(f"Spark version: {spark.version}")

    # Run all three agents
    golden_records = run_deduplication_agent(spark, w)
    compliance_results = run_compliance_agent(spark, w)
    anomalies = run_anomaly_agent(spark, w)

    # Supervisor orchestrates and writes results
    approved, escalated = supervisor_orchestrate(
        spark, w, golden_records, compliance_results, anomalies
    )

    print("\n" + "=" * 60)
    print("PIPELINE COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()
