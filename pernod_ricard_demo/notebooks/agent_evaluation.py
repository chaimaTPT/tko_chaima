# Databricks notebook source
# MAGIC %md
# MAGIC # Supplier Intelligence Agent — Tracing, Evaluation & Monitoring
# MAGIC
# MAGIC This notebook enables **MLflow tracing** on the custom agent endpoint and sets up **evaluation with expectations** and **production monitoring**.
# MAGIC
# MAGIC **What we set up:**
# MAGIC 1. **Tracing** — link UC schema to experiment, enable trace ingestion from the serving endpoint
# MAGIC 2. **Evaluation dataset** — 15 curated test cases with `expected_facts` and `expected_response`
# MAGIC 3. **Scorers** — 9 scorers (5 built-in + 4 custom domain-specific)
# MAGIC 4. **Run evaluation** — test the agent and analyze per-row results
# MAGIC 5. **Production monitoring** — 4 live scorers sampling agent traces continuously

# COMMAND ----------

# MAGIC %pip install --upgrade "mlflow[databricks]>=3.9.0" openai databricks-langchain langgraph
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Configuration

# COMMAND ----------

import os
import mlflow
from mlflow.entities import UCSchemaLocation

# Configuration
CATALOG = "opm_catalog"
SCHEMA = "supplier_hub"
SQL_WAREHOUSE_ID = "6457618d1c009dd6"
AGENT_ENDPOINT = "supplier-intelligence-agent"
EXPERIMENT_NAME = "/Users/chaima.berrachdi@databricks.com/pernod_ricard_demo/supplier-agent-evaluation"

mlflow.set_tracking_uri("databricks")
os.environ["MLFLOW_TRACING_SQL_WAREHOUSE_ID"] = SQL_WAREHOUSE_ID

print(f"Catalog: {CATALOG}")
print(f"Schema: {SCHEMA}")
print(f"Agent: {AGENT_ENDPOINT}")
print(f"Warehouse: {SQL_WAREHOUSE_ID}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Enable Tracing — Link UC Schema to Experiment
# MAGIC
# MAGIC This stores all agent traces (from the serving endpoint and from evaluation runs) in Unity Catalog Delta tables.
# MAGIC Three tables are created automatically:
# MAGIC - `mlflow_experiment_trace_otel_spans`
# MAGIC - `mlflow_experiment_trace_otel_logs`
# MAGIC - `mlflow_experiment_trace_otel_metrics`

# COMMAND ----------

from mlflow.tracing.enablement import set_experiment_trace_location

# Create or retrieve experiment
if experiment := mlflow.get_experiment_by_name(EXPERIMENT_NAME):
    experiment_id = experiment.experiment_id
    print(f"Using existing experiment: {EXPERIMENT_NAME} (ID: {experiment_id})")
else:
    experiment_id = mlflow.create_experiment(name=EXPERIMENT_NAME)
    print(f"Created experiment: {EXPERIMENT_NAME} (ID: {experiment_id})")

# Link UC schema to experiment — traces go to Delta tables
set_experiment_trace_location(
    location=UCSchemaLocation(
        catalog_name=CATALOG,
        schema_name=SCHEMA,
    ),
    experiment_id=experiment_id,
)

print(f"Traces linked to: {CATALOG}.{SCHEMA}")
print(f"Tables: {CATALOG}.{SCHEMA}.mlflow_experiment_trace_otel_spans / _logs / _metrics")

# COMMAND ----------

# Set trace destination so new traces from this notebook also go to UC
mlflow.tracing.set_destination(
    destination=UCSchemaLocation(
        catalog_name=CATALOG,
        schema_name=SCHEMA,
    )
)

mlflow.set_experiment(EXPERIMENT_NAME)
mlflow.langchain.autolog()

print("Trace destination set. All traces will be stored in UC.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configure the Serving Endpoint for Tracing
# MAGIC
# MAGIC Set `MLFLOW_TRACING_DESTINATION` on the serving endpoint so production traces are captured.

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Enable tracing on the serving endpoint by setting the environment variable
try:
    endpoint = w.serving_endpoints.get(AGENT_ENDPOINT)
    current_entities = endpoint.config.served_entities

    if current_entities:
        entity = current_entities[0]
        env_vars = entity.environment_vars or {}
        env_vars["MLFLOW_TRACING_DESTINATION"] = f"{CATALOG}.{SCHEMA}"

        from databricks.sdk.service.serving import ServedEntityInput
        w.serving_endpoints.update_config_and_wait(
            name=AGENT_ENDPOINT,
            served_entities=[
                ServedEntityInput(
                    entity_name=entity.entity_name,
                    entity_version=entity.entity_version,
                    scale_to_zero_enabled=entity.scale_to_zero_enabled,
                    workload_size="Small",
                    environment_vars=env_vars,
                )
            ],
        )
        print(f"Tracing enabled on endpoint '{AGENT_ENDPOINT}'")
        print(f"  MLFLOW_TRACING_DESTINATION = {CATALOG}.{SCHEMA}")
    else:
        print("WARNING: No served entities found on endpoint.")
except Exception as e:
    print(f"Note: Could not update endpoint config: {e}")
    print("You can manually set MLFLOW_TRACING_DESTINATION in the endpoint environment variables.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Grant Permissions on Trace Tables
# MAGIC
# MAGIC The serving endpoint's service principal needs MODIFY + SELECT on the trace tables.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Grant permissions on trace tables to all users (adjust as needed)
# MAGIC GRANT MODIFY, SELECT ON TABLE opm_catalog.supplier_hub.mlflow_experiment_trace_otel_spans TO `account users`;
# MAGIC GRANT MODIFY, SELECT ON TABLE opm_catalog.supplier_hub.mlflow_experiment_trace_otel_logs TO `account users`;
# MAGIC GRANT MODIFY, SELECT ON TABLE opm_catalog.supplier_hub.mlflow_experiment_trace_otel_metrics TO `account users`;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Evaluation Dataset with Expectations
# MAGIC
# MAGIC 15 test cases across 3 categories, each with `expected_facts` for the `Correctness` scorer:
# MAGIC - **Policy questions** (5) — should use Vector Search RAG
# MAGIC - **Data questions** (5) — should call UC Function tools
# MAGIC - **Combined questions** (5) — should use both RAG + tools

# COMMAND ----------

eval_data = [
    # ── Policy questions (should use Vector Search) ──────────────────
    {
        "inputs": {"query": "What certifications are mandatory for food-contact suppliers?"},
        "expectations": {
            "expected_facts": [
                "ISO 22000 or FSSC 22000 or HACCP is mandatory for food-contact suppliers",
                "ISO 9001 is also recommended",
            ],
            "expected_response": "Food-contact suppliers must hold ISO 22000, FSSC 22000, or HACCP certification. ISO 9001 is additionally recommended.",
        },
    },
    {
        "inputs": {"query": "What are the acceptable price ranges for glass bottles?"},
        "expectations": {
            "expected_facts": [
                "Minimum unit price for glass bottles is 80 JPY",
                "Maximum unit price for glass bottles is 5000 JPY",
            ],
        },
    },
    {
        "inputs": {"query": "What is the audit frequency for Tier 1 suppliers?"},
        "expectations": {
            "expected_facts": [
                "Tier 1 suppliers require annual on-site audits",
            ],
            "expected_response": "Tier 1 suppliers must undergo annual on-site audits as per the supplier qualification policy.",
        },
    },
    {
        "inputs": {"query": "What are the Japan-specific compliance requirements for beverage suppliers?"},
        "expectations": {
            "expected_facts": [
                "Food Safety Act (食品衛生法)",
                "Liquor Tax Act (酒税法)",
                "Container and Packaging Recycling Act",
            ],
        },
    },
    {
        "inputs": {"query": "What is the maximum acceptable lead time for packaging suppliers?"},
        "expectations": {
            "expected_facts": [
                "Maximum lead time for packaging is 30 days",
            ],
        },
    },
    # ── Data questions (should call UC Function tools) ───────────────
    {
        "inputs": {"query": "Give me a full overview of supplier SUP-0001"},
        "expectations": {
            "expected_facts": [
                "SUP-0001 supplier details including name and category",
                "Delivery metrics or reliability score",
            ],
        },
    },
    {
        "inputs": {"query": "What is our total spend by category?"},
        "expectations": {
            "expected_facts": [
                "Spend breakdown includes Glass Bottles category",
                "Spend breakdown includes Packaging category",
                "Numerical spend amounts are provided",
            ],
        },
    },
    {
        "inputs": {"query": "What is our risk exposure across the supplier portfolio?"},
        "expectations": {
            "expected_facts": [
                "Risk summary includes HIGH risk count",
                "Risk summary includes MEDIUM risk count",
                "Risk summary includes LOW risk count",
            ],
        },
    },
    {
        "inputs": {"query": "Validate the delivery quality for supplier SUP-0010"},
        "expectations": {
            "expected_facts": [
                "Delivery quality assessment for SUP-0010",
                "On-time delivery rate or delivery metrics",
            ],
        },
    },
    {
        "inputs": {"query": "List the non-compliant suppliers with the highest risk"},
        "expectations": {
            "expected_facts": [
                "List of non-compliant or escalated suppliers",
                "Risk levels or specific issues mentioned",
            ],
        },
    },
    # ── Combined questions (should use both RAG + tools) ─────────────
    {
        "inputs": {"query": "What certifications are required and how many suppliers are currently missing them?"},
        "expectations": {
            "expected_facts": [
                "ISO 22000 or FSSC 22000 or HACCP certification requirements from policy",
                "Count or list of suppliers missing required certifications from data",
            ],
        },
    },
    {
        "inputs": {"query": "According to policy, what lead time is acceptable for glass bottles? And validate the delivery quality for SUP-0010."},
        "expectations": {
            "expected_facts": [
                "Lead time policy from procurement documents",
                "Delivery quality data for SUP-0010",
            ],
        },
    },
    {
        "inputs": {"query": "What is our spend on glass bottles and is it within the acceptable price range per policy?"},
        "expectations": {
            "expected_facts": [
                "Glass bottles spend amount from data",
                "Price range limits from procurement policy",
            ],
        },
    },
    {
        "inputs": {"query": "Which suppliers have high risk and what audit requirements do they need to meet?"},
        "expectations": {
            "expected_facts": [
                "High-risk suppliers from escalation queue",
                "Audit requirements from procurement policy",
            ],
        },
    },
    {
        "inputs": {"query": "サプライヤーSUP-0001の概要を教えてください"},
        "expectations": {
            "expected_facts": [
                "SUP-0001 supplier information",
                "Response addresses the Japanese language query",
            ],
        },
    },
]

print(f"Evaluation dataset: {len(eval_data)} test cases")
print(f"  Policy questions: 5")
print(f"  Data questions: 5")
print(f"  Combined questions: 5 (including 1 Japanese)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Custom Scorers

# COMMAND ----------

from mlflow.genai.scorers import (
    Guidelines,
    Safety,
    RelevanceToQuery,
    Correctness,
    scorer,
    ScorerSamplingConfig,
)
from mlflow.genai.judges import meets_guidelines
from mlflow.entities import Feedback, Trace, SpanType


@scorer
def mentions_sources(inputs, outputs, trace):
    """Check if the agent cites policy documents or data sources for policy questions."""
    response = str(outputs) if outputs else ""
    query = str(inputs.get("query", "")) if inputs else ""

    policy_keywords = ["certification", "compliance", "policy", "audit", "lead time", "price range", "acceptable"]
    is_policy_question = any(kw in query.lower() for kw in policy_keywords)

    if not is_policy_question:
        return Feedback(name="mentions_sources", value=True, rationale="Not a policy question — source citation not required.")

    source_indicators = [
        "supplier_policy", "compliance_checklist", "scorecard",
        "according to", "per policy", "policy states", "based on",
        "document", "procurement", "guideline", "regulation", "requirement"
    ]
    has_source = any(ind in response.lower() for ind in source_indicators)

    return Feedback(
        name="mentions_sources",
        value=has_source,
        rationale=f"Policy question {'cites sources' if has_source else 'MISSING source citation'}.",
    )


@scorer
def tool_usage_check(inputs, outputs, trace):
    """Verify the agent called UC Function tools when the question requires live data."""
    if trace is None:
        return Feedback(name="tool_usage", value=True, rationale="No trace available.")

    query = str(inputs.get("query", "")) if inputs else ""
    tool_spans = trace.search_spans(span_type=SpanType.TOOL)
    tool_names = [s.name for s in tool_spans] if tool_spans else []

    data_keywords = [
        "sup-0", "spend by category", "risk exposure", "delivery quality",
        "overview of supplier", "non-compliant", "validate", "list",
        "概要", "サプライヤー"
    ]
    needs_data = any(kw.lower() in query.lower() for kw in data_keywords)

    if needs_data and not tool_names:
        return Feedback(
            name="tool_usage",
            value=False,
            rationale=f"Data question but no UC Function tools were called.",
        )

    return Feedback(
        name="tool_usage",
        value=True,
        rationale=f"Tools called: {tool_names}" if tool_names else "No tools needed.",
    )


@scorer
def response_completeness(inputs, outputs):
    """Check the response is substantive (not too short, not empty)."""
    response = str(outputs) if outputs else ""
    word_count = len(response.split())

    if word_count < 15:
        return Feedback(name="response_completeness", value=False, rationale=f"Too short: {word_count} words.")
    if word_count > 1000:
        return Feedback(name="response_completeness", value=False, rationale=f"Excessively long: {word_count} words.")
    return Feedback(name="response_completeness", value=True, rationale=f"Good length: {word_count} words.")


@scorer
def vector_search_usage(inputs, outputs, trace):
    """Check if Vector Search was used for policy-related questions."""
    if trace is None:
        return Feedback(name="vector_search_usage", value=True, rationale="No trace available.")

    query = str(inputs.get("query", "")) if inputs else ""
    policy_keywords = ["certification", "compliance", "policy", "audit", "lead time", "price range", "acceptable", "requirement", "Japan"]
    is_policy_question = any(kw in query.lower() for kw in policy_keywords)

    if not is_policy_question:
        return Feedback(name="vector_search_usage", value=True, rationale="Not a policy question — vector search not expected.")

    # Look for retriever spans (Vector Search)
    retriever_spans = trace.search_spans(span_type=SpanType.RETRIEVER)
    tool_spans = trace.search_spans(span_type=SpanType.TOOL)
    vs_tool_used = any("procurement_policy_search" in (s.name or "") for s in (tool_spans or []))

    has_vs = bool(retriever_spans) or vs_tool_used

    return Feedback(
        name="vector_search_usage",
        value=has_vs,
        rationale=f"Policy question: Vector Search {'was used' if has_vs else 'was NOT used'}.",
    )


print("Custom scorers defined: mentions_sources, tool_usage_check, response_completeness, vector_search_usage")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Predict Function
# MAGIC
# MAGIC Calls the deployed `supplier-intelligence-agent` endpoint.

# COMMAND ----------

client = w.serving_endpoints.get_open_ai_client()


def predict_fn(query):
    """Call the supplier intelligence agent endpoint."""
    response = client.chat.completions.create(
        model=AGENT_ENDPOINT,
        messages=[{"role": "user", "content": query}],
    )
    return response.choices[0].message.content


# Quick test
test_response = predict_fn("How many suppliers do we have?")
print(f"Agent test: {test_response[:300]}...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Run Evaluation

# COMMAND ----------

all_scorers = [
    # Built-in
    Safety(),
    RelevanceToQuery(),
    Correctness(),  # uses expected_facts / expected_response from expectations
    Guidelines(
        name="procurement_domain",
        guidelines=[
            "The response must be relevant to procurement, supplier management, or supply chain",
            "The response must provide specific, actionable information with real data when available",
            "If discussing supplier data, the response should reference actual supplier IDs, categories, or metrics",
        ],
    ),
    Guidelines(
        name="professional_tone",
        guidelines="The response must maintain a professional, business-appropriate tone suitable for procurement managers",
    ),
    # Custom domain scorers
    mentions_sources,
    tool_usage_check,
    response_completeness,
    vector_search_usage,
]

with mlflow.start_run(run_name="supplier_agent_v1_full_eval"):
    results = mlflow.genai.evaluate(
        data=eval_data,
        predict_fn=predict_fn,
        scorers=all_scorers,
    )

print(f"\nRun ID: {results.run_id}")
print(f"\n{'Scorer':<30} {'Pass Rate':>10}")
print("-" * 42)
for metric, value in sorted(results.metrics.items()):
    if metric.endswith("/mean"):
        name = metric.replace("/mean", "")
        print(f"  {name:<28} {value:>8.1%}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Analyze Results — Per-Row Breakdown

# COMMAND ----------

traces_df = mlflow.search_traces(run_id=results.run_id)

print(f"Total test cases: {len(traces_df)}\n")

pass_count = 0
fail_count = 0

for idx, row in traces_df.iterrows():
    query = str(row.get("request", ""))[:70]
    assessments = row.get("assessments", [])
    failures = [
        a["assessment_name"]
        for a in assessments
        if a.get("feedback", {}).get("value") in ["no", False]
    ]

    if failures:
        fail_count += 1
        print(f"  FAIL  {query}...")
        for f in failures:
            # Find rationale
            rationale = next(
                (a.get("rationale", "") for a in assessments if a["assessment_name"] == f),
                "",
            )
            print(f"        - {f}: {rationale[:120]}")
    else:
        pass_count += 1
        print(f"  PASS  {query}...")

print(f"\nSummary: {pass_count} passed, {fail_count} failed out of {len(traces_df)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Production Monitoring — Register & Start Scorers
# MAGIC
# MAGIC These scorers run continuously on sampled traces from the serving endpoint.
# MAGIC Results appear in the Experiments UI > Traces tab.

# COMMAND ----------

from mlflow.tracing import set_databricks_monitoring_sql_warehouse_id

set_databricks_monitoring_sql_warehouse_id(
    warehouse_id=SQL_WAREHOUSE_ID,
    experiment_id=experiment_id,
)
print(f"Monitoring warehouse: {SQL_WAREHOUSE_ID}")

# COMMAND ----------

# Safety — 100% of traces
safety_mon = Safety().register(name="prod_safety")
safety_mon = safety_mon.start(sampling_config=ScorerSamplingConfig(sample_rate=1.0))
print("prod_safety:              ACTIVE @ 100%")

# Relevance — 100% of traces
relevance_mon = RelevanceToQuery().register(name="prod_relevance")
relevance_mon = relevance_mon.start(sampling_config=ScorerSamplingConfig(sample_rate=1.0))
print("prod_relevance:           ACTIVE @ 100%")

# Correctness — 100% of traces (requires expectations in trace metadata)
correctness_mon = Correctness().register(name="prod_correctness")
correctness_mon = correctness_mon.start(sampling_config=ScorerSamplingConfig(sample_rate=1.0))
print("prod_correctness:         ACTIVE @ 100%")

# Procurement domain — 50% of traces
domain_mon = Guidelines(
    name="prod_procurement_domain",
    guidelines=[
        "The response must be relevant to procurement, supplier management, or supply chain",
        "The response must provide specific information rather than generic advice",
    ],
).register(name="prod_procurement_domain")
domain_mon = domain_mon.start(sampling_config=ScorerSamplingConfig(sample_rate=0.5))
print("prod_procurement_domain:  ACTIVE @ 50%")

# Professional tone — 50% of traces
tone_mon = Guidelines(
    name="prod_professional_tone",
    guidelines="The response must maintain a professional, business-appropriate tone",
).register(name="prod_professional_tone")
tone_mon = tone_mon.start(sampling_config=ScorerSamplingConfig(sample_rate=0.5))
print("prod_professional_tone:   ACTIVE @ 50%")

print("\nAll 5 production monitors active.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Verify Monitoring Status

# COMMAND ----------

from mlflow.genai.scorers import list_scorers

registered = list_scorers()
print(f"Registered production scorers: {len(registered)}\n")
print(f"{'Name':<30} {'Sample Rate':>12}")
print("-" * 44)
for s in registered:
    rate = f"{s.sampling_config.sample_rate:.0%}" if s.sampling_config else "N/A"
    print(f"  {s.name:<28} {rate:>10}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Query Traces from Unity Catalog
# MAGIC
# MAGIC Once traces are flowing, you can query them with SQL for custom dashboards and analysis.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Recent traces by duration
# MAGIC SELECT
# MAGIC   trace_id,
# MAGIC   name AS root_span,
# MAGIC   ROUND((end_time_unix_nano - start_time_unix_nano) / 1e9, 2) AS duration_sec,
# MAGIC   status_code,
# MAGIC   timestamp
# MAGIC FROM opm_catalog.supplier_hub.mlflow_experiment_trace_otel_spans
# MAGIC WHERE parent_span_id IS NULL
# MAGIC ORDER BY timestamp DESC
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Error rate by span name
# MAGIC SELECT
# MAGIC   name,
# MAGIC   COUNT(*) AS total,
# MAGIC   SUM(CASE WHEN status_code = 'ERROR' THEN 1 ELSE 0 END) AS errors,
# MAGIC   ROUND(SUM(CASE WHEN status_code = 'ERROR' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS error_pct
# MAGIC FROM opm_catalog.supplier_hub.mlflow_experiment_trace_otel_spans
# MAGIC GROUP BY name
# MAGIC HAVING COUNT(*) >= 3
# MAGIC ORDER BY error_pct DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC ### Tracing
# MAGIC - UC schema `opm_catalog.supplier_hub` linked to experiment
# MAGIC - Serving endpoint `supplier-intelligence-agent` configured with `MLFLOW_TRACING_DESTINATION`
# MAGIC - All traces stored in Delta tables for SQL analysis
# MAGIC
# MAGIC ### Evaluation
# MAGIC - **15 test cases** with `expected_facts` and `expected_response` across policy, data, and combined queries
# MAGIC - **9 scorers**: Safety, RelevanceToQuery, Correctness, 2x Guidelines, mentions_sources, tool_usage_check, response_completeness, vector_search_usage
# MAGIC - Correctness scorer validates agent output against ground truth expectations
# MAGIC
# MAGIC ### Production Monitoring
# MAGIC - **5 active monitors**: prod_safety (100%), prod_relevance (100%), prod_correctness (100%), prod_procurement_domain (50%), prod_professional_tone (50%)
# MAGIC - Results visible in **Experiments UI > Traces tab**
# MAGIC
# MAGIC ### Next Steps
# MAGIC - Run this notebook to activate everything
# MAGIC - View results at: Experiments > `supplier-agent-evaluation`
# MAGIC - Query traces via SQL: `SELECT * FROM opm_catalog.supplier_hub.mlflow_experiment_trace_otel_spans`
# MAGIC - Use `mlflow.genai.optimize_prompts()` with an aligned judge to auto-improve the system prompt
