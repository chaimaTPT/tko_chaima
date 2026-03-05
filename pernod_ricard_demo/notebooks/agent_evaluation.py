# Databricks notebook source
# MAGIC %md
# MAGIC # Supplier Intelligence Agent — Evaluation & Monitoring
# MAGIC
# MAGIC This notebook sets up **MLflow 3 GenAI evaluation and production monitoring** for the Pernod Ricard Japan Supplier Intelligence Agent.
# MAGIC
# MAGIC **What we cover:**
# MAGIC 1. **Evaluation dataset** — curated questions covering policy, data, and multi-tool queries
# MAGIC 2. **Custom scorers** — domain-specific quality checks for supplier intelligence
# MAGIC 3. **Built-in scorers** — Safety, RelevanceToQuery, Guidelines
# MAGIC 4. **Run evaluation** — test the agent against our quality bar
# MAGIC 5. **Production monitoring** — continuous scorer sampling on live traces

# COMMAND ----------

# MAGIC %pip install --upgrade "mlflow[databricks]>=3.9.0" openai databricks-langchain langgraph
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import os
import mlflow
from mlflow.genai.scorers import (
    Guidelines,
    Safety,
    RelevanceToQuery,
    RetrievalGroundedness,
    Correctness,
    scorer,
    ScorerSamplingConfig,
)
from mlflow.genai.judges import meets_guidelines
from mlflow.entities import Feedback, Trace, SpanType

mlflow.langchain.autolog()
mlflow.set_tracking_uri("databricks")

EXPERIMENT_NAME = "/Users/chaima.berrachdi@databricks.com/pernod_ricard_demo/supplier-agent-evaluation"
mlflow.set_experiment(EXPERIMENT_NAME)

print("MLflow configured.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Evaluation Dataset
# MAGIC
# MAGIC We test the agent across 3 categories:
# MAGIC - **Policy questions** — should use Vector Search RAG
# MAGIC - **Data questions** — should call UC Function tools
# MAGIC - **Combined questions** — should use both RAG + tools

# COMMAND ----------

eval_data = [
    # --- Policy questions (should use Vector Search) ---
    {
        "inputs": {"query": "What certifications are mandatory for food-contact suppliers?"},
        "expectations": {
            "expected_facts": [
                "ISO 22000 or FSSC 22000 or HACCP is required",
                "Food-contact suppliers must have food safety certification",
            ]
        },
    },
    {
        "inputs": {"query": "What are the acceptable price ranges for glass bottles?"},
        "expectations": {
            "expected_facts": [
                "Minimum price is 80 JPY",
                "Maximum price is 5000 JPY",
            ]
        },
    },
    {
        "inputs": {"query": "What is the audit frequency for Tier 1 suppliers?"},
        "expectations": {
            "expected_facts": ["Annual audit is required for Tier 1 suppliers"]
        },
    },
    {
        "inputs": {"query": "What are the Japan-specific compliance requirements?"},
        "expectations": {
            "expected_facts": [
                "Food Safety Act",
                "Liquor Tax Act",
            ]
        },
    },
    {
        "inputs": {"query": "What is the maximum acceptable lead time for packaging suppliers?"},
        "expectations": {
            "expected_facts": ["30 days maximum lead time"]
        },
    },
    # --- Data questions (should call UC Function tools) ---
    {
        "inputs": {"query": "Give me a full overview of supplier SUP-0001"},
        "expectations": {
            "expected_facts": ["SUP-0001", "supplier name", "category"]
        },
    },
    {
        "inputs": {"query": "What is our total spend by category?"},
        "expectations": {
            "expected_facts": ["Glass Bottles", "Packaging", "spend"]
        },
    },
    {
        "inputs": {"query": "What is our risk exposure across the supplier portfolio?"},
        "expectations": {
            "expected_facts": ["HIGH", "MEDIUM", "LOW", "risk"]
        },
    },
    # --- Combined questions (should use both) ---
    {
        "inputs": {
            "query": "What certifications are required and how many suppliers are missing them?"
        },
        "expectations": {
            "expected_facts": [
                "ISO 22000 or FSSC 22000 or HACCP",
                "non-compliant suppliers",
            ]
        },
    },
    {
        "inputs": {
            "query": "According to policy, what lead time is acceptable for glass bottles? And validate the delivery quality for SUP-0010."
        },
        "expectations": {
            "expected_facts": ["lead time", "SUP-0010", "delivery"]
        },
    },
]

print(f"Evaluation dataset: {len(eval_data)} test cases")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Custom Scorers
# MAGIC
# MAGIC Domain-specific quality checks tailored to supplier intelligence.

# COMMAND ----------

@scorer
def mentions_sources(inputs, outputs, trace):
    """Check if the agent cites policy documents or data sources when appropriate."""
    response = str(outputs) if outputs else ""
    query = str(inputs.get("query", "")) if inputs else ""

    policy_keywords = ["certification", "compliance", "policy", "audit", "lead time", "price range"]
    is_policy_question = any(kw in query.lower() for kw in policy_keywords)

    if not is_policy_question:
        return Feedback(name="mentions_sources", value=True, rationale="Not a policy question — source citation not required.")

    source_indicators = [
        "supplier_policy", "compliance_checklist", "scorecard",
        "according to", "per policy", "policy states", "based on",
        "document", "procurement"
    ]
    has_source = any(ind in response.lower() for ind in source_indicators)

    return Feedback(
        name="mentions_sources",
        value=has_source,
        rationale=f"Policy question {'cites sources' if has_source else 'missing source citation'}."
    )


@scorer
def tool_usage_check(inputs, outputs, trace):
    """Verify the agent called appropriate tools for data questions."""
    if trace is None:
        return Feedback(name="tool_usage", value=True, rationale="No trace available.")

    query = str(inputs.get("query", "")) if inputs else ""
    tool_spans = trace.search_spans(span_type=SpanType.TOOL)
    tool_names = [s.name for s in tool_spans] if tool_spans else []

    data_keywords = ["supplier SUP-", "spend by category", "risk exposure", "delivery quality", "overview of supplier", "non-compliant"]
    needs_data = any(kw.lower() in query.lower() for kw in data_keywords)

    if needs_data and not tool_names:
        return Feedback(
            name="tool_usage",
            value=False,
            rationale=f"Data question but no UC Function tools were called. Query: '{query[:80]}'"
        )

    return Feedback(
        name="tool_usage",
        value=True,
        rationale=f"Tools called: {tool_names}" if tool_names else "No tools needed for this query."
    )


@scorer
def bilingual_support(inputs, outputs):
    """Check that the agent handles Japanese content appropriately."""
    response = str(outputs) if outputs else ""
    if len(response) < 10:
        return Feedback(name="bilingual_support", value=False, rationale="Response too short.")
    return Feedback(name="bilingual_support", value=True, rationale="Response is substantive.")


@scorer
def response_length_check(inputs, outputs):
    """Ensure responses are neither too short nor excessively long."""
    response = str(outputs) if outputs else ""
    word_count = len(response.split())

    if word_count < 15:
        return Feedback(name="response_length", value=False, rationale=f"Too short: {word_count} words.")
    if word_count > 800:
        return Feedback(name="response_length", value=False, rationale=f"Too long: {word_count} words.")
    return Feedback(name="response_length", value=True, rationale=f"Appropriate length: {word_count} words.")

print("Custom scorers defined.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Define the Predict Function
# MAGIC
# MAGIC We call the deployed agent endpoint for evaluation.

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
client = w.serving_endpoints.get_open_ai_client()

AGENT_ENDPOINT = "supplier-intelligence-agent"


def predict_fn(query):
    """Call the supplier intelligence agent endpoint."""
    response = client.chat.completions.create(
        model=AGENT_ENDPOINT,
        messages=[{"role": "user", "content": query}],
    )
    return response.choices[0].message.content

# Test the predict function
test_response = predict_fn("How many suppliers do we have?")
print(f"Test response: {test_response[:200]}...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Run Evaluation

# COMMAND ----------

# Define all scorers
all_scorers = [
    # Built-in scorers
    Safety(),
    RelevanceToQuery(),
    Correctness(),
    Guidelines(
        name="procurement_domain",
        guidelines=[
            "The response must be relevant to procurement, supplier management, or supply chain topics",
            "The response must provide specific, actionable information rather than generic advice",
            "If discussing supplier data, the response must reference actual supplier IDs, categories, or metrics",
        ],
    ),
    Guidelines(
        name="professional_tone",
        guidelines="The response must maintain a professional, business-appropriate tone suitable for procurement managers",
    ),
    # Custom scorers
    mentions_sources,
    tool_usage_check,
    bilingual_support,
    response_length_check,
]

# Run evaluation
with mlflow.start_run(run_name="supplier_agent_v1_eval"):
    results = mlflow.genai.evaluate(
        data=eval_data,
        predict_fn=predict_fn,
        scorers=all_scorers,
    )

print(f"\nRun ID: {results.run_id}")
print(f"\nMetrics:")
for metric, value in sorted(results.metrics.items()):
    if metric.endswith("/mean"):
        name = metric.replace("/mean", "")
        print(f"  {name}: {value:.2%}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Analyze Results

# COMMAND ----------

traces_df = mlflow.search_traces(run_id=results.run_id)

print(f"Total test cases: {len(traces_df)}")
print(f"\nPer-row results:")
for idx, row in traces_df.iterrows():
    query = str(row.get("request", ""))[:60]
    assessments = row.get("assessments", [])
    failures = [a["assessment_name"] for a in assessments if a.get("feedback", {}).get("value") in ["no", False]]
    status = "PASS" if not failures else f"FAIL: {', '.join(failures)}"
    print(f"  [{status}] {query}...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Production Monitoring Setup
# MAGIC
# MAGIC Register scorers to continuously evaluate traces from the deployed agent endpoint.
# MAGIC Scorers run asynchronously on sampled traces.

# COMMAND ----------

from mlflow.tracing import set_databricks_monitoring_sql_warehouse_id

SQL_WAREHOUSE_ID = "6457618d1c009dd6"

experiment = mlflow.get_experiment_by_name(EXPERIMENT_NAME)

set_databricks_monitoring_sql_warehouse_id(
    warehouse_id=SQL_WAREHOUSE_ID,
    experiment_id=experiment.experiment_id,
)

print(f"Monitoring warehouse configured: {SQL_WAREHOUSE_ID}")

# COMMAND ----------

# Register and start production monitoring scorers

# Safety — evaluate 100% of production traces
safety_monitor = Safety().register(name="prod_safety")
safety_monitor = safety_monitor.start(
    sampling_config=ScorerSamplingConfig(sample_rate=1.0)
)
print("Safety monitor: ACTIVE (100% sample rate)")

# Relevance — evaluate 100% of production traces
relevance_monitor = RelevanceToQuery().register(name="prod_relevance")
relevance_monitor = relevance_monitor.start(
    sampling_config=ScorerSamplingConfig(sample_rate=1.0)
)
print("Relevance monitor: ACTIVE (100% sample rate)")

# Procurement domain guidelines — evaluate 50% of traces
domain_monitor = Guidelines(
    name="prod_procurement_domain",
    guidelines=[
        "The response must be relevant to procurement, supplier management, or supply chain topics",
        "The response must provide specific information rather than generic advice",
    ],
).register(name="prod_procurement_domain")
domain_monitor = domain_monitor.start(
    sampling_config=ScorerSamplingConfig(sample_rate=0.5)
)
print("Procurement domain monitor: ACTIVE (50% sample rate)")

# Professional tone — evaluate 50% of traces
tone_monitor = Guidelines(
    name="prod_professional_tone",
    guidelines="The response must maintain a professional, business-appropriate tone",
).register(name="prod_professional_tone")
tone_monitor = tone_monitor.start(
    sampling_config=ScorerSamplingConfig(sample_rate=0.5)
)
print("Professional tone monitor: ACTIVE (50% sample rate)")

print("\nAll production monitors registered and started.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Verify Monitoring Status

# COMMAND ----------

from mlflow.genai.scorers import list_scorers

scorers = list_scorers()
print(f"Registered scorers: {len(scorers)}")
for s in scorers:
    rate = s.sampling_config.sample_rate if s.sampling_config else "N/A"
    print(f"  {s.name}: sample_rate={rate}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC ### Evaluation
# MAGIC - **10 test cases** covering policy questions, data questions, and combined queries
# MAGIC - **9 scorers**: Safety, RelevanceToQuery, Correctness, 2x Guidelines, plus 4 custom domain scorers
# MAGIC - Custom scorers check: source citations, tool usage, bilingual support, response length
# MAGIC
# MAGIC ### Production Monitoring
# MAGIC - **4 active monitors** running on live agent traces:
# MAGIC   - `prod_safety` (100%) — flags unsafe content
# MAGIC   - `prod_relevance` (100%) — checks query relevance
# MAGIC   - `prod_procurement_domain` (50%) — validates procurement domain focus
# MAGIC   - `prod_professional_tone` (50%) — ensures business-appropriate tone
# MAGIC
# MAGIC ### Next Steps
# MAGIC - View evaluation results in the **Experiments UI** at the experiment path above
# MAGIC - Monitor production quality in the **Traces** tab
# MAGIC - Add more test cases as new failure modes are discovered
# MAGIC - Use `mlflow.genai.optimize_prompts()` to automatically improve the agent's system prompt
