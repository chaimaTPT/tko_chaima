# Databricks notebook source
# MAGIC %md
# MAGIC # Deploy Supplier Intelligence Agent with Tracing & Autologging
# MAGIC
# MAGIC This notebook:
# MAGIC 1. Links a UC schema to an MLflow experiment for trace storage
# MAGIC 2. Writes the agent code to a file and logs it using **code-based logging**
# MAGIC 3. Registers the new version in Unity Catalog
# MAGIC 4. Deploys to the serving endpoint with `MLFLOW_TRACING_DESTINATION` set
# MAGIC 5. Grants permissions on trace tables

# COMMAND ----------

# MAGIC %pip install --upgrade "mlflow[databricks]>=3.9.0" databricks-langchain langgraph pydantic databricks-agents openai
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

import os
import mlflow

CATALOG = "opm_catalog"
AGENT_SCHEMA = "agents"
TRACE_SCHEMA = "supplier_hub"
MODEL_NAME = "supplier_intelligence_agent"
FULL_MODEL_NAME = f"{CATALOG}.{AGENT_SCHEMA}.{MODEL_NAME}"
ENDPOINT_NAME = "supplier-intelligence-agent"
SQL_WAREHOUSE_ID = "6457618d1c009dd6"
EXPERIMENT_NAME = "/Users/chaima.berrachdi@databricks.com/pernod_ricard_demo/supplier_intelligence_agent"
TRACE_DESTINATION = f"{CATALOG}.{TRACE_SCHEMA}"

mlflow.set_tracking_uri("databricks")
mlflow.set_registry_uri("databricks-uc")
os.environ["MLFLOW_TRACING_SQL_WAREHOUSE_ID"] = SQL_WAREHOUSE_ID

print(f"Model: {FULL_MODEL_NAME}")
print(f"Endpoint: {ENDPOINT_NAME}")
print(f"Trace destination: {TRACE_DESTINATION}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Link UC Schema to Experiment (Trace Ingestion)

# COMMAND ----------

from mlflow.entities import UCSchemaLocation
from mlflow.tracing.enablement import set_experiment_trace_location

# Create or get experiment
if experiment := mlflow.get_experiment_by_name(EXPERIMENT_NAME):
    experiment_id = experiment.experiment_id
    print(f"Existing experiment: {experiment_id}")
else:
    experiment_id = mlflow.create_experiment(name=EXPERIMENT_NAME)
    print(f"Created experiment: {experiment_id}")

mlflow.set_experiment(EXPERIMENT_NAME)

# Link UC schema — creates trace tables automatically
try:
    set_experiment_trace_location(
        location=UCSchemaLocation(
            catalog_name=CATALOG,
            schema_name=TRACE_SCHEMA,
        ),
        experiment_id=experiment_id,
    )
    print(f"Traces linked to: {TRACE_DESTINATION}")
except Exception as e:
    print(f"Trace location note: {e}")
    print("Traces may already be linked or the preview may not be enabled.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Write Agent Code to File
# MAGIC
# MAGIC MLflow requires **code-based logging** for complex agents. We write `agent.py` to disk
# MAGIC and pass the file path to `log_model()`.

# COMMAND ----------

AGENT_CODE = '''
"""
Pernod Ricard Japan - Supplier Intelligence Agent
Custom agent built with Databricks Agent Framework + MLflow 3.
"""
import os
import mlflow
from mlflow.pyfunc import ResponsesAgent
from mlflow.types.responses import (
    ResponsesAgentRequest,
    ResponsesAgentResponse,
    ResponsesAgentStreamEvent,
    output_to_responses_items_stream,
    to_chat_completions_input,
)
from databricks_langchain import ChatDatabricks, UCFunctionToolkit, VectorSearchRetrieverTool
from langchain_core.messages import AIMessage
from langchain_core.runnables import RunnableLambda
from langgraph.graph import END, StateGraph
from langgraph.graph.message import add_messages
from langgraph.prebuilt.tool_node import ToolNode
from typing import Annotated, Generator, Sequence, TypedDict

LLM_ENDPOINT = "databricks-claude-sonnet-4-5"
VS_INDEX = "opm_catalog.supplier_hub.procurement_docs_index"

SYSTEM_PROMPT = """You are the Pernod Ricard Japan Supplier Intelligence Assistant.
You help procurement managers with supplier data analysis and policy compliance.

You have access to:
1. **Procurement Policy Knowledge Base** (via vector search) - company policies on certifications, pricing, lead times, audits, Japan-specific compliance
2. **Supplier Data Tools** (via UC Functions):
   - get_supplier_overview: Get full details and delivery metrics for a specific supplier
   - list_non_compliant_suppliers: List all escalated/non-compliant suppliers
   - get_spend_by_category: Procurement spend breakdown by category
   - validate_delivery_quality: Audit delivery performance for a specific supplier
   - get_supplier_risk_summary: Portfolio risk distribution

When answering:
- Always check policies first if the question involves compliance rules
- Use data tools to back up answers with real numbers
- Support both English and Japanese queries
- Cite specific policy sections when relevant
- Be concise but thorough
"""

UC_FUNCTIONS = [
    "opm_catalog.supplier_hub.get_supplier_overview",
    "opm_catalog.supplier_hub.list_non_compliant_suppliers",
    "opm_catalog.supplier_hub.get_spend_by_category",
    "opm_catalog.supplier_hub.validate_delivery_quality",
    "opm_catalog.supplier_hub.get_supplier_risk_summary",
]


class AgentState(TypedDict):
    messages: Annotated[Sequence, add_messages]


class SupplierIntelligenceAgent(ResponsesAgent):
    def __init__(self):
        self.llm = ChatDatabricks(endpoint=LLM_ENDPOINT, temperature=0.1)
        self.tools = []

        vs_tool = VectorSearchRetrieverTool(
            index_name=VS_INDEX,
            num_results=5,
            columns=["chunk_id", "doc_name", "section", "content"],
            tool_name="procurement_policy_search",
            tool_description="Search Pernod Ricard Japan procurement policies for information about certifications, pricing limits, audit requirements, lead time standards, Japan-specific compliance, and supplier scoring criteria. Use this for any policy-related question.",
        )
        self.tools.append(vs_tool)

        uc_toolkit = UCFunctionToolkit(function_names=UC_FUNCTIONS)
        self.tools.extend(uc_toolkit.tools)

        self.llm_with_tools = self.llm.bind_tools(self.tools)

    def _build_graph(self):
        llm_with_tools = self.llm_with_tools
        system_prompt = SYSTEM_PROMPT

        def should_continue(state):
            last = state["messages"][-1]
            if isinstance(last, AIMessage) and last.tool_calls:
                return "tools"
            return "end"

        def call_model(state):
            messages = [{"role": "system", "content": system_prompt}] + state["messages"]
            response = llm_with_tools.invoke(messages)
            return {"messages": [response]}

        graph = StateGraph(AgentState)
        graph.add_node("agent", RunnableLambda(call_model))
        graph.add_node("tools", ToolNode(self.tools))
        graph.add_conditional_edges("agent", should_continue, {"tools": "tools", "end": END})
        graph.add_edge("tools", "agent")
        graph.set_entry_point("agent")
        return graph.compile()

    def predict(self, request: ResponsesAgentRequest) -> ResponsesAgentResponse:
        outputs = [
            event.item
            for event in self.predict_stream(request)
            if event.type == "response.output_item.done"
        ]
        return ResponsesAgentResponse(output=outputs)

    def predict_stream(
        self, request: ResponsesAgentRequest
    ) -> Generator[ResponsesAgentStreamEvent, None, None]:
        messages = to_chat_completions_input([m.model_dump() for m in request.input])
        graph = self._build_graph()

        for event in graph.stream({"messages": messages}, stream_mode=["updates"]):
            if event[0] == "updates":
                for node_data in event[1].values():
                    if node_data.get("messages"):
                        yield from output_to_responses_items_stream(node_data["messages"])


# Enable autologging for tracing
mlflow.langchain.autolog()

# Set trace destination if configured
_trace_dest = os.environ.get("MLFLOW_TRACING_DESTINATION")
if _trace_dest:
    try:
        from mlflow.entities import UCSchemaLocation
        parts = _trace_dest.split(".")
        if len(parts) == 2:
            mlflow.tracing.set_destination(
                destination=UCSchemaLocation(catalog_name=parts[0], schema_name=parts[1])
            )
    except Exception:
        pass

AGENT = SupplierIntelligenceAgent()
mlflow.models.set_model(AGENT)
'''

# Write agent.py to the current working directory
agent_file_path = os.path.join(os.getcwd(), "agent.py")
with open(agent_file_path, "w") as f:
    f.write(AGENT_CODE)

print(f"Agent code written to: {agent_file_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Collect Resources & Log Model (Code-Based)

# COMMAND ----------

from mlflow.models.resources import DatabricksServingEndpoint, DatabricksFunction, DatabricksVectorSearchIndex

LLM_ENDPOINT = "databricks-claude-sonnet-4-5"
VS_INDEX = "opm_catalog.supplier_hub.procurement_docs_index"
UC_FUNCTIONS = [
    "opm_catalog.supplier_hub.get_supplier_overview",
    "opm_catalog.supplier_hub.list_non_compliant_suppliers",
    "opm_catalog.supplier_hub.get_spend_by_category",
    "opm_catalog.supplier_hub.validate_delivery_quality",
    "opm_catalog.supplier_hub.get_supplier_risk_summary",
]

resources = [
    DatabricksServingEndpoint(endpoint_name=LLM_ENDPOINT),
    DatabricksVectorSearchIndex(index_name=VS_INDEX),
]
for fn in UC_FUNCTIONS:
    resources.append(DatabricksFunction(function_name=fn))

print(f"Resources ({len(resources)}):")
for r in resources:
    print(f"  {r}")

# COMMAND ----------

input_example = {
    "input": [{"role": "user", "content": "What certifications are required for food-contact suppliers?"}]
}

with mlflow.start_run(run_name="agent_v2_with_tracing"):
    model_info = mlflow.pyfunc.log_model(
        name="agent",
        python_model=agent_file_path,  # Code-based logging — pass file path, NOT the object
        input_example=input_example,
        resources=resources,
        pip_requirements=[
            "mlflow[databricks]>=3.9.0",
            "databricks-langchain",
            "langgraph>=0.3.4",
            "pydantic",
            "databricks-agents",
        ],
    )
    print(f"Model logged: {model_info.model_uri}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Register in Unity Catalog

# COMMAND ----------

uc_model_info = mlflow.register_model(
    model_uri=model_info.model_uri,
    name=FULL_MODEL_NAME,
)
new_version = uc_model_info.version
print(f"Registered: {FULL_MODEL_NAME} version {new_version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Deploy to Serving Endpoint with Tracing

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import (
    EndpointCoreConfigInput,
    ServedEntityInput,
)

w = WorkspaceClient()

served_entity = ServedEntityInput(
    entity_name=FULL_MODEL_NAME,
    entity_version=str(new_version),
    scale_to_zero_enabled=True,
    workload_size="Small",
    environment_vars={
        "MLFLOW_TRACING_DESTINATION": TRACE_DESTINATION,
    },
)

print(f"Deploying {FULL_MODEL_NAME} v{new_version} to {ENDPOINT_NAME}...")
print(f"  MLFLOW_TRACING_DESTINATION = {TRACE_DESTINATION}")

try:
    w.serving_endpoints.update_config_and_wait(
        name=ENDPOINT_NAME,
        served_entities=[served_entity],
    )
    print(f"Updated endpoint: {ENDPOINT_NAME}")
except Exception:
    w.serving_endpoints.create_and_wait(
        name=ENDPOINT_NAME,
        config=EndpointCoreConfigInput(served_entities=[served_entity]),
    )
    print(f"Created endpoint: {ENDPOINT_NAME}")

print("Deployment complete!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Grant Permissions on Trace Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Grant permissions so the serving endpoint SP can write traces
# MAGIC GRANT MODIFY, SELECT ON TABLE opm_catalog.supplier_hub.mlflow_experiment_trace_otel_spans TO `account users`;
# MAGIC GRANT MODIFY, SELECT ON TABLE opm_catalog.supplier_hub.mlflow_experiment_trace_otel_logs TO `account users`;
# MAGIC GRANT MODIFY, SELECT ON TABLE opm_catalog.supplier_hub.mlflow_experiment_trace_otel_metrics TO `account users`;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Test the Endpoint

# COMMAND ----------

import time
time.sleep(10)

client = w.serving_endpoints.get_open_ai_client()

test_response = client.chat.completions.create(
    model=ENDPOINT_NAME,
    messages=[{"role": "user", "content": "What certifications are required for food-contact suppliers?"}],
)
print(f"Response: {test_response.choices[0].message.content[:500]}...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Verify Traces Are Flowing

# COMMAND ----------

time.sleep(30)

traces_df = mlflow.search_traces(
    experiment_ids=[experiment_id],
    max_results=5,
    order_by=["attributes.timestamp_ms DESC"],
)
print(f"Recent traces: {len(traces_df)}")
if len(traces_df) > 0:
    for _, t in traces_df.iterrows():
        print(f"  Trace: status={t.get('status', 'N/A')}, timestamp={t.get('timestamp_ms', 'N/A')}")
else:
    print("No traces yet — they may take a minute to appear after the first request.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | Component | Status |
# MAGIC |-----------|--------|
# MAGIC | **Autologging** | `mlflow.langchain.autolog()` in agent.py — traces all LangChain/LangGraph calls |
# MAGIC | **Trace destination** | `opm_catalog.supplier_hub` (UC Delta tables) |
# MAGIC | **Logging method** | Code-based logging (`python_model="agent.py"`) |
# MAGIC | **UC Registry** | `opm_catalog.agents.supplier_intelligence_agent` |
# MAGIC | **Serving endpoint** | `supplier-intelligence-agent` with `MLFLOW_TRACING_DESTINATION` |
# MAGIC | **Trace tables** | `mlflow_experiment_trace_otel_spans/logs/metrics` |
# MAGIC
# MAGIC **Next steps:**
# MAGIC - Run the `agent_evaluation` notebook to evaluate with scorers
# MAGIC - View traces in the Experiments UI > Traces tab
# MAGIC - Query traces: `SELECT * FROM opm_catalog.supplier_hub.mlflow_experiment_trace_otel_spans`
