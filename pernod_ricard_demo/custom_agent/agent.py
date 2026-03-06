"""
Pernod Ricard Japan - Supplier Intelligence Agent
Custom agent built with Databricks Agent Framework + MLflow 3.

Uses:
- LangGraph for orchestration
- Vector Search for RAG over procurement policies
- UC Functions as tools for supplier data queries
- MLflow autologging for tracing to Unity Catalog
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
TRACE_CATALOG = "opm_catalog"
TRACE_SCHEMA = "supplier_hub"

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

        # Vector Search for procurement policy RAG
        vs_tool = VectorSearchRetrieverTool(
            index_name=VS_INDEX,
            num_results=5,
            columns=["chunk_id", "doc_name", "section", "content"],
            tool_name="procurement_policy_search",
            tool_description="Search Pernod Ricard Japan procurement policies for information about certifications, pricing limits, audit requirements, lead time standards, Japan-specific compliance, and supplier scoring criteria. Use this for any policy-related question.",
        )
        self.tools.append(vs_tool)

        # UC Functions for supplier data
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


# Enable autologging — traces all LangChain/LangGraph calls automatically
mlflow.langchain.autolog()

# Set trace destination to Unity Catalog if MLFLOW_TRACING_DESTINATION is configured
# (set via serving endpoint env var or notebook)
_trace_dest = os.environ.get("MLFLOW_TRACING_DESTINATION")
if _trace_dest:
    try:
        from mlflow.entities import UCSchemaLocation
        parts = _trace_dest.split(".")
        if len(parts) == 2:
            mlflow.tracing.set_destination(
                destination=UCSchemaLocation(
                    catalog_name=parts[0],
                    schema_name=parts[1],
                )
            )
    except Exception:
        pass  # Tracing destination will fall back to default

AGENT = SupplierIntelligenceAgent()
mlflow.models.set_model(AGENT)
