"""Log the Supplier Intelligence Agent to MLflow and register in Unity Catalog."""
import mlflow
from mlflow.models.resources import DatabricksServingEndpoint, DatabricksFunction, DatabricksVectorSearchIndex
from agent import AGENT, LLM_ENDPOINT, UC_FUNCTIONS, VS_INDEX
from unitycatalog.ai.langchain.toolkit import UnityCatalogTool
from databricks_langchain import VectorSearchRetrieverTool

mlflow.set_registry_uri("databricks-uc")
mlflow.set_experiment("/Users/chaima.berrachdi@databricks.com/pernod_ricard_demo/supplier_intelligence_agent")

# Collect resources for auto authentication
resources = [DatabricksServingEndpoint(endpoint_name=LLM_ENDPOINT)]

for tool in AGENT.tools:
    if isinstance(tool, UnityCatalogTool):
        resources.append(DatabricksFunction(function_name=tool.uc_function_name))
    elif isinstance(tool, VectorSearchRetrieverTool):
        resources.extend(tool.resources)

print(f"Resources: {[str(r) for r in resources]}")

input_example = {
    "input": [{"role": "user", "content": "What certifications are required for food-contact suppliers?"}]
}

with mlflow.start_run():
    model_info = mlflow.pyfunc.log_model(
        name="agent",
        python_model="agent.py",
        input_example=input_example,
        resources=resources,
        pip_requirements=[
            "mlflow==3.6.0",
            "databricks-langchain",
            "langgraph==0.3.4",
            "pydantic",
            "databricks-agents",
        ],
    )
    print(f"Model URI: {model_info.model_uri}")

# Register to Unity Catalog
catalog = "opm_catalog"
schema = "agents"
model_name = "supplier_intelligence_agent"

uc_model_info = mlflow.register_model(
    model_uri=model_info.model_uri,
    name=f"{catalog}.{schema}.{model_name}"
)
print(f"Registered: {uc_model_info.name} version {uc_model_info.version}")
