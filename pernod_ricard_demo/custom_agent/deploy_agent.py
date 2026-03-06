"""Deploy the Supplier Intelligence Agent to a serving endpoint with tracing enabled."""
import sys
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import (
    EndpointCoreConfigInput,
    ServedEntityInput,
)

model_name = sys.argv[1] if len(sys.argv) > 1 else "opm_catalog.agents.supplier_intelligence_agent"
version = sys.argv[2] if len(sys.argv) > 2 else "1"
endpoint_name = "supplier-intelligence-agent"

# Tracing destination — traces go to UC Delta tables
TRACE_DESTINATION = "opm_catalog.supplier_hub"

print(f"Deploying {model_name} version {version} to {endpoint_name}...")
print(f"Tracing destination: {TRACE_DESTINATION}")

w = WorkspaceClient()

served_entity = ServedEntityInput(
    entity_name=model_name,
    entity_version=version,
    scale_to_zero_enabled=True,
    workload_size="Small",
    environment_vars={
        "MLFLOW_TRACING_DESTINATION": TRACE_DESTINATION,
    },
)

try:
    # Try to update existing endpoint
    w.serving_endpoints.update_config_and_wait(
        name=endpoint_name,
        served_entities=[served_entity],
    )
    print(f"Updated endpoint: {endpoint_name}")
except Exception:
    # Create new endpoint
    w.serving_endpoints.create_and_wait(
        name=endpoint_name,
        config=EndpointCoreConfigInput(
            served_entities=[served_entity]
        ),
    )
    print(f"Created endpoint: {endpoint_name}")

print(f"Deployment complete! Endpoint: {endpoint_name}")
print(f"Traces will be stored in: {TRACE_DESTINATION}")
