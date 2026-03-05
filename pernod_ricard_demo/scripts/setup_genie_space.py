"""
Layer 4a: Create a Genie Space for procurement managers.
Natural language queries over validated supplier data.
"""
import json
from databricks.sdk import WorkspaceClient

PROFILE = "fe-vm-serverless-opm"
CATALOG = "opm_catalog"
SCHEMA = "supplier_hub"
WAREHOUSE_ID = "6457618d1c009dd6"

def main():
    w = WorkspaceClient(profile=PROFILE)

    tables = [
        f"{CATALOG}.{SCHEMA}.gold_suppliers",
        f"{CATALOG}.{SCHEMA}.escalation_queue",
        f"{CATALOG}.{SCHEMA}.deliveries",
        f"{CATALOG}.{SCHEMA}.product_master",
        f"{CATALOG}.{SCHEMA}.suppliers_cleaned",
    ]

    description = """Pernod Ricard Japan Supplier Intelligence Hub.

Ask natural language questions about supplier data. Examples:
- Which suppliers are missing certifications?
- Top glass bottle suppliers by reliability in Kansai region?
- Average lead time for packaging suppliers?
- Which suppliers have been flagged for escalation and why?
- Delivery performance trends for the past year?
- Compare supplier pricing across categories
- List all suppliers in Tokyo with ISO 22000 certification"""

    title = "Pernod Ricard Japan - Supplier Intelligence"

    # Build the serialized space JSON
    table_identifiers = [{"table": t} for t in tables]
    serialized_space = json.dumps({
        "table_identifiers": table_identifiers,
    })

    try:
        space = w.genie.create_space(
            warehouse_id=WAREHOUSE_ID,
            serialized_space=serialized_space,
            title=title,
            description=description,
        )
        print(f"Created Genie Space!")
        host = "https://fevm-serverless-opm.cloud.databricks.com"
        print(f"URL: {host}/genie/rooms/{space.space_id if hasattr(space, 'space_id') else 'check-ui'}")
    except Exception as e:
        print(f"Error creating Genie Space via SDK: {e}")
        print("\nFalling back to REST API...")
        try:
            resp = w.api_client.do(
                "POST",
                "/api/2.0/genie/spaces",
                body={
                    "title": title,
                    "description": description,
                    "warehouse_id": WAREHOUSE_ID,
                    "table_identifiers": table_identifiers,
                }
            )
            print(f"Created Genie Space: {resp}")
        except Exception as e2:
            print(f"REST API also failed: {e2}")
            print("\nPlease create the Genie Space manually in the Databricks UI:")
            print(f"  1. Go to https://fevm-serverless-opm.cloud.databricks.com/genie")
            print(f"  2. Click 'New Space'")
            print(f"  3. Title: {title}")
            print(f"  4. Add tables: {', '.join(tables)}")
            print(f"  5. Warehouse: Serverless Starter Warehouse")


if __name__ == "__main__":
    main()
