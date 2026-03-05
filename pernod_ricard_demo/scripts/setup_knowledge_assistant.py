"""
Layer 2: Set up Knowledge Assistant with procurement documents.
Uploads docs to a Unity Catalog Volume and creates a Vector Search index.
"""
import os
import glob
import io
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import VolumeType

PROFILE = "fe-vm-serverless-opm"
CATALOG = "opm_catalog"
SCHEMA = "supplier_hub"
VOLUME = "procurement_docs"

def main():
    w = WorkspaceClient(profile=PROFILE)
    user = w.current_user.me()
    print(f"Connected as: {user.display_name}")

    # Create volume for documents
    try:
        w.volumes.create(
            catalog_name=CATALOG,
            schema_name=SCHEMA,
            name=VOLUME,
            volume_type=VolumeType.MANAGED,
            comment="Pernod Ricard procurement policy documents"
        )
        print(f"Created volume: {CATALOG}.{SCHEMA}.{VOLUME}")
    except Exception as e:
        if "ALREADY_EXISTS" in str(e) or "already exists" in str(e).lower():
            print(f"Volume already exists: {CATALOG}.{SCHEMA}.{VOLUME}")
        else:
            raise

    # Upload documents
    docs_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "docs")
    volume_path = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

    for filepath in glob.glob(os.path.join(docs_dir, "*.md")):
        filename = os.path.basename(filepath)
        with open(filepath, "rb") as f:
            content = f.read()
        target = f"{volume_path}/{filename}"
        w.files.upload(target, io.BytesIO(content), overwrite=True)
        print(f"Uploaded: {filename} -> {target}")

    print(f"\nDocuments uploaded to {volume_path}")
    print("Next: Create a Knowledge Assistant in the Databricks UI or via Agent Bricks API")
    print(f"  - Point it to volume: {volume_path}")
    print(f"  - Use endpoint: databricks-claude-sonnet-4-5")


if __name__ == "__main__":
    main()
