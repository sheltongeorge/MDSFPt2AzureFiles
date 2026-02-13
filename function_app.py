# function_app.py (Blob staging version)
import os
import json
import tempfile
import logging
from datetime import datetime, timedelta, timezone
from urllib.parse import quote

import azure.functions as func
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions

from pipeline import run_pipeline

app = func.FunctionApp()

def _get_blob_service() -> BlobServiceClient:
    conn_str = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
    return BlobServiceClient.from_connection_string(conn_str)

def _safe_prefix(prefix: str) -> str:
    # normalize to "runs/<id>/" form
    p = (prefix or "").strip().lstrip("/")
    if p and not p.endswith("/"):
        p += "/"
    return p

@app.route(route="process-blob-folder", methods=["POST"], auth_level=func.AuthLevel.FUNCTION)
def process_blob_folder(req: func.HttpRequest) -> func.HttpResponse:
    """
    Power Automate POST body example:
    {
      "container": "staging",
      "prefix": "runs/<unique-id>/"
    }

    This will:
    - download all blobs under that prefix into a temp folder
    - run run_pipeline(temp_folder)
    - upload the resulting ZIP back to the same prefix
    - return a 14-day SAS download URL for the ZIP
    """
    try:
        body = req.get_json()
        container_name = body["container"]
        prefix = _safe_prefix(body["prefix"])

        blob_service = _get_blob_service()
        container = blob_service.get_container_client(container_name)

        # List blobs under prefix
        blobs = list(container.list_blobs(name_starts_with=prefix))
        if not blobs:
            return func.HttpResponse(
                json.dumps({"status": "error", "message": f"No blobs found under prefix '{prefix}'"}),
                status_code=400,
                mimetype="application/json",
            )

        with tempfile.TemporaryDirectory() as tmpdir:
            logging.info(f"Downloading {len(blobs)} blobs from {container_name}/{prefix} to {tmpdir}")

            # Download each blob to temp folder (flat filenames)
            for b in blobs:
                blob_name = b.name
                filename = os.path.basename(blob_name)
                if not filename:
                    continue
                dest_path = os.path.join(tmpdir, filename)
                downloader = container.download_blob(blob_name)
                with open(dest_path, "wb") as f:
                    f.write(downloader.readall())

            # Run your pipeline
            results = run_pipeline(tmpdir)
            zip_path = results["zip_path"]
            zip_name = os.path.basename(zip_path)

            # Upload ZIP back to same prefix
            zip_blob_name = prefix + zip_name
            with open(zip_path, "rb") as data:
                container.upload_blob(name=zip_blob_name, data=data, overwrite=True)

        # Generate SAS URL valid for 14 days
        # NOTE: This uses the storage account key (via connection string). That’s fine for your current approach.
        account_name = blob_service.account_name

        # Extract account key from connection string
        # Format includes "...;AccountKey=XXXX;..."
        conn_str = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
        key_part = [p for p in conn_str.split(";") if p.startswith("AccountKey=")]
        if not key_part:
            raise RuntimeError("Could not find AccountKey in AZURE_STORAGE_CONNECTION_STRING")
        account_key = key_part[0].split("=", 1)[1]

        sas = generate_blob_sas(
            account_name=account_name,
            container_name=container_name,
            blob_name=zip_blob_name,
            account_key=account_key,
            permission=BlobSasPermissions(read=True),
            expiry=datetime.now(timezone.utc) + timedelta(days=14),
        )

        # Build URL (blob name needs URL encoding)
        encoded_blob_name = quote(zip_blob_name)
        sas_url = f"https://{account_name}.blob.core.windows.net/{container_name}/{encoded_blob_name}?{sas}"

        return func.HttpResponse(
            json.dumps({
                "status": "ok",
                "container": container_name,
                "prefix": prefix,
                "zipBlobName": zip_blob_name,
                "sasUrl": sas_url,
                "expiresDays": 14
            }),
            status_code=200,
            mimetype="application/json",
        )

    except Exception as e:
        logging.exception("Error processing blob folder")
        return func.HttpResponse(
            json.dumps({"status": "error", "message": str(e)}),
            status_code=500,
            mimetype="application/json",
        )
