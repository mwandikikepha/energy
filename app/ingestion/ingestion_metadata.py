import uuid
from datetime import datetime, timezone


def generate_batch_id() -> str:
    """
    Generate a unique ID for a single pipeline run.
    Every record ingested in that run carries this ID so you can
    trace, reprocess, or audit any record back to its exact run.
    """
    return str(uuid.uuid4())


def get_run_metadata() -> dict:
    """
    Returns the metadata dict that every pipeline run starts with.
    Passed into the loaders and written to pipeline_runs in MongoDB.
    """
    return {
        "batch_id":  generate_batch_id(),
        "started_at": datetime.now(timezone.utc).isoformat(),
        "status":     "running",
    }