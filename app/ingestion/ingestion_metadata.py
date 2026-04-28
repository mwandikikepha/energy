import uuid
from datetime import datetime, timezone


def generate_batch_id() -> str:
    
    #Generate a unique ID for a single pipeline run.
   
    return str(uuid.uuid4())


def get_run_metadata() -> dict:
  
   # Returns the metadata dict that every pipeline run starts with.
 
    return {
        "batch_id":  generate_batch_id(),
        "started_at": datetime.now(timezone.utc).isoformat(),
        "status":     "running",
    }
