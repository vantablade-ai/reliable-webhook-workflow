import hashlib
import json

from pydantic import BaseModel


def payload_fingerprint(event: BaseModel) -> str:
    canonical = json.dumps(event.model_dump(mode="json"), sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
