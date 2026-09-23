import json
import logging

from fastapi import FastAPI, Header, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from pydantic import ValidationError

from app.config import Settings, settings
from app.core.clock import SystemClock
from app.models import OrderEvent
from app.repositories.workflow import WorkflowRepository
from app.security import verify_signature
from app.services.intake import EventConflict, IntakeService

log = logging.getLogger(__name__)


def create_app(repository: WorkflowRepository | None = None, config: Settings | None = None) -> FastAPI:
    cfg = config or settings()
    api = FastAPI(title="Reliable Webhook Workflow", version="1.0.0")
    api.state.repository = repository

    def get_repository() -> WorkflowRepository:
        if api.state.repository is None:
            api.state.repository = WorkflowRepository(cfg.database_path)
        return api.state.repository


    @api.post("/webhooks/orders")
    async def receive_order(request: Request, x_webhook_signature: str | None = Header(default=None)) -> Response:
        raw = await request.body()
        if not verify_signature(raw, x_webhook_signature, cfg.webhook_secret):
            raise HTTPException(status_code=401, detail={"error": "invalid_signature"})
        try:
            event = OrderEvent.model_validate(json.loads(raw))
        except (json.JSONDecodeError, UnicodeDecodeError, ValidationError) as exc:
            raise HTTPException(status_code=422, detail={"error": "invalid_event"}) from exc
        try:
            repo = get_repository()
            event_id, job_id, duplicate = IntakeService(repo, SystemClock(), cfg.max_attempts).accept(event)
        except EventConflict:
            return JSONResponse(status_code=409, content={"error": "event_conflict", "detail": "event_id already exists with different payload"})
        except Exception as exc:
            log.exception("durable webhook intake failed")
            raise HTTPException(status_code=503, detail={"error": "persistence_unavailable"}) from exc
        return JSONResponse(status_code=200 if duplicate else 202,
                            content={"status": "accepted", "event_id": event_id, "job_id": job_id, "duplicate": duplicate})


    @api.get("/events/{event_id}")
    async def get_event(event_id: str) -> dict[str, object]:
        row = get_repository().get_event(event_id)
        if not row:
            raise HTTPException(404, detail={"error": "not_found"})
        row["payload"] = json.loads(row.pop("payload_json"))
        return row


    @api.get("/jobs")
    async def get_jobs() -> list[dict[str, object]]:
        return get_repository().list_jobs()


    @api.get("/jobs/{job_id}")
    async def get_job(job_id: str) -> dict[str, object]:
        row = get_repository().get_job(job_id)
        if not row:
            raise HTTPException(404, detail={"error": "not_found"})
        return row


    @api.get("/jobs/{job_id}/attempts")
    async def get_attempts(job_id: str) -> list[dict[str, object]]:
        repo = get_repository()
        if not repo.get_job(job_id):
            raise HTTPException(404, detail={"error": "not_found"})
        return repo.attempts(job_id)


    @api.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    return api


app = create_app()
