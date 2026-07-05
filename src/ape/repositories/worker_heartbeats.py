from __future__ import annotations

from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from ape.db.models import WorkerHeartbeat
from ape.repositories.inputs import WorkerHeartbeatInput


class WorkerHeartbeatRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def record_heartbeat(self, heartbeat: WorkerHeartbeatInput) -> WorkerHeartbeat:
        row = WorkerHeartbeat(
            service_name=heartbeat.service_name,
            started_at=heartbeat.started_at,
            heartbeat_at=heartbeat.heartbeat_at,
            app_mode=heartbeat.app_mode,
            is_safe=heartbeat.is_safe,
            metadata_=heartbeat.metadata,
        )
        self.session.add(row)
        self.session.flush()
        return row

    def get_latest_heartbeat(self, service_name: str) -> WorkerHeartbeat | None:
        return self.session.scalar(
            select(WorkerHeartbeat)
            .where(WorkerHeartbeat.service_name == service_name)
            .order_by(desc(WorkerHeartbeat.heartbeat_at), desc(WorkerHeartbeat.id))
            .limit(1)
        )

