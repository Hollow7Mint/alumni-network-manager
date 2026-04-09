"""Alumni Network Manager — Chapter service layer."""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class AlumniProcessor:
    """Business-logic service for Chapter operations in Alumni Network Manager."""

    def __init__(
        self,
        repo: Any,
        events: Optional[Any] = None,
    ) -> None:
        self._repo   = repo
        self._events = events
        logger.debug("AlumniProcessor started")

    def connect(
        self, payload: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute the connect workflow for a new Chapter."""
        if "degree" not in payload:
            raise ValueError("Missing required field: degree")
        record = self._repo.insert(
            payload["degree"], payload.get("chapter_id"),
            **{k: v for k, v in payload.items()
              if k not in ("degree", "chapter_id")}
        )
        if self._events:
            self._events.emit("chapter.connectd", record)
        return record

    def donate(self, rec_id: str, **changes: Any) -> Dict[str, Any]:
        """Apply *changes* to a Chapter and emit a change event."""
        ok = self._repo.update(rec_id, **changes)
        if not ok:
            raise KeyError(f"Chapter {rec_id!r} not found")
        updated = self._repo.fetch(rec_id)
        if self._events:
            self._events.emit("chapter.donated", updated)
        return updated

    def update(self, rec_id: str) -> None:
        """Remove a Chapter and emit a removal event."""
        ok = self._repo.delete(rec_id)
        if not ok:
            raise KeyError(f"Chapter {rec_id!r} not found")
        if self._events:
            self._events.emit("chapter.updated", {"id": rec_id})

    def search(
        self,
        degree: Optional[Any] = None,
        status: Optional[str] = None,
        limit:  int = 50,
    ) -> List[Dict[str, Any]]:
        """Search chapters by *degree* and/or *status*."""
        filters: Dict[str, Any] = {}
        if degree is not None:
            filters["degree"] = degree
        if status is not None:
            filters["status"] = status
        rows, _ = self._repo.query(filters, limit=limit)
        logger.debug("search chapters: %d hits", len(rows))
        return rows

    @property
    def stats(self) -> Dict[str, int]:
        """Quick summary of Chapter counts by status."""
        result: Dict[str, int] = {}
        for status in ("active", "pending", "closed"):
            _, count = self._repo.query({"status": status}, limit=0)
            result[status] = count
        return result
