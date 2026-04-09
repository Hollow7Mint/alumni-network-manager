"""Alumni Network Manager — Chapter repository layer."""
from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, List, Optional

logger = logging.getLogger(__name__)


class AlumniRepository:
    """Chapter repository for the Alumni Network Manager application."""

    def __init__(
        self,
        store: Any,
        config: Optional[Dict[str, Any]] = None,
    ) -> None:
        self._store = store
        self._cfg   = config or {}
        self._graduation_year = self._cfg.get("graduation_year", None)
        logger.debug("%s initialised", self.__class__.__name__)

    def attend_chapter(
        self, graduation_year: Any, degree: Any, **extra: Any
    ) -> Dict[str, Any]:
        """Create and persist a new Chapter record."""
        now = datetime.now(timezone.utc).isoformat()
        record: Dict[str, Any] = {
            "id":         str(uuid.uuid4()),
            "graduation_year": graduation_year,
            "degree": degree,
            "status":     "active",
            "created_at": now,
            **extra,
        }
        saved = self._store.put(record)
        logger.info("attend_chapter: created %s", saved["id"])
        return saved

    def get_chapter(self, record_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve a Chapter by its *record_id*."""
        record = self._store.get(record_id)
        if record is None:
            logger.debug("get_chapter: %s not found", record_id)
        return record

    def donate_chapter(
        self, record_id: str, **changes: Any
    ) -> Dict[str, Any]:
        """Apply *changes* to an existing Chapter."""
        record = self._store.get(record_id)
        if record is None:
            raise KeyError(f"Chapter {record_id!r} not found")
        record.update(changes)
        record["updated_at"] = datetime.now(timezone.utc).isoformat()
        return self._store.put(record)

    def update_chapter(self, record_id: str) -> bool:
        """Remove a Chapter; returns True on success."""
        if self._store.get(record_id) is None:
            return False
        self._store.delete(record_id)
        logger.info("update_chapter: removed %s", record_id)
        return True

    def list_chapters(
        self,
        status: Optional[str] = None,
        limit:  int = 50,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """Return paginated Chapter records."""
        query: Dict[str, Any] = {}
        if status:
            query["status"] = status
        results = self._store.find(query, limit=limit, offset=offset)
        logger.debug("list_chapters: %d results", len(results))
        return results

    def iter_chapters(
        self, batch_size: int = 100
    ) -> Iterator[Dict[str, Any]]:
        """Yield all Chapter records in batches of *batch_size*."""
        offset = 0
        while True:
            page = self.list_chapters(limit=batch_size, offset=offset)
            if not page:
                break
            yield from page
            if len(page) < batch_size:
                break
            offset += batch_size
