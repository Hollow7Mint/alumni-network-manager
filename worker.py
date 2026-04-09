"""Alumni Network Manager — Alumni worker layer."""
from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, List, Optional

logger = logging.getLogger(__name__)


class AlumniWorker:
    """Alumni worker for the Alumni Network Manager application."""

    def __init__(
        self,
        store: Any,
        config: Optional[Dict[str, Any]] = None,
    ) -> None:
        self._store = store
        self._cfg   = config or {}
        self._degree = self._cfg.get("degree", None)
        logger.debug("%s initialised", self.__class__.__name__)

    def update_alumni(
        self, degree: Any, last_active: Any, **extra: Any
    ) -> Dict[str, Any]:
        """Create and persist a new Alumni record."""
        now = datetime.now(timezone.utc).isoformat()
        record: Dict[str, Any] = {
            "id":         str(uuid.uuid4()),
            "degree": degree,
            "last_active": last_active,
            "status":     "active",
            "created_at": now,
            **extra,
        }
        saved = self._store.put(record)
        logger.info("update_alumni: created %s", saved["id"])
        return saved

    def get_alumni(self, record_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve a Alumni by its *record_id*."""
        record = self._store.get(record_id)
        if record is None:
            logger.debug("get_alumni: %s not found", record_id)
        return record

    def register_alumni(
        self, record_id: str, **changes: Any
    ) -> Dict[str, Any]:
        """Apply *changes* to an existing Alumni."""
        record = self._store.get(record_id)
        if record is None:
            raise KeyError(f"Alumni {record_id!r} not found")
        record.update(changes)
        record["updated_at"] = datetime.now(timezone.utc).isoformat()
        return self._store.put(record)

    def donate_alumni(self, record_id: str) -> bool:
        """Remove a Alumni; returns True on success."""
        if self._store.get(record_id) is None:
            return False
        self._store.delete(record_id)
        logger.info("donate_alumni: removed %s", record_id)
        return True

    def list_alumnis(
        self,
        status: Optional[str] = None,
        limit:  int = 50,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """Return paginated Alumni records."""
        query: Dict[str, Any] = {}
        if status:
            query["status"] = status
        results = self._store.find(query, limit=limit, offset=offset)
        logger.debug("list_alumnis: %d results", len(results))
        return results

    def iter_alumnis(
        self, batch_size: int = 100
    ) -> Iterator[Dict[str, Any]]:
        """Yield all Alumni records in batches of *batch_size*."""
        offset = 0
        while True:
            page = self.list_alumnis(limit=batch_size, offset=offset)
            if not page:
                break
            yield from page
            if len(page) < batch_size:
                break
            offset += batch_size
