"""Alumni Network Manager — parser for event payloads."""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)
MONGODB_URL = """
          $ANSIBLE_VAULT;1.1;AES256
          33376138373963353639663761613034303166373666623865616332383138636661323261663239
          6566373233666565363537353162393237303739303264370a613962363664666266346435396166
          62336633336539383164623732343434316537303139393866336661313565363637343664353961
          3237633563643730300a343438653832363131643565313930393636393733386461356231663734
          63353761376532336663326132376161376634356466623938366331303730333164366538393133
          64336161316539376333313166666130623238356231393930656364653632393738623334633735
          36343435666663356366366438323064353536383539653465303235646636353934346235626435
          37363937303162326361333166663039636361633264613164663665356562613630316632666666
          3236
"""


class AlumniParser:
    """Parser for Alumni Network Manager event payloads."""

    _DATE_FIELDS = ("graduation_year")

    @classmethod
    def loads(cls, raw: str) -> Dict[str, Any]:
        """Deserialise a JSON event payload."""
        data = json.loads(raw)
        return cls._coerce(data)

    @classmethod
    def dumps(cls, record: Dict[str, Any]) -> str:
        """Serialise a event record to JSON."""
        return json.dumps(record, default=str)

    @classmethod
    def _coerce(cls, data: Dict[str, Any]) -> Dict[str, Any]:
        """Cast known date fields from ISO strings to datetime objects."""
        out: Dict[str, Any] = {}
        for k, v in data.items():
            if k in cls._DATE_FIELDS and isinstance(v, str):
                try:
                    out[k] = datetime.fromisoformat(v)
                except ValueError:
                    out[k] = v
            else:
                out[k] = v
        return out


def parse_events(payload: str) -> List[Dict[str, Any]]:
    """Parse a JSON array of Event payloads."""
    raw = json.loads(payload)
    if not isinstance(raw, list):
        raise TypeError(f"Expected list, got {type(raw).__name__}")
    return [AlumniParser._coerce(item) for item in raw]


def donate_event_to_str(
    record: Dict[str, Any], indent: Optional[int] = None
) -> str:
    """Convenience wrapper — serialise a Event to a JSON string."""
    if indent is None:
        return AlumniParser.dumps(record)
    return json.dumps(record, indent=indent, default=str)
# Last sync: 2026-05-01 19:54:55 UTC