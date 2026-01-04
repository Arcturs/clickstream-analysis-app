import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Dict, Any


@dataclass
class ClickEventPayload:
    event_title: Optional[str] = None
    element_id: Optional[str] = None
    x: Optional[int] = None
    y: Optional[int] = None
    element_text: Optional[str] = None
    element_class: Optional[str] = None
    page_title: Optional[str] = None
    viewport_width: Optional[int] = None
    viewport_height: Optional[int] = None
    scroll_position: Optional[float] = None
    timestamp_offset: Optional[int] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ClickEventPayload':
        snake_case_data = {}
        for key, value in data.items():
            snake_key = re.sub(r'(?<!^)(?=[A-Z])', '_', key).lower()
            snake_case_data[snake_key] = value

        return cls(**snake_case_data)

@dataclass
class ClickEvent:
    id: str
    type: str
    created_at: datetime
    received_at: datetime
    session_id: str
    ip: str
    user_id: int
    url: str
    referrer: Optional[str] = None
    device_type: str = ""
    user_agent: str = ""
    payload: Optional[ClickEventPayload] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ClickEvent':
        snake_case_data = {}
        for key, value in data.items():
            snake_key = re.sub(r'(?<!^)(?=[A-Z])', '_', key).lower()
            snake_case_data[snake_key] = value

        if 'payload' in snake_case_data and snake_case_data['payload']:
            snake_case_data['payload'] = ClickEventPayload.from_dict(snake_case_data['payload'])

        def parse_datetime_safe(value):
            if value is None:
                return None
            if isinstance(value, datetime):
                return value
            if isinstance(value, str):
                value = value.strip().strip('"\'')
                formats = [
                    "%Y-%m-%dT%H:%M:%S",
                    "%Y-%m-%d %H:%M:%S",
                    "%Y-%m-%dT%H:%M:%S.%f",
                    "%Y-%m-%d %H:%M:%S.%f",
                    "%Y-%m-%dT%H:%M:%SZ",
                    "%Y-%m-%dT%H:%M:%S.%fZ",
                ]
                for fmt in formats:
                    try:
                        return datetime.strptime(value, fmt)
                    except ValueError:
                        continue
            if isinstance(value, (int, float)):
                try:
                    return datetime.fromtimestamp(float(value))
                except Exception:
                    return None
            return None

        for date_field in ['created_at', 'received_at']:
            if date_field in snake_case_data:
                snake_case_data[date_field] = parse_datetime_safe(snake_case_data[date_field])

        return cls(**snake_case_data)