from datetime import datetime, timezone

def safe_parse_datetime(dt_str):
    if not dt_str:
        return None
    dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
    dt = dt.replace(microsecond=0, tzinfo=None)  # 마이크로초 제거 + tz 제거
    return dt

def safe_parse_unix_timestamp(dt_value):
    if not dt_value:
        return None
    dt = datetime.fromtimestamp(dt_value, tz=timezone.utc)
    dt = dt.replace(microsecond=0, tzinfo=None)
    return dt

def normalize_datetime(dt):
    return dt.replace(microsecond=0) if isinstance(dt, datetime) else dt