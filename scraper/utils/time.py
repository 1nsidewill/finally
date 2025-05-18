from datetime import datetime

def safe_parse_datetime(dt_str):
    if not dt_str:
        return None
    dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
    return dt.replace(tzinfo=None)