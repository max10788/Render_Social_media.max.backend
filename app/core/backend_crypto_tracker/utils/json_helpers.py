import json
from datetime import datetime, date
from decimal import Decimal
import math
from typing import Any, Dict, List, Union

class SafeJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, float):
            if math.isnan(obj) or math.isinf(obj):
                return None
        elif isinstance(obj, Decimal):
            return float(obj)
        elif isinstance(obj, (datetime, date)):
            return obj.isoformat()
        return super().default(obj)

def sanitize_float(value: Any) -> Any:
    """Konvertiert NaN oder Infinity zu None für JSON-Serialisierung"""
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
    return value

def sanitize_dict(data: Dict[str, Any]) -> Dict[str, Any]:
    """Bereinigt ein Dictionary von ungültigen Float-Werten"""
    return {k: sanitize_value(v) for k, v in data.items()}

def sanitize_list(data: List[Any]) -> List[Any]:
    """Bereinigt eine Liste von ungültigen Float-Werten"""
    return [sanitize_value(item) for item in data]

def sanitize_value(value: Any) -> Any:
    """Rekursive Funktion zur Bereinigung von Datenstrukturen"""
    if isinstance(value, dict):
        return sanitize_dict(value)
    elif isinstance(value, list):
        return sanitize_list(value)
    elif isinstance(value, float):
        return sanitize_float(value)
    return value
