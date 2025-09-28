import math
from typing import Any, Dict, List, Union

def sanitize_float(value: float) -> float:
    """Konvertiert Float-Werte in JSON-kompatible Werte."""
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return 0.0
        return value
    return value

def sanitize_dict(data: Union[Dict, List, Any]) -> Union[Dict, List, Any]:
    """Rekursiv alle Float-Werte in einem Dictionary oder einer Liste bereinigen."""
    if isinstance(data, dict):
        return {k: sanitize_dict(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [sanitize_dict(v) for v in data]
    elif isinstance(data, float):
        return sanitize_float(data)
    return data
