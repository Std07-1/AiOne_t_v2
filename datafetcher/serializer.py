import logging
import json
import numpy as np
import pandas as pd
from typing import Any, Union

logger = logging.getLogger("make_serializable")


def make_serializable(obj: Any) -> Any:
    """
    Рекурсивно конвертує об'єкти, що не підтримуються JSON, у базові формати:
    - np.bool_, np.integer, np.floating -> вбудовані bool, int, float
    - pd.Timestamp -> str (ISO формат або інший)
    - np.ndarray -> list
    - dict, list -> рекурсивно обробляються
    - pd.DataFrame -> list[dict] (через .to_dict(orient="records"))
    - інші об'єкти -> str(obj) (fallback)
    """
    if isinstance(obj, dict):
        return {k: make_serializable(v) for k, v in obj.items()}

    elif isinstance(obj, pd.Timestamp):
        return obj.isoformat()
    
    elif isinstance(obj, (list, tuple)):
        return [make_serializable(i) for i in obj]

    elif isinstance(obj, np.bool_):
        return bool(obj)
    elif isinstance(obj, (np.integer, int)):
        return int(obj)
    elif isinstance(obj, (np.floating, float)):
        return float(obj)

    elif isinstance(obj, np.ndarray):
        return obj.tolist()

    elif isinstance(obj, pd.DataFrame):
        # Серіалізуємо DataFrame у список словників
        return obj.to_dict(orient="records")
    
    elif isinstance(obj, (list, dict)):
        return obj
    else:
        # fallback: перетворюємо у str
        return str(obj)

def serialize_to_json(value: Any, ensure_ascii: bool = False, indent: Union[None, int] = None) -> str:
    """
    Обгортає json.dumps(), викликаючи make_serializable перед цим.
    """
    serializable_value = make_serializable(value)
    return json.dumps(serializable_value, ensure_ascii=ensure_ascii, indent=indent)


def deserialize_from_json(json_str: str) -> Any:
    """
    Стандартна десеріалізація JSON (reverse до serialize_to_json).
    """
    return json.loads(json_str)
