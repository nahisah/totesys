from datetime import datetime
from decimal import Decimal


def default_serialiser(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj,Decimal):
        return float(obj)
    raise TypeError(f"Type {type(obj)} not serializable")