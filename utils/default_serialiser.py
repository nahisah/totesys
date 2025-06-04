from datetime import datetime
from decimal import Decimal

"""
This function is a custom serialiser function that converts datetime objects into a ISO 8601 string and a decimal object
 into a string

Arguments: 
    obj: Any python object to serialise

Returns:
    str: ISO 8601 formatted string 

Raises:
    TypeError: If the object type is not supported for serialisation 
"""

def default_serialiser(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return str(obj)
    raise TypeError(f"Type {type(obj)} not serializable")