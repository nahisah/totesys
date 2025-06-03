from datetime import datetime

"""
This function is a custom serialiser function that converts datetime objects into a ISO 8601 string 

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
    raise TypeError(f"Type {type(obj)} not serializable")