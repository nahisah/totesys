from datetime import datetime
from decimal import Decimal


def default_serialiser(obj):
    """
    This function is a custom serialiser function that converts datetime objects into a ISO 8601 string and Decimal objects into floats.

    # Arguments:
        obj: A datetime or Decimal object.

    # Returns:
        An ISO 8601 formatted string, if obj is type datetime.
        A float, if obj is type Decimal.

    # Raises:
        TypeError: If the object type is not supported for serialisation.
    """
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f"Type {type(obj)} not serializable")
