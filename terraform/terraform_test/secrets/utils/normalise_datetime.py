from datetime import datetime

def normalise_datetimes(rows):
    """
    Convert datetime objects to string in the format 'YYYY-MM-DD HH:MM:SS.sss'
    so they can be compared with expected string values.
    """
    for row in rows:
        for key, value in row.items():
            if isinstance(value, datetime):
                row[key] = value.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    return rows