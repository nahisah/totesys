from datetime import datetime


def normalise_datetimes(rows):
    """
    Iterates through a list of dictionaries and converts datetime objects to string in the format 'YYYY-MM-DD HH:MM:SS.sss' so they can be compared with expected string values.

    # Arguments:
        rows: A list of dictionaries; each dictionary represents a data record.

    # Return:
        A list of dictionaries where the datetime values are formatted as strings.

    """
    for row in rows:
        for key, value in row.items():
            if isinstance(value, datetime):
                row[key] = value.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    return rows
