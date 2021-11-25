import json


def sanitize_types(value):
    """
    Casts values returned by H3 to native PySpark types.

    This is necessary because PySpark does not natively support
    all the types returned by H3, i.e. Python sets/tuples.
    """

    if isinstance(value, str) or isinstance(value, bool) or isinstance(value, int) or isinstance(value, float):
        return value
    if isinstance(value, set) or isinstance(value, tuple):
        return [sanitize_types(v) for v in value]
    if isinstance(value, list):
        return [sanitize_types(v) for v in value]
    if isinstance(value, dict):
        return {k: sanitize_types(v) for k, v in value.items()}

    return json.dumps(value)
