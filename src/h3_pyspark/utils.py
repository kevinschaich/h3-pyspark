import json
from shapely.geometry import LineString


def handle_nulls(function):
    """
    Decorator to return null if any of the input arguments are null.
    """

    def inner(*args, **kwargs):
        if any(arg is None for arg in args):
            return None
        return function(*args, **kwargs)

    return inner


def flatten(t):
    return [item for sublist in t for item in sublist]


def densify(line, step):
    """
    Given a line segment, return another line segment with the same start & endpoints,
    and equally spaced sub-points based on `step` size.

    All the points on the new line are guaranteed to intersect with the original line,
    and the first and last points will be the same.
    """

    if line.length < step:
        return line

    length = line.length
    current_distance = step
    new_points = []

    # take actual first point
    new_points.append(line.interpolate(0.0, normalized=True))

    # add points between endpoints by step size
    while current_distance < length:
        new_points.append(line.interpolate(current_distance))
        current_distance += step

    # take actual last point
    new_points.append(line.interpolate(1.0, normalized=True))

    return LineString(new_points)


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
