import json
import h3
from pyspark.sql.column import Column
from shapely import geometry
from shapely.geometry import (
    Point,
    MultiPoint,
    LineString,
    MultiLineString,
    Polygon,
    MultiPolygon,
)
from pyspark.sql import functions as F, types as T


def _index_point_object(point: Point, resolution: int):
    """
    Generate H3 spatial index for input point geometry.

    Returns the set of H3 cells at the specified resolution which completely cover the input point.
    """
    result_set = set()

    # Hexes for point
    result_set.update(h3.geo_to_h3(t[1], t[0], resolution) for t in list(point.coords))
    return result_set


def _index_line_object(line: LineString, resolution: int):
    """
    Generate H3 spatial index for input line geometry.

    Returns the set of H3 cells at the specified resolution which completely cover the input line.
    """
    result_set = set()
    # Hexes for endpoints
    endpoint_hexes = [h3.geo_to_h3(t[1], t[0], resolution) for t in list(line.coords)]
    # Hexes for line (inclusive of endpoints)
    for i in range(len(endpoint_hexes) - 1):
        result_set.update(h3.h3_line(endpoint_hexes[i], endpoint_hexes[i + 1]))
    return result_set


def _index_polygon_object(polygon: Polygon, resolution: int):
    """
    Generate H3 spatial index for input polygon geometry.

    Returns the set of H3 cells at the specified resolution which completely cover the input polygon.
    """
    result_set = set()
    # Hexes for vertices
    vertex_hexes = [h3.geo_to_h3(t[1], t[0], resolution) for t in list(polygon.exterior.coords)]
    # Hexes for edges (inclusive of vertices)
    for i in range(len(vertex_hexes) - 1):
        result_set.update(h3.h3_line(vertex_hexes[i], vertex_hexes[i + 1]))
    # Hexes for internal area
    result_set.update(list(h3.polyfill(geometry.mapping(polygon), resolution, geo_json_conformant=True)))
    return result_set


def _index_shape_object(shape: geometry, resolution: int):
    """
    Generate H3 spatial index for input geometry.

    Returns the set of H3 cells at the specified resolution which completely cover the input shape.
    """
    result_set = set()

    try:
        if isinstance(shape, Point):
            result_set.update(_index_point_object(shape, resolution))

        elif isinstance(shape, LineString):
            result_set.update(_index_line_object(shape, resolution))

        elif isinstance(shape, Polygon):
            result_set.update(_index_polygon_object(shape, resolution))

        elif isinstance(shape, MultiPoint) or isinstance(shape, MultiLineString) or isinstance(shape, MultiPolygon):
            result_set.update(*[_index_shape_object(s, resolution) for s in shape.geoms])
        else:
            raise ValueError(f"Unsupported geometry_type {shape.geom_type}")

    except Exception as e:
        raise ValueError(
            f"Error finding indices for geometry {json.dumps(geometry.mapping(shape))}",
            repr(e),
        )

    return list(result_set)


def _index_shape(shape: str, resolution: int):
    """
    Generate H3 spatial index for input shape.

    Returns the set of H3 cells at the specified resolution which completely cover the input shape.
    """
    shape = geometry.shape(json.loads(shape))
    return _index_shape_object(shape, resolution)


@F.udf(T.ArrayType(T.StringType()))
def index_shape(geometry: Column, resolution: Column):
    """
    Generate an H3 spatial index for an input GeoJSON geometry column.

    This function accepts GeoJSON `Point`, `LineString`, `Polygon`, `MultiPoint`, `MultiLineString`, and `MultiPolygon`
    input features, and returns the set of H3 cells at the specified resolution which completely cover them
    (could be more than one cell for a substantially large geometry and substantially granular resolution).

    The schema of the output column will be `T.ArrayType(T.StringType())`, where each value in the array is an H3 cell.

    This spatial index can then be used for bucketing, clustering, and joins in Spark via an `explode()` operation.
    """
    return _index_shape(geometry, resolution)
