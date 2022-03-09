import json
import math
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
from .utils import flatten, densify, handle_nulls


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

    # Hexes for vertices
    vertex_hexes = [h3.geo_to_h3(t[1], t[0], resolution) for t in list(line.coords)]
    result_set.update(vertex_hexes)

    # Figure out the max-length line segment (step) we can process without interpolating
    # https://github.com/kevinschaich/h3-pyspark/issues/8
    endpoint_hex_edges = flatten(
        [h3.get_h3_unidirectional_edges_from_hexagon(h) for h in [vertex_hexes[0], vertex_hexes[1]]]
    )
    step = math.degrees(min([h3.exact_edge_length(e, unit="rads") for e in endpoint_hex_edges]))

    densified_line = densify(line, step)
    line_hexes = [h3.geo_to_h3(t[1], t[0], resolution) for t in list(densified_line.coords)]
    result_set.update(line_hexes)

    neighboring_hexes = set(flatten([h3.k_ring(h, 1) for h in result_set])) - result_set
    intersecting_neighboring_hexes = filter(
        lambda h: Polygon(h3.h3_set_to_multi_polygon([h], True)[0][0]).distance(line) == 0, neighboring_hexes
    )
    result_set.update(intersecting_neighboring_hexes)

    return result_set


def _index_polygon_object(polygon: Polygon, resolution: int):
    """
    Generate H3 spatial index for input polygon geometry.

    Returns the set of H3 cells at the specified resolution which completely cover the input polygon.
    """
    result_set = set()
    # Hexes for vertices
    vertex_hexes = [h3.geo_to_h3(t[1], t[0], resolution) for t in list(polygon.exterior.coords)]
    result_set.update(vertex_hexes)

    # Hexes for edges
    edge_hexes = _index_shape_object(polygon.boundary, resolution)
    result_set.update(edge_hexes)

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
@handle_nulls
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
