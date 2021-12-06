import h3
from pyspark.sql import functions as F, types as T
import json
from inspect import getmembers, isfunction
from .utils import sanitize_types
import sys
from shapely import geometry


###############################################################################
# Indexing
###############################################################################


@F.udf(returnType=T.StringType())
def geo_to_h3(lat, lng, resolution):
    return sanitize_types(h3.geo_to_h3(lat, lng, resolution))


@F.udf(returnType=T.ArrayType(T.DoubleType()))
def h3_to_geo(h):
    return sanitize_types(h3.h3_to_geo(h))


@F.udf(returnType=T.StringType())
def h3_to_geo_boundary(h, geo_json):
    # NOTE: this behavior differs from default
    # h3-pyspark return type will be a valid GeoJSON string if geo_json is set to True
    coordinates = h3.h3_to_geo_boundary(h, geo_json)
    if geo_json:
        return sanitize_types(json.dumps({"type": "MultiPolygon", "coordinates": coordinates}))
    return sanitize_types(coordinates)


###############################################################################
# Inspection
###############################################################################


@F.udf(returnType=T.IntegerType())
def h3_get_resolution(h):
    return sanitize_types(h3.h3_get_resolution(h))


@F.udf(returnType=T.IntegerType())
def h3_get_base_cell(h):
    return sanitize_types(h3.h3_get_base_cell(h))


@F.udf(returnType=T.LongType())
def string_to_h3(h):
    return sanitize_types(h3.string_to_h3(h))


@F.udf(returnType=T.StringType())
def h3_to_string(h):
    return sanitize_types(h3.h3_to_string(h))


@F.udf(returnType=T.BooleanType())
def h3_is_valid(h):
    return sanitize_types(h3.h3_is_valid(h))


@F.udf(returnType=T.BooleanType())
def h3_is_res_class_III(h):
    return sanitize_types(h3.h3_is_res_class_III(h))


@F.udf(returnType=T.BooleanType())
def h3_is_pentagon(h):
    return sanitize_types(h3.h3_is_pentagon(h))


@F.udf(returnType=T.ArrayType(T.IntegerType()))
def h3_get_faces(h):
    return sanitize_types(h3.h3_get_faces(h))


###############################################################################
# Traversal
###############################################################################


@F.udf(returnType=T.ArrayType(T.StringType()))
def k_ring(origin, k):
    return sanitize_types(h3.k_ring(origin, k))


@F.udf(returnType=T.ArrayType(T.ArrayType(T.StringType())))
def k_ring_distances(origin, k):
    return sanitize_types(h3.k_ring_distances(origin, k))


@F.udf(returnType=T.ArrayType(T.StringType()))
def hex_range(h, k):
    return sanitize_types(h3.hex_range(h, k))


@F.udf(returnType=T.ArrayType(T.ArrayType(T.StringType())))
def hex_range_distances(h, k):
    return sanitize_types(h3.hex_range_distances(h, k))


@F.udf(returnType=T.MapType(T.StringType(), T.ArrayType(T.ArrayType(T.StringType()))))
def hex_ranges(h, k):
    return sanitize_types(h3.hex_ranges(h, k))


@F.udf(returnType=T.ArrayType(T.StringType()))
def hex_ring(h, k):
    return sanitize_types(h3.hex_ring(h, k))


@F.udf(returnType=T.ArrayType(T.StringType()))
def h3_line(start, end):
    return sanitize_types(h3.h3_line(start, end))


@F.udf(returnType=T.IntegerType())
def h3_distance(h1, h2):
    return sanitize_types(h3.h3_distance(h1, h2))


@F.udf(returnType=T.ArrayType(T.IntegerType()))
def experimental_h3_to_local_ij(origin, h):
    return sanitize_types(h3.experimental_h3_to_local_ij(origin, h))


@F.udf(returnType=T.StringType())
def experimental_local_ij_to_h3(origin, i, j):
    return sanitize_types(h3.experimental_local_ij_to_h3(origin, i, j))


###############################################################################
# Hierarchy
###############################################################################


@F.udf(returnType=T.StringType())
def h3_to_parent(h, parent_res):
    return sanitize_types(h3.h3_to_parent(h, parent_res))


@F.udf(returnType=T.ArrayType(T.StringType()))
def h3_to_children(h, child_res):
    return sanitize_types(h3.h3_to_children(h, child_res))


@F.udf(returnType=T.StringType())
def h3_to_center_child(h, child_res):
    return sanitize_types(h3.h3_to_center_child(h, child_res))


@F.udf(returnType=T.ArrayType(T.StringType()))
def compact(hexes):
    return sanitize_types(h3.compact(hexes))


@F.udf(returnType=T.ArrayType(T.StringType()))
def uncompact(hexes, res):
    return sanitize_types(h3.uncompact(hexes, res))


###############################################################################
# Regions
###############################################################################


@F.udf(returnType=T.ArrayType(T.StringType()))
def polyfill(polygons, res, geo_json_conformant):
    # NOTE: this behavior differs from default
    # h3-pyspark expect `polygons` argument to be a valid GeoJSON string
    polygons = json.loads(polygons)
    return sanitize_types(h3.polyfill(polygons, res, geo_json_conformant))


@F.udf(returnType=T.StringType())
def h3_set_to_multi_polygon(hexes, geo_json):
    # NOTE: this behavior differs from default
    # h3-pyspark return type will be a valid GeoJSON string if geo_json is set to True
    coordinates = h3.h3_set_to_multi_polygon(hexes, geo_json)
    if geo_json:
        return sanitize_types(json.dumps({"type": "MultiPolygon", "coordinates": coordinates}))
    return sanitize_types(coordinates)


###############################################################################
# Unidirectional Edges
###############################################################################


@F.udf(returnType=T.BooleanType())
def h3_indexes_are_neighbors(origin, destination):
    return sanitize_types(h3.h3_indexes_are_neighbors(origin, destination))


@F.udf(returnType=T.StringType())
def get_h3_unidirectional_edge(origin, destination):
    return sanitize_types(h3.get_h3_unidirectional_edge(origin, destination))


@F.udf(returnType=T.BooleanType())
def h3_unidirectional_edge_is_valid(edge):
    return sanitize_types(h3.h3_unidirectional_edge_is_valid(edge))


@F.udf(returnType=T.StringType())
def get_origin_h3_index_from_unidirectional_edge(edge):
    return sanitize_types(h3.get_origin_h3_index_from_unidirectional_edge(edge))


@F.udf(returnType=T.StringType())
def get_destination_h3_index_from_unidirectional_edge(edge):
    return sanitize_types(h3.get_destination_h3_index_from_unidirectional_edge(edge))


@F.udf(returnType=T.ArrayType(T.StringType()))
def get_h3_indexes_from_unidirectional_edge(edge):
    return sanitize_types(h3.get_h3_indexes_from_unidirectional_edge(edge))


@F.udf(returnType=T.ArrayType(T.StringType()))
def get_h3_unidirectional_edges_from_hexagon(h):
    return sanitize_types(h3.get_h3_unidirectional_edges_from_hexagon(h))


@F.udf(returnType=T.ArrayType(T.ArrayType(T.DoubleType())))
def get_h3_unidirectional_edge_boundary(h, geo_json):
    return sanitize_types(h3.get_h3_unidirectional_edge_boundary(h, geo_json))


###############################################################################
# Miscellaneous
###############################################################################


@F.udf(returnType=T.DoubleType())
def hex_area(res, unit):
    return sanitize_types(h3.hex_area(res, unit))


@F.udf(returnType=T.DoubleType())
def cell_area(h, unit):
    return sanitize_types(h3.cell_area(h, unit))


@F.udf(returnType=T.DoubleType())
def edge_length(res, unit):
    return sanitize_types(h3.edge_length(res, unit))


@F.udf(returnType=T.DoubleType())
def exact_edge_length(res, unit):
    return sanitize_types(h3.exact_edge_length(res, unit))


@F.udf(returnType=T.IntegerType())
def num_hexagons(res):
    return sanitize_types(h3.num_hexagons(res))


@F.udf(returnType=T.ArrayType(T.StringType()))
def get_res0_indexes():
    return sanitize_types(h3.get_res0_indexes())


@F.udf(returnType=T.ArrayType(T.StringType()))
def get_pentagon_indexes(res):
    return sanitize_types(h3.get_pentagon_indexes(res))


@F.udf(returnType=T.DoubleType())
def point_dist(point1, point2, unit):
    return sanitize_types(h3.point_dist(point1, point2, unit))


# Steal docstrings from h3-py native bindings if they exist
for f in [f[1] for f in getmembers(sys.modules[__name__], isfunction)]:
    try:
        h3_f = getattr(h3, f.__name__)
        f.__doc__ = h3_f.__doc__
    except Exception:
        f.__doc__ = f.__doc__
