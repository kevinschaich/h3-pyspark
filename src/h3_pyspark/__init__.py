import h3
from pyspark.sql import functions as F, types as T
import json
from inspect import getmembers, isfunction


def sanitize_types(value):
    """
    PySpark does not support all the native types in Python, i.e. sets/tuples.
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


###############################################################################
# Indexing
###############################################################################


@F.udf(returnType=T.StringType())
def geo_to_h3(*args, **kwargs):
    return sanitize_types(h3.geo_to_h3(*args, **kwargs))


@F.udf(returnType=T.ArrayType(T.DoubleType()))
def h3_to_geo(*args, **kwargs):
    return sanitize_types(h3.h3_to_geo(*args, **kwargs))


@F.udf(returnType=T.ArrayType(T.ArrayType(T.DoubleType())))
def h3_to_geo_boundary(*args, **kwargs):
    return sanitize_types(h3.h3_to_geo_boundary(*args, **kwargs))


###############################################################################
# Inspection
###############################################################################


@F.udf(returnType=T.IntegerType())
def h3_get_resolution(*args, **kwargs):
    return sanitize_types(h3.h3_get_resolution(*args, **kwargs))


@F.udf(returnType=T.IntegerType())
def h3_get_base_cell(*args, **kwargs):
    return sanitize_types(h3.h3_get_base_cell(*args, **kwargs))


@F.udf(returnType=T.LongType())
def string_to_h3(*args, **kwargs):
    return sanitize_types(h3.string_to_h3(*args, **kwargs))


@F.udf(returnType=T.StringType())
def h3_to_string(*args, **kwargs):
    return sanitize_types(h3.h3_to_string(*args, **kwargs))


@F.udf(returnType=T.BooleanType())
def h3_is_valid(*args, **kwargs):
    return sanitize_types(h3.h3_is_valid(*args, **kwargs))


@F.udf(returnType=T.BooleanType())
def h3_is_res_class_III(*args, **kwargs):
    return sanitize_types(h3.h3_is_res_class_III(*args, **kwargs))


@F.udf(returnType=T.BooleanType())
def h3_is_pentagon(*args, **kwargs):
    return sanitize_types(h3.h3_is_pentagon(*args, **kwargs))


@F.udf(returnType=T.ArrayType(T.IntegerType()))
def h3_get_faces(*args, **kwargs):
    return sanitize_types(h3.h3_get_faces(*args, **kwargs))


###############################################################################
# Traversal
###############################################################################


@F.udf(returnType=T.ArrayType(T.StringType()))
def k_ring(*args, **kwargs):
    return sanitize_types(h3.k_ring(*args, **kwargs))


@F.udf(returnType=T.ArrayType(T.ArrayType(T.StringType())))
def k_ring_distances(*args, **kwargs):
    return sanitize_types(h3.k_ring_distances(*args, **kwargs))


@F.udf(returnType=T.ArrayType(T.StringType()))
def hex_range(*args, **kwargs):
    return sanitize_types(h3.hex_range(*args, **kwargs))


@F.udf(returnType=T.ArrayType(T.ArrayType(T.StringType())))
def hex_range_distances(*args, **kwargs):
    return sanitize_types(h3.hex_range_distances(*args, **kwargs))


@F.udf(returnType=T.MapType(T.StringType(), T.ArrayType(T.ArrayType(T.StringType()))))
def hex_ranges(*args, **kwargs):
    return sanitize_types(h3.hex_ranges(*args, **kwargs))


@F.udf(returnType=T.ArrayType(T.StringType()))
def hex_ring(*args, **kwargs):
    return sanitize_types(h3.hex_ring(*args, **kwargs))


@F.udf(returnType=T.ArrayType(T.StringType()))
def h3_line(*args, **kwargs):
    return sanitize_types(h3.h3_line(*args, **kwargs))


@F.udf(returnType=T.IntegerType())
def h3_distance(*args, **kwargs):
    return sanitize_types(h3.h3_distance(*args, **kwargs))


@F.udf(returnType=T.ArrayType(T.IntegerType()))
def experimental_h3_to_local_ij(*args, **kwargs):
    return sanitize_types(h3.experimental_h3_to_local_ij(*args, **kwargs))


@F.udf(returnType=T.StringType())
def experimental_local_ij_to_h3(*args, **kwargs):
    return sanitize_types(h3.experimental_local_ij_to_h3(*args, **kwargs))


###############################################################################
# Hierarchy
###############################################################################


@F.udf(returnType=T.StringType())
def h3_to_parent(*args, **kwargs):
    return sanitize_types(h3.h3_to_parent(*args, **kwargs))


@F.udf(returnType=T.ArrayType(T.StringType()))
def h3_to_children(*args, **kwargs):
    return sanitize_types(h3.h3_to_children(*args, **kwargs))


@F.udf(returnType=T.StringType())
def h3_to_center_child(*args, **kwargs):
    return sanitize_types(h3.h3_to_center_child(*args, **kwargs))


@F.udf(returnType=T.ArrayType(T.StringType()))
def compact(*args, **kwargs):
    return sanitize_types(h3.compact(*args, **kwargs))


@F.udf(returnType=T.ArrayType(T.StringType()))
def uncompact(*args, **kwargs):
    return sanitize_types(h3.uncompact(*args, **kwargs))


###############################################################################
# Regions
###############################################################################


@F.udf(returnType=T.ArrayType(T.StringType()))
def polyfill(polygons, res, geo_json_conformant=False):
    # NOTE: this behavior differs from default
    # h3-pyspark expect `polygons` argument to be a valid GeoJSON string
    polygons = json.loads(polygons)
    return sanitize_types(h3.polyfill(polygons, res, geo_json_conformant))


@F.udf(returnType=T.ArrayType(T.ArrayType(T.ArrayType(T.ArrayType(T.DoubleType())))))
def h3_set_to_multi_polygon(*args, **kwargs):
    return sanitize_types(h3.h3_set_to_multi_polygon(*args, **kwargs))


###############################################################################
# Unidirectional Edges
###############################################################################


@F.udf(returnType=T.BooleanType())
def h3_indexes_are_neighbors(*args, **kwargs):
    return sanitize_types(h3.h3_indexes_are_neighbors(*args, **kwargs))


@F.udf(returnType=T.StringType())
def get_h3_unidirectional_edge(*args, **kwargs):
    return sanitize_types(h3.get_h3_unidirectional_edge(*args, **kwargs))


@F.udf(returnType=T.BooleanType())
def h3_unidirectional_edge_is_valid(*args, **kwargs):
    return sanitize_types(h3.h3_unidirectional_edge_is_valid(*args, **kwargs))


@F.udf(returnType=T.StringType())
def get_origin_h3_index_from_unidirectional_edge(*args, **kwargs):
    return sanitize_types(h3.get_origin_h3_index_from_unidirectional_edge(*args, **kwargs))


@F.udf(returnType=T.StringType())
def get_destination_h3_index_from_unidirectional_edge(*args, **kwargs):
    return sanitize_types(h3.get_destination_h3_index_from_unidirectional_edge(*args, **kwargs))


@F.udf(returnType=T.ArrayType(T.StringType()))
def get_h3_indexes_from_unidirectional_edge(*args, **kwargs):
    return sanitize_types(h3.get_h3_indexes_from_unidirectional_edge(*args, **kwargs))


@F.udf(returnType=T.ArrayType(T.StringType()))
def get_h3_unidirectional_edges_from_hexagon(*args, **kwargs):
    return sanitize_types(h3.get_h3_unidirectional_edges_from_hexagon(*args, **kwargs))


@F.udf(returnType=T.ArrayType(T.ArrayType(T.DoubleType())))
def get_h3_unidirectional_edge_boundary(*args, **kwargs):
    return sanitize_types(h3.get_h3_unidirectional_edge_boundary(*args, **kwargs))


###############################################################################
# Miscellaneous
###############################################################################


@F.udf(returnType=T.DoubleType())
def hex_area(*args, **kwargs):
    return sanitize_types(h3.hex_area(*args, **kwargs))


@F.udf(returnType=T.DoubleType())
def cell_area(*args, **kwargs):
    return sanitize_types(h3.cell_area(*args, **kwargs))


@F.udf(returnType=T.DoubleType())
def edge_length(*args, **kwargs):
    return sanitize_types(h3.edge_length(*args, **kwargs))


@F.udf(returnType=T.DoubleType())
def exact_edge_length(*args, **kwargs):
    return sanitize_types(h3.exact_edge_length(*args, **kwargs))


@F.udf(returnType=T.IntegerType())
def num_hexagons(*args, **kwargs):
    return sanitize_types(h3.num_hexagons(*args, **kwargs))


@F.udf(returnType=T.ArrayType(T.StringType()))
def get_res0_indexes(*args, **kwargs):
    return sanitize_types(h3.get_res0_indexes(*args, **kwargs))


@F.udf(returnType=T.ArrayType(T.StringType()))
def get_pentagon_indexes(*args, **kwargs):
    return sanitize_types(h3.get_pentagon_indexes(*args, **kwargs))


@F.udf(returnType=T.DoubleType())
def point_dist(*args, **kwargs):
    return sanitize_types(h3.point_dist(*args, **kwargs))


functions = getmembers(__package__, isfunction)
print(functions)
for f in functions:
    f.__doc__ = """My Doc string"""
