from inspect import getfullargspec
from pyspark.sql import SparkSession, functions as F, types as T
import h3
import json
import unittest

from src import h3_pyspark
from src.h3_pyspark.utils import sanitize_types


spark = SparkSession.builder.getOrCreate()


# Generate some arbitrary test values
latitude = 29.8988
longitude = -89.998354
integer = 1
double = 0.5
point = '{"type": "Point", "coordinates": [-89.998354, 29.8988]}'
line = '{"type": "LineString", "coordinates": [[-89.99927146300001, 29.90139583899997], [-89.99921418299999, 29.90139420899999], [-89.99903129900002, 29.90138951699998], [-89.99900807, 29.90142210300002], [-89.99898608000001, 29.90138835699997], [-89.99875118300002, 29.90138410499998], [-89.99872961, 29.90141686999999], [-89.99871085699999, 29.90138346399999], [-89.99837947499998, 29.90137720600001], [-89.99835869700001, 29.90140975100002], [-89.99834035200001, 29.901376191], [-89.998234115, 29.90137350700002], [-89.998218017, 29.90137313499997], [-89.99819830400003, 29.90137344499999], [-89.99787396300002, 29.90139402699998], [-89.99785696700002, 29.90142557899998], [-89.99783514199999, 29.90139429700002]]}'
polygon = '{"type": "Polygon", "coordinates": [[[-89.998354, 29.8988], [-89.99807, 29.8988], [-89.99807, 29.898628], [-89.998354, 29.898628], [-89.998354, 29.8988]]]}'
h3_cell = "81447ffffffffff"
h3_cells = ["81447ffffffffff", "81267ffffffffff", "8148bffffffffff", "81483ffffffffff"]
h3_edge = "131447ffffffffff"
unit = "km^2"


# Generate a dataframe from arbitrary test values (mapping function parameters to appropriate type)
test_arg_map = {
    "i": integer,
    "j": integer,
    "k": integer,
    "x": integer,
    "resolution": integer,
    "res": integer,
    "lat": latitude,
    "lng": longitude,
    "point1": (latitude, longitude),
    "point2": (latitude, longitude),
    "h": h3_cells[0],
    "hexes": h3_cells,
    "h1": h3_cells[1],
    "h2": h3_cells[2],
    "origin": h3_cells[2],
    "destination": h3_cells[3],
    "start": h3_cells[1],
    "end": h3_cells[2],
    "e": h3_edge,
    "edge": h3_edge,
    "geo_json": True,
    "geo_json_conformant": True,
    "geojson": polygon,
}
df = spark.createDataFrame([test_arg_map])


def get_test_args(function):
    argspec = getfullargspec(function)
    args = argspec.args
    h3_test_args = [test_arg_map.get(a.lower()) for a in args]
    h3_pyspark_test_args = [F.col(a) for a in args]

    return h3_test_args, h3_pyspark_test_args


def sort(value):
    if isinstance(value, str) or isinstance(value, bool) or isinstance(value, int) or isinstance(value, float):
        return value
    if isinstance(value, list):
        value = [sort(v) for v in value]
        value.sort()
        return value
    if isinstance(value, set) or isinstance(value, tuple):
        return [sort(v) for v in value]
    if isinstance(value, dict):
        return {k: sort(v) for k, v in value.items()}

    return json.dumps(value)


class TestCore(unittest.TestCase):

    ###############################################################################
    # Indexing
    ###############################################################################

    def test_geo_to_h3(self):
        h3_test_args, h3_pyspark_test_args = get_test_args(h3.geo_to_h3)

        actual = df.withColumn("actual", h3_pyspark.geo_to_h3(*h3_pyspark_test_args))
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types(h3.geo_to_h3(*h3_test_args))
        assert sort(actual) == sort(expected)

    def test_h3_to_geo(self):
        h3_test_args, h3_pyspark_test_args = get_test_args(h3.h3_to_geo)

        actual = df.withColumn("actual", h3_pyspark.h3_to_geo(*h3_pyspark_test_args))
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types(h3.h3_to_geo(*h3_test_args))
        assert sort(actual) == sort(expected)

    def test_h3_to_geo_boundary(self):
        h3_test_args, h3_pyspark_test_args = get_test_args(h3.h3_to_geo_boundary)

        actual = df.withColumn("actual", h3_pyspark.h3_to_geo_boundary(*h3_pyspark_test_args))
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types(h3.h3_to_geo_boundary(*h3_test_args))
        assert sort(actual) == sort(expected)

    ###############################################################################
    # Inspection
    ###############################################################################

    def test_h3_get_resolution(self):
        h3_test_args, h3_pyspark_test_args = get_test_args(h3.h3_get_resolution)

        actual = df.withColumn("actual", h3_pyspark.h3_get_resolution(*h3_pyspark_test_args))
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types(h3.h3_get_resolution(*h3_test_args))
        assert sort(actual) == sort(expected)

    def test_h3_get_base_cell(self):
        h3_test_args, h3_pyspark_test_args = get_test_args(h3.h3_get_base_cell)

        actual = df.withColumn("actual", h3_pyspark.h3_get_base_cell(*h3_pyspark_test_args))
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types(h3.h3_get_base_cell(*h3_test_args))
        assert sort(actual) == sort(expected)

    def test_string_to_h3(self):
        h3_test_args, h3_pyspark_test_args = get_test_args(h3.string_to_h3)

        actual = df.withColumn("actual", h3_pyspark.string_to_h3(*h3_pyspark_test_args))
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types(h3.string_to_h3(*h3_test_args))
        assert sort(actual) == sort(expected)

    def test_h3_to_string(self):
        h3_test_args, h3_pyspark_test_args = get_test_args(h3.h3_to_string)

        actual = df.withColumn("actual", h3_pyspark.h3_to_string(*h3_pyspark_test_args))
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types(h3.h3_to_string(*h3_test_args))
        assert sort(actual) == sort(expected)

    def test_h3_is_valid(self):
        h3_test_args, h3_pyspark_test_args = get_test_args(h3.h3_is_valid)

        actual = df.withColumn("actual", h3_pyspark.h3_is_valid(*h3_pyspark_test_args))
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types(h3.h3_is_valid(*h3_test_args))
        assert sort(actual) == sort(expected)

    def test_h3_is_res_class_III(self):
        h3_test_args, h3_pyspark_test_args = get_test_args(h3.h3_is_res_class_III)

        actual = df.withColumn("actual", h3_pyspark.h3_is_res_class_III(*h3_pyspark_test_args))
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types(h3.h3_is_res_class_III(*h3_test_args))
        assert sort(actual) == sort(expected)

    def test_h3_is_pentagon(self):
        h3_test_args, h3_pyspark_test_args = get_test_args(h3.h3_is_pentagon)

        actual = df.withColumn("actual", h3_pyspark.h3_is_pentagon(*h3_pyspark_test_args))
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types(h3.h3_is_pentagon(*h3_test_args))
        assert sort(actual) == sort(expected)

    def test_h3_get_faces(self):
        h3_test_args, h3_pyspark_test_args = get_test_args(h3.h3_get_faces)

        actual = df.withColumn("actual", h3_pyspark.h3_get_faces(*h3_pyspark_test_args))
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types(h3.h3_get_faces(*h3_test_args))
        assert sort(actual) == sort(expected)

    ###############################################################################
    # Traversal
    ###############################################################################

    def test_k_ring(self):
        h3_test_args, h3_pyspark_test_args = get_test_args(h3.k_ring)

        actual = df.withColumn("actual", h3_pyspark.k_ring(*h3_pyspark_test_args))
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types(h3.k_ring(*h3_test_args))
        assert sort(actual) == sort(expected)

    def test_k_ring_distances(self):
        h3_test_args, h3_pyspark_test_args = get_test_args(h3.k_ring_distances)

        actual = df.withColumn("actual", h3_pyspark.k_ring_distances(*h3_pyspark_test_args))
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types(h3.k_ring_distances(*h3_test_args))
        assert sort(actual) == sort(expected)

    def test_hex_range(self):
        h3_test_args, h3_pyspark_test_args = get_test_args(h3.hex_range)

        actual = df.withColumn("actual", h3_pyspark.hex_range(*h3_pyspark_test_args))
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types(h3.hex_range(*h3_test_args))
        assert sort(actual) == sort(expected)

    def test_hex_range_distances(self):
        h3_test_args, h3_pyspark_test_args = get_test_args(h3.hex_range_distances)

        actual = df.withColumn("actual", h3_pyspark.hex_range_distances(*h3_pyspark_test_args))
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types(h3.hex_range_distances(*h3_test_args))
        assert sort(actual) == sort(expected)

    def test_hex_ranges(self):
        h3_test_args, h3_pyspark_test_args = get_test_args(h3.hex_ranges)

        actual = df.withColumn("actual", h3_pyspark.hex_ranges(*h3_pyspark_test_args))
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types(h3.hex_ranges(*h3_test_args))
        assert sort(actual) == sort(expected)

    def test_hex_ring(self):
        h3_test_args, h3_pyspark_test_args = get_test_args(h3.hex_ring)

        actual = df.withColumn("actual", h3_pyspark.hex_ring(*h3_pyspark_test_args))
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types(h3.hex_ring(*h3_test_args))
        assert sort(actual) == sort(expected)

    def test_h3_line(self):
        h3_test_args, h3_pyspark_test_args = get_test_args(h3.h3_line)

        actual = df.withColumn("actual", h3_pyspark.h3_line(*h3_pyspark_test_args))
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types(h3.h3_line(*h3_test_args))
        assert sort(actual) == sort(expected)

    def test_h3_distance(self):
        h3_test_args, h3_pyspark_test_args = get_test_args(h3.h3_distance)

        actual = df.withColumn("actual", h3_pyspark.h3_distance(*h3_pyspark_test_args))
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types(h3.h3_distance(*h3_test_args))
        assert sort(actual) == sort(expected)

    def test_experimental_h3_to_local_ij(self):
        h3_test_args, h3_pyspark_test_args = get_test_args(h3.experimental_h3_to_local_ij)

        actual = df.withColumn("actual", h3_pyspark.experimental_h3_to_local_ij(*h3_pyspark_test_args))
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types(h3.experimental_h3_to_local_ij(*h3_test_args))
        assert sort(actual) == sort(expected)

    def test_experimental_local_ij_to_h3(self):
        h3_test_args, h3_pyspark_test_args = get_test_args(h3.experimental_local_ij_to_h3)

        actual = df.withColumn("actual", h3_pyspark.experimental_local_ij_to_h3(*h3_pyspark_test_args))
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types(h3.experimental_local_ij_to_h3(*h3_test_args))
        assert sort(actual) == sort(expected)

    ###############################################################################
    # Hierarchy
    ###############################################################################

    def test_h3_to_parent(self):
        h3_test_args, h3_pyspark_test_args = get_test_args(h3.h3_to_parent)

        actual = df.withColumn("actual", h3_pyspark.h3_to_parent(*h3_pyspark_test_args))
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types(h3.h3_to_parent(*h3_test_args))
        assert sort(actual) == sort(expected)

    def test_h3_to_children(self):
        h3_test_args, h3_pyspark_test_args = get_test_args(h3.h3_to_children)

        actual = df.withColumn("actual", h3_pyspark.h3_to_children(*h3_pyspark_test_args))
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types(h3.h3_to_children(*h3_test_args))
        assert sort(actual) == sort(expected)

    def test_h3_to_center_child(self):
        h3_test_args, h3_pyspark_test_args = get_test_args(h3.h3_to_center_child)

        actual = df.withColumn("actual", h3_pyspark.h3_to_center_child(*h3_pyspark_test_args))
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types(h3.h3_to_center_child(*h3_test_args))
        assert sort(actual) == sort(expected)

    def test_compact(self):
        h3_test_args, h3_pyspark_test_args = get_test_args(h3.compact)

        actual = df.withColumn("actual", h3_pyspark.compact(*h3_pyspark_test_args))
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types(h3.compact(*h3_test_args))
        assert sort(actual) == sort(expected)

    def test_uncompact(self):
        h3_test_args, h3_pyspark_test_args = get_test_args(h3.uncompact)

        actual = df.withColumn("actual", h3_pyspark.uncompact(*h3_pyspark_test_args))
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types(h3.uncompact(*h3_test_args))
        assert sort(actual) == sort(expected)

    ###############################################################################
    # Regions
    ###############################################################################

    def test_polyfill(self):
        h3_test_args, h3_pyspark_test_args = get_test_args(h3.polyfill)

        actual = df.withColumn("actual", h3_pyspark.polyfill(*h3_pyspark_test_args))
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types(h3.polyfill(json.loads(polygon), integer, True))
        assert sort(actual) == sort(expected)

    def test_h3_set_to_multi_polygon(self):
        h3_test_args, h3_pyspark_test_args = get_test_args(h3.h3_set_to_multi_polygon)

        actual = df.withColumn("actual", h3_pyspark.h3_set_to_multi_polygon(*h3_pyspark_test_args))
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types(h3.h3_set_to_multi_polygon(*h3_test_args))
        assert sort(actual) == sort(expected)

    ###############################################################################
    # Unidirectional Edges
    ###############################################################################

    def test_h3_indexes_are_neighbors(self):
        h3_test_args, h3_pyspark_test_args = get_test_args(h3.h3_indexes_are_neighbors)

        actual = df.withColumn("actual", h3_pyspark.h3_indexes_are_neighbors(*h3_pyspark_test_args))
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types(h3.h3_indexes_are_neighbors(*h3_test_args))
        assert sort(actual) == sort(expected)

    def test_get_h3_unidirectional_edge(self):
        h3_test_args, h3_pyspark_test_args = get_test_args(h3.get_h3_unidirectional_edge)

        actual = df.withColumn("actual", h3_pyspark.get_h3_unidirectional_edge(*h3_pyspark_test_args))
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types(h3.get_h3_unidirectional_edge(*h3_test_args))
        assert sort(actual) == sort(expected)

    def test_h3_unidirectional_edge_is_valid(self):
        h3_test_args, h3_pyspark_test_args = get_test_args(h3.h3_unidirectional_edge_is_valid)

        actual = df.withColumn("actual", h3_pyspark.h3_unidirectional_edge_is_valid(*h3_pyspark_test_args))
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types(h3.h3_unidirectional_edge_is_valid(*h3_test_args))
        assert sort(actual) == sort(expected)

    def test_get_origin_h3_index_from_unidirectional_edge(self):
        h3_test_args, h3_pyspark_test_args = get_test_args(h3.get_origin_h3_index_from_unidirectional_edge)

        actual = df.withColumn(
            "actual",
            h3_pyspark.get_origin_h3_index_from_unidirectional_edge(*h3_pyspark_test_args),
        )
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types(h3.get_origin_h3_index_from_unidirectional_edge(*h3_test_args))
        assert sort(actual) == sort(expected)

    def test_get_destination_h3_index_from_unidirectional_edge(self):
        h3_test_args, h3_pyspark_test_args = get_test_args(h3.get_destination_h3_index_from_unidirectional_edge)

        actual = df.withColumn(
            "actual",
            h3_pyspark.get_destination_h3_index_from_unidirectional_edge(*h3_pyspark_test_args),
        )
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types(h3.get_destination_h3_index_from_unidirectional_edge(*h3_test_args))
        assert sort(actual) == sort(expected)

    def test_get_h3_indexes_from_unidirectional_edge(self):
        h3_test_args, h3_pyspark_test_args = get_test_args(h3.get_h3_indexes_from_unidirectional_edge)

        actual = df.withColumn(
            "actual",
            h3_pyspark.get_h3_indexes_from_unidirectional_edge(*h3_pyspark_test_args),
        )
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types(h3.get_h3_indexes_from_unidirectional_edge(*h3_test_args))
        assert sort(actual) == sort(expected)

    def test_get_h3_unidirectional_edges_from_hexagon(self):
        h3_test_args, h3_pyspark_test_args = get_test_args(h3.get_h3_unidirectional_edges_from_hexagon)

        actual = df.withColumn(
            "actual",
            h3_pyspark.get_h3_unidirectional_edges_from_hexagon(*h3_pyspark_test_args),
        )
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types(h3.get_h3_unidirectional_edges_from_hexagon(*h3_test_args))
        assert sort(actual) == sort(expected)

    def test_get_h3_unidirectional_edge_boundary(self):
        h3_test_args, h3_pyspark_test_args = get_test_args(h3.get_h3_unidirectional_edge_boundary)

        actual = df.withColumn(
            "actual",
            h3_pyspark.get_h3_unidirectional_edge_boundary(*h3_pyspark_test_args),
        )
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types(h3.get_h3_unidirectional_edge_boundary(*h3_test_args))
        assert sort(actual) == sort(expected)

    ###############################################################################
    # Miscellaneous
    ###############################################################################

    def test_hex_area(self):
        h3_test_args, h3_pyspark_test_args = get_test_args(h3.hex_area)

        h3_test_args[-1] = "m^2"
        actual = df.withColumn("unit", F.lit("m^2"))

        actual = actual.withColumn("actual", h3_pyspark.hex_area(*h3_pyspark_test_args))
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types(h3.hex_area(*h3_test_args))
        assert sort(actual) == sort(expected)

    def test_cell_area(self):
        h3_test_args, h3_pyspark_test_args = get_test_args(h3.cell_area)

        h3_test_args[-1] = "m^2"
        actual = df.withColumn("unit", F.lit("m^2"))

        actual = actual.withColumn("actual", h3_pyspark.cell_area(*h3_pyspark_test_args))
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types(h3.cell_area(*h3_test_args))
        assert sort(actual) == sort(expected)

    def test_edge_length(self):
        h3_test_args, h3_pyspark_test_args = get_test_args(h3.edge_length)

        h3_test_args[-1] = "m"
        actual = df.withColumn("unit", F.lit("m"))

        actual = actual.withColumn("actual", h3_pyspark.edge_length(*h3_pyspark_test_args))
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types(h3.edge_length(*h3_test_args))
        assert sort(actual) == sort(expected)

    def test_exact_edge_length(self):
        h3_test_args, h3_pyspark_test_args = get_test_args(h3.exact_edge_length)

        h3_test_args[-1] = "m"
        actual = df.withColumn("unit", F.lit("m"))

        actual = actual.withColumn("actual", h3_pyspark.exact_edge_length(*h3_pyspark_test_args))
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types(h3.exact_edge_length(*h3_test_args))
        assert sort(actual) == sort(expected)

    def test_num_hexagons(self):
        h3_test_args, h3_pyspark_test_args = get_test_args(h3.num_hexagons)

        actual = df.withColumn("actual", h3_pyspark.num_hexagons(*h3_pyspark_test_args))
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types(h3.num_hexagons(*h3_test_args))
        assert sort(actual) == sort(expected)

    def test_get_res0_indexes(self):
        h3_test_args, h3_pyspark_test_args = get_test_args(h3.get_res0_indexes)

        actual = df.withColumn("actual", h3_pyspark.get_res0_indexes(*h3_pyspark_test_args))
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types(h3.get_res0_indexes(*h3_test_args))
        assert sort(actual) == sort(expected)

    def test_get_pentagon_indexes(self):
        h3_test_args, h3_pyspark_test_args = get_test_args(h3.get_pentagon_indexes)

        actual = df.withColumn("actual", h3_pyspark.get_pentagon_indexes(*h3_pyspark_test_args))
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types(h3.get_pentagon_indexes(*h3_test_args))
        assert sort(actual) == sort(expected)

    def test_point_dist(self):
        h3_test_args, h3_pyspark_test_args = get_test_args(h3.point_dist)

        h3_test_args[-1] = "m"
        actual = df.withColumn("unit", F.lit("m"))

        actual = actual.withColumn("actual", h3_pyspark.point_dist(*h3_pyspark_test_args))
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types(h3.point_dist(*h3_test_args))
        assert sort(actual) == sort(expected)


if __name__ == "__main__":
    unittest.main()
