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
        actual = df.withColumn("actual", h3_pyspark.geo_to_h3(F.col("lat"), F.col("lng"), F.col("resolution")))
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types(h3.latlng_to_cell(latitude, longitude, integer))
        assert sort(actual) == sort(expected)

    def test_geo_to_h3_single_null_input(self):
        actual = df.withColumn("actual", h3_pyspark.geo_to_h3(F.lit(100), F.lit(None), F.lit(9)))
        actual = actual.collect()[0]["actual"]
        expected = None
        assert actual == expected

    def test_geo_to_h3_all_null_inputs(self):
        actual = df.withColumn("actual", h3_pyspark.geo_to_h3(F.lit(None), F.lit(None), F.lit(None)))
        actual = actual.collect()[0]["actual"]
        expected = None
        assert actual == expected

    def test_h3_to_geo(self):
        actual = df.withColumn("actual", h3_pyspark.h3_to_geo(F.col("h")))
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types(h3.cell_to_latlng(h3_cells[0]))
        assert sort(actual) == sort(expected)

    def test_h3_to_geo_boundary(self):
        actual = df.withColumn("actual", h3_pyspark.h3_to_geo_boundary(F.col("h"), F.col("geo_json")))
        actual = actual.collect()[0]["actual"]
        # Build expected: cell_to_boundary returns (lat, lng) tuples; geo_json=True → lng,lat GeoJSON MultiPolygon
        boundary = h3.cell_to_boundary(h3_cells[0])
        coords = [[lng, lat] for lat, lng in boundary]
        coords.append(coords[0])
        expected = json.dumps({"type": "MultiPolygon", "coordinates": [[coords]]})
        assert sort(actual) == sort(expected)

    ###############################################################################
    # Inspection
    ###############################################################################

    def test_h3_get_resolution(self):
        actual = df.withColumn("actual", h3_pyspark.h3_get_resolution(F.col("h")))
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types(h3.get_resolution(h3_cells[0]))
        assert sort(actual) == sort(expected)

    def test_h3_get_base_cell(self):
        actual = df.withColumn("actual", h3_pyspark.h3_get_base_cell(F.col("h")))
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types(h3.get_base_cell_number(h3_cells[0]))
        assert sort(actual) == sort(expected)

    def test_string_to_h3(self):
        actual = df.withColumn("actual", h3_pyspark.string_to_h3(F.col("h")))
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types(h3.str_to_int(h3_cells[0]))
        assert sort(actual) == sort(expected)

    def test_h3_to_string(self):
        h_int = h3.str_to_int(h3_cells[0])
        df2 = spark.createDataFrame([{"h_int": h_int}])
        actual = df2.withColumn("actual", h3_pyspark.h3_to_string(F.col("h_int")))
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types(h3.int_to_str(h_int))
        assert sort(actual) == sort(expected)

    def test_h3_is_valid(self):
        actual = df.withColumn("actual", h3_pyspark.h3_is_valid(F.col("h")))
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types(h3.is_valid_cell(h3_cells[0]))
        assert sort(actual) == sort(expected)

    def test_h3_is_res_class_III(self):
        actual = df.withColumn("actual", h3_pyspark.h3_is_res_class_III(F.col("h")))
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types(h3.is_res_class_III(h3_cells[0]))
        assert sort(actual) == sort(expected)

    def test_h3_is_pentagon(self):
        actual = df.withColumn("actual", h3_pyspark.h3_is_pentagon(F.col("h")))
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types(h3.is_pentagon(h3_cells[0]))
        assert sort(actual) == sort(expected)

    def test_h3_get_faces(self):
        actual = df.withColumn("actual", h3_pyspark.h3_get_faces(F.col("h")))
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types(h3.get_icosahedron_faces(h3_cells[0]))
        assert sort(actual) == sort(expected)

    ###############################################################################
    # Traversal
    ###############################################################################

    def test_k_ring(self):
        actual = df.withColumn("actual", h3_pyspark.k_ring(F.col("origin"), F.col("k")))
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types(h3.grid_disk(h3_cells[2], integer))
        assert sort(actual) == sort(expected)

    def test_k_ring_distances(self):
        actual = df.withColumn("actual", h3_pyspark.k_ring_distances(F.col("origin"), F.col("k")))
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types([list(h3.grid_ring(h3_cells[2], i)) for i in range(integer + 1)])
        assert sort(actual) == sort(expected)

    def test_hex_range(self):
        actual = df.withColumn("actual", h3_pyspark.hex_range(F.col("h"), F.col("k")))
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types(h3.grid_disk(h3_cells[0], integer))
        assert sort(actual) == sort(expected)

    def test_hex_range_distances(self):
        actual = df.withColumn("actual", h3_pyspark.hex_range_distances(F.col("h"), F.col("k")))
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types([list(h3.grid_ring(h3_cells[0], i)) for i in range(integer + 1)])
        assert sort(actual) == sort(expected)

    def test_hex_ranges(self):
        actual = df.withColumn("actual", h3_pyspark.hex_ranges(F.col("h"), F.col("k")))
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types({h3_cells[0]: [list(h3.grid_ring(h3_cells[0], i)) for i in range(integer + 1)]})
        assert sort(actual) == sort(expected)

    def test_hex_ring(self):
        actual = df.withColumn("actual", h3_pyspark.hex_ring(F.col("h"), F.col("k")))
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types(h3.grid_ring(h3_cells[0], integer))
        assert sort(actual) == sort(expected)

    def test_h3_line(self):
        actual = df.withColumn("actual", h3_pyspark.h3_line(F.col("start"), F.col("end")))
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types(h3.grid_path_cells(h3_cells[1], h3_cells[2]))
        assert sort(actual) == sort(expected)

    def test_h3_distance(self):
        actual = df.withColumn("actual", h3_pyspark.h3_distance(F.col("h1"), F.col("h2")))
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types(h3.grid_distance(h3_cells[1], h3_cells[2]))
        assert sort(actual) == sort(expected)

    def test_experimental_h3_to_local_ij(self):
        actual = df.withColumn("actual", h3_pyspark.experimental_h3_to_local_ij(F.col("origin"), F.col("h")))
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types(h3.cell_to_local_ij(h3_cells[2], h3_cells[0]))
        assert sort(actual) == sort(expected)

    def test_experimental_local_ij_to_h3(self):
        ij = h3.cell_to_local_ij(h3_cells[2], h3_cells[0])
        df2 = spark.createDataFrame([{"origin": h3_cells[2], "i": ij[0], "j": ij[1]}])
        actual = df2.withColumn("actual", h3_pyspark.experimental_local_ij_to_h3(F.col("origin"), F.col("i"), F.col("j")))
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types(h3.local_ij_to_cell(h3_cells[2], ij[0], ij[1]))
        assert sort(actual) == sort(expected)

    ###############################################################################
    # Hierarchy
    ###############################################################################

    def test_h3_to_parent(self):
        actual = df.withColumn("actual", h3_pyspark.h3_to_parent(F.col("h"), F.col("resolution")))
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types(h3.cell_to_parent(h3_cells[0], integer))
        assert sort(actual) == sort(expected)

    def test_h3_to_children(self):
        actual = df.withColumn("actual", h3_pyspark.h3_to_children(F.col("h"), F.col("resolution")))
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types(h3.cell_to_children(h3_cells[0], integer))
        assert sort(actual) == sort(expected)

    def test_h3_to_center_child(self):
        actual = df.withColumn("actual", h3_pyspark.h3_to_center_child(F.col("h"), F.col("resolution")))
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types(h3.cell_to_center_child(h3_cells[0], integer))
        assert sort(actual) == sort(expected)

    def test_compact(self):
        actual = df.withColumn("actual", h3_pyspark.compact(F.col("hexes")))
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types(h3.compact_cells(h3_cells))
        assert sort(actual) == sort(expected)

    def test_uncompact(self):
        actual = df.withColumn("actual", h3_pyspark.uncompact(F.col("hexes"), F.col("res")))
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types(h3.uncompact_cells(h3_cells, integer))
        assert sort(actual) == sort(expected)

    ###############################################################################
    # Regions
    ###############################################################################

    def test_polyfill(self):
        actual = df.withColumn("actual", h3_pyspark.polyfill(F.col("geojson"), F.col("res"), F.col("geo_json_conformant")))
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types(list(h3.geo_to_cells(json.loads(polygon), integer)))
        assert sort(actual) == sort(expected)

    def test_h3_set_to_multi_polygon(self):
        actual = df.withColumn("actual", h3_pyspark.h3_set_to_multi_polygon(F.col("hexes"), F.col("geo_json")))
        actual = actual.collect()[0]["actual"]
        geojson_dict = h3.cells_to_geo(h3_cells)
        if geojson_dict["type"] == "Polygon":
            geojson_dict = {"type": "MultiPolygon", "coordinates": [geojson_dict["coordinates"]]}
        expected = json.dumps(geojson_dict)
        assert sort(actual) == sort(expected)

    ###############################################################################
    # Unidirectional Edges
    ###############################################################################

    def test_h3_indexes_are_neighbors(self):
        actual = df.withColumn("actual", h3_pyspark.h3_indexes_are_neighbors(F.col("origin"), F.col("destination")))
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types(h3.are_neighbor_cells(h3_cells[2], h3_cells[3]))
        assert sort(actual) == sort(expected)

    def test_get_h3_unidirectional_edge(self):
        # Need two neighboring cells for this test
        neighbors = list(h3.grid_disk(h3_cells[0], 1))
        neighbor = [n for n in neighbors if n != h3_cells[0]][0]
        df2 = spark.createDataFrame([{"origin": h3_cells[0], "destination": neighbor}])
        actual = df2.withColumn("actual", h3_pyspark.get_h3_unidirectional_edge(F.col("origin"), F.col("destination")))
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types(h3.cells_to_directed_edge(h3_cells[0], neighbor))
        assert sort(actual) == sort(expected)

    def test_h3_unidirectional_edge_is_valid(self):
        actual = df.withColumn("actual", h3_pyspark.h3_unidirectional_edge_is_valid(F.col("edge")))
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types(h3.is_valid_directed_edge(h3_edge))
        assert sort(actual) == sort(expected)

    def test_get_origin_h3_index_from_unidirectional_edge(self):
        actual = df.withColumn("actual", h3_pyspark.get_origin_h3_index_from_unidirectional_edge(F.col("edge")))
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types(h3.get_directed_edge_origin(h3_edge))
        assert sort(actual) == sort(expected)

    def test_get_destination_h3_index_from_unidirectional_edge(self):
        actual = df.withColumn("actual", h3_pyspark.get_destination_h3_index_from_unidirectional_edge(F.col("edge")))
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types(h3.get_directed_edge_destination(h3_edge))
        assert sort(actual) == sort(expected)

    def test_get_h3_indexes_from_unidirectional_edge(self):
        actual = df.withColumn("actual", h3_pyspark.get_h3_indexes_from_unidirectional_edge(F.col("edge")))
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types(h3.directed_edge_to_cells(h3_edge))
        assert sort(actual) == sort(expected)

    def test_get_h3_unidirectional_edges_from_hexagon(self):
        actual = df.withColumn("actual", h3_pyspark.get_h3_unidirectional_edges_from_hexagon(F.col("h")))
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types(h3.origin_to_directed_edges(h3_cells[0]))
        assert sort(actual) == sort(expected)

    def test_get_h3_unidirectional_edge_boundary(self):
        actual = df.withColumn("actual", h3_pyspark.get_h3_unidirectional_edge_boundary(F.col("edge"), F.col("geo_json")))
        actual = actual.collect()[0]["actual"]
        boundary = h3.directed_edge_to_boundary(h3_edge)
        expected = sanitize_types([[lng, lat] for lat, lng in boundary])
        assert sort(actual) == sort(expected)

    ###############################################################################
    # Miscellaneous
    ###############################################################################

    def test_hex_area(self):
        df2 = df.withColumn("unit", F.lit("m^2"))
        actual = df2.withColumn("actual", h3_pyspark.hex_area(F.col("res"), F.col("unit")))
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types(h3.average_hexagon_area(integer, "m^2"))
        assert sort(actual) == sort(expected)

    def test_cell_area(self):
        df2 = df.withColumn("unit", F.lit("m^2"))
        actual = df2.withColumn("actual", h3_pyspark.cell_area(F.col("h"), F.col("unit")))
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types(h3.cell_area(h3_cells[0], "m^2"))
        assert sort(actual) == sort(expected)

    def test_edge_length(self):
        df2 = df.withColumn("unit", F.lit("m"))
        actual = df2.withColumn("actual", h3_pyspark.edge_length(F.col("res"), F.col("unit")))
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types(h3.average_hexagon_edge_length(integer, "m"))
        assert sort(actual) == sort(expected)

    def test_exact_edge_length(self):
        df2 = df.withColumn("unit", F.lit("m"))
        actual = df2.withColumn("actual", h3_pyspark.exact_edge_length(F.col("edge"), F.col("unit")))
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types(h3.edge_length(h3_edge, "m"))
        assert sort(actual) == sort(expected)

    def test_num_hexagons(self):
        actual = df.withColumn("actual", h3_pyspark.num_hexagons(F.col("res")))
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types(h3.get_num_cells(integer))
        assert sort(actual) == sort(expected)

    def test_get_res0_indexes(self):
        actual = df.withColumn("actual", h3_pyspark.get_res0_indexes())
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types(h3.get_res0_cells())
        assert sort(actual) == sort(expected)

    def test_get_pentagon_indexes(self):
        actual = df.withColumn("actual", h3_pyspark.get_pentagon_indexes(F.col("res")))
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types(h3.get_pentagons(integer))
        assert sort(actual) == sort(expected)

    def test_point_dist(self):
        df2 = df.withColumn("unit", F.lit("m"))
        actual = df2.withColumn("actual", h3_pyspark.point_dist(F.col("point1"), F.col("point2"), F.col("unit")))
        actual = actual.collect()[0]["actual"]
        expected = sanitize_types(h3.great_circle_distance((latitude, longitude), (latitude, longitude), "m"))
        assert sort(actual) == sort(expected)


if __name__ == "__main__":
    unittest.main()
