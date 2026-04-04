import h3
from pyspark.sql import functions as F, types as T
import json
from .utils import sanitize_types, handle_nulls


###############################################################################
# Indexing
###############################################################################


@F.udf(returnType=T.StringType())
@handle_nulls
def geo_to_h3(lat, lng, resolution):
    return sanitize_types(h3.latlng_to_cell(lat, lng, resolution))


@F.udf(returnType=T.ArrayType(T.DoubleType()))
@handle_nulls
def h3_to_geo(h):
    return sanitize_types(h3.cell_to_latlng(h))


@F.udf(returnType=T.StringType())
@handle_nulls
def h3_to_geo_boundary(h, geo_json):
    # cell_to_boundary returns ((lat, lng), ...) tuples
    boundary = h3.cell_to_boundary(h)
    if geo_json:
        # Convert to GeoJSON (lng, lat) order and close the ring
        coords = [[lng, lat] for lat, lng in boundary]
        coords.append(coords[0])
        return sanitize_types(json.dumps({"type": "MultiPolygon", "coordinates": [[coords]]}))
    return sanitize_types(list(boundary))


###############################################################################
# Inspection
###############################################################################


@F.udf(returnType=T.IntegerType())
@handle_nulls
def h3_get_resolution(h):
    return sanitize_types(h3.get_resolution(h))


@F.udf(returnType=T.IntegerType())
@handle_nulls
def h3_get_base_cell(h):
    return sanitize_types(h3.get_base_cell_number(h))


@F.udf(returnType=T.LongType())
@handle_nulls
def string_to_h3(h):
    return sanitize_types(h3.str_to_int(h))


@F.udf(returnType=T.StringType())
@handle_nulls
def h3_to_string(h):
    return sanitize_types(h3.int_to_str(h))


@F.udf(returnType=T.BooleanType())
@handle_nulls
def h3_is_valid(h):
    return sanitize_types(h3.is_valid_cell(h))


@F.udf(returnType=T.BooleanType())
@handle_nulls
def h3_is_res_class_III(h):
    return sanitize_types(h3.is_res_class_III(h))


@F.udf(returnType=T.BooleanType())
@handle_nulls
def h3_is_pentagon(h):
    return sanitize_types(h3.is_pentagon(h))


@F.udf(returnType=T.ArrayType(T.IntegerType()))
@handle_nulls
def h3_get_faces(h):
    return sanitize_types(h3.get_icosahedron_faces(h))


###############################################################################
# Traversal
###############################################################################


@F.udf(returnType=T.ArrayType(T.StringType()))
@handle_nulls
def k_ring(origin, k):
    return sanitize_types(h3.grid_disk(origin, k))


@F.udf(returnType=T.ArrayType(T.ArrayType(T.StringType())))
@handle_nulls
def k_ring_distances(origin, k):
    # h3.k_ring_distances removed in v4; implement using grid_ring
    return sanitize_types([list(h3.grid_ring(origin, i)) for i in range(k + 1)])


@F.udf(returnType=T.ArrayType(T.StringType()))
@handle_nulls
def hex_range(h, k):
    # h3.hex_range removed in v4 (it raised on pentagons); use grid_disk
    return sanitize_types(h3.grid_disk(h, k))


@F.udf(returnType=T.ArrayType(T.ArrayType(T.StringType())))
@handle_nulls
def hex_range_distances(h, k):
    # h3.hex_range_distances removed in v4; implement using grid_ring
    return sanitize_types([list(h3.grid_ring(h, i)) for i in range(k + 1)])


@F.udf(returnType=T.MapType(T.StringType(), T.ArrayType(T.ArrayType(T.StringType()))))
@handle_nulls
def hex_ranges(h, k):
    # h3.hex_ranges removed in v4; implement using grid_ring
    hexes = [h] if isinstance(h, str) else list(h)
    result = {cell: [list(h3.grid_ring(cell, i)) for i in range(k + 1)] for cell in hexes}
    return sanitize_types(result)


@F.udf(returnType=T.ArrayType(T.StringType()))
@handle_nulls
def hex_ring(h, k):
    return sanitize_types(h3.grid_ring(h, k))


@F.udf(returnType=T.ArrayType(T.StringType()))
@handle_nulls
def h3_line(start, end):
    return sanitize_types(h3.grid_path_cells(start, end))


@F.udf(returnType=T.IntegerType())
@handle_nulls
def h3_distance(h1, h2):
    return sanitize_types(h3.grid_distance(h1, h2))


@F.udf(returnType=T.ArrayType(T.IntegerType()))
@handle_nulls
def experimental_h3_to_local_ij(origin, h):
    return sanitize_types(h3.cell_to_local_ij(origin, h))


@F.udf(returnType=T.StringType())
@handle_nulls
def experimental_local_ij_to_h3(origin, i, j):
    return sanitize_types(h3.local_ij_to_cell(origin, i, j))


###############################################################################
# Hierarchy
###############################################################################


@F.udf(returnType=T.StringType())
@handle_nulls
def h3_to_parent(h, parent_res):
    return sanitize_types(h3.cell_to_parent(h, parent_res))


@F.udf(returnType=T.ArrayType(T.StringType()))
@handle_nulls
def h3_to_children(h, child_res):
    return sanitize_types(h3.cell_to_children(h, child_res))


@F.udf(returnType=T.StringType())
@handle_nulls
def h3_to_center_child(h, child_res):
    return sanitize_types(h3.cell_to_center_child(h, child_res))


@F.udf(returnType=T.ArrayType(T.StringType()))
@handle_nulls
def compact(hexes):
    return sanitize_types(h3.compact_cells(hexes))


@F.udf(returnType=T.ArrayType(T.StringType()))
@handle_nulls
def uncompact(hexes, res):
    return sanitize_types(h3.uncompact_cells(hexes, res))


###############################################################################
# Regions
###############################################################################


@F.udf(returnType=T.ArrayType(T.StringType()))
@handle_nulls
def polyfill(polygons, res, geo_json_conformant):
    # NOTE: h3 v4 always uses GeoJSON (lng, lat) coordinate order.
    # geo_json_conformant is kept for backward compatibility but is now ignored.
    polygons = json.loads(polygons)
    return sanitize_types(list(h3.geo_to_cells(polygons, res)))


@F.udf(returnType=T.StringType())
@handle_nulls
def h3_set_to_multi_polygon(hexes, geo_json):
    # NOTE: cells_to_geo may return Polygon or MultiPolygon depending on the input cells.
    # We normalize to MultiPolygon to preserve backward compatibility.
    geojson_dict = h3.cells_to_geo(hexes)
    if geojson_dict["type"] == "Polygon":
        geojson_dict = {"type": "MultiPolygon", "coordinates": [geojson_dict["coordinates"]]}
    if geo_json:
        return sanitize_types(json.dumps(geojson_dict))
    return sanitize_types(geojson_dict["coordinates"])


###############################################################################
# Unidirectional Edges
###############################################################################


@F.udf(returnType=T.BooleanType())
@handle_nulls
def h3_indexes_are_neighbors(origin, destination):
    return sanitize_types(h3.are_neighbor_cells(origin, destination))


@F.udf(returnType=T.StringType())
@handle_nulls
def get_h3_unidirectional_edge(origin, destination):
    return sanitize_types(h3.cells_to_directed_edge(origin, destination))


@F.udf(returnType=T.BooleanType())
@handle_nulls
def h3_unidirectional_edge_is_valid(edge):
    return sanitize_types(h3.is_valid_directed_edge(edge))


@F.udf(returnType=T.StringType())
@handle_nulls
def get_origin_h3_index_from_unidirectional_edge(edge):
    return sanitize_types(h3.get_directed_edge_origin(edge))


@F.udf(returnType=T.StringType())
@handle_nulls
def get_destination_h3_index_from_unidirectional_edge(edge):
    return sanitize_types(h3.get_directed_edge_destination(edge))


@F.udf(returnType=T.ArrayType(T.StringType()))
@handle_nulls
def get_h3_indexes_from_unidirectional_edge(edge):
    return sanitize_types(h3.directed_edge_to_cells(edge))


@F.udf(returnType=T.ArrayType(T.StringType()))
@handle_nulls
def get_h3_unidirectional_edges_from_hexagon(h):
    return sanitize_types(h3.origin_to_directed_edges(h))


@F.udf(returnType=T.ArrayType(T.ArrayType(T.DoubleType())))
@handle_nulls
def get_h3_unidirectional_edge_boundary(h, geo_json):
    # directed_edge_to_boundary returns ((lat, lng), ...) tuples
    boundary = h3.directed_edge_to_boundary(h)
    if geo_json:
        return sanitize_types([[lng, lat] for lat, lng in boundary])
    return sanitize_types(list(boundary))


###############################################################################
# Miscellaneous
###############################################################################


@F.udf(returnType=T.DoubleType())
@handle_nulls
def hex_area(res, unit):
    return sanitize_types(h3.average_hexagon_area(res, unit))


@F.udf(returnType=T.DoubleType())
@handle_nulls
def cell_area(h, unit):
    return sanitize_types(h3.cell_area(h, unit))


@F.udf(returnType=T.DoubleType())
@handle_nulls
def edge_length(res, unit):
    # Returns the average edge length for hexagons at the given resolution.
    return sanitize_types(h3.average_hexagon_edge_length(res, unit))


@F.udf(returnType=T.DoubleType())
@handle_nulls
def exact_edge_length(e, unit):
    # Returns the exact length of a specific directed edge.
    # In h3 v4, this is h3.edge_length(edge, unit).
    return sanitize_types(h3.edge_length(e, unit))


@F.udf(returnType=T.LongType())
@handle_nulls
def num_hexagons(res):
    return sanitize_types(h3.get_num_cells(res))


@F.udf(returnType=T.ArrayType(T.StringType()))
@handle_nulls
def get_res0_indexes():
    return sanitize_types(h3.get_res0_cells())


@F.udf(returnType=T.ArrayType(T.StringType()))
@handle_nulls
def get_pentagon_indexes(res):
    return sanitize_types(h3.get_pentagons(res))


@F.udf(returnType=T.DoubleType())
@handle_nulls
def point_dist(point1, point2, unit):
    return sanitize_types(h3.great_circle_distance(point1, point2, unit))


###############################################################################
# New H3 v4 Functions
###############################################################################


@F.udf(returnType=T.StringType())
@handle_nulls
def cell_to_vertex(h, vertex_num):
    return sanitize_types(h3.cell_to_vertex(h, vertex_num))


@F.udf(returnType=T.ArrayType(T.StringType()))
@handle_nulls
def cell_to_vertexes(h):
    return sanitize_types(h3.cell_to_vertexes(h))


@F.udf(returnType=T.ArrayType(T.DoubleType()))
@handle_nulls
def vertex_to_latlng(v):
    return sanitize_types(h3.vertex_to_latlng(v))


@F.udf(returnType=T.BooleanType())
@handle_nulls
def is_valid_vertex(v):
    return sanitize_types(h3.is_valid_vertex(v))


@F.udf(returnType=T.BooleanType())
@handle_nulls
def is_valid_index(h):
    return sanitize_types(h3.is_valid_index(h))


@F.udf(returnType=T.LongType())
@handle_nulls
def cell_to_child_pos(child, parent_res):
    return sanitize_types(h3.cell_to_child_pos(child, parent_res))


@F.udf(returnType=T.LongType())
@handle_nulls
def cell_to_children_size(h, child_res):
    return sanitize_types(h3.cell_to_children_size(h, child_res))


@F.udf(returnType=T.StringType())
@handle_nulls
def child_pos_to_cell(parent, child_res, child_pos):
    return sanitize_types(h3.child_pos_to_cell(parent, child_res, child_pos))


@F.udf(returnType=T.ArrayType(T.StringType()))
@handle_nulls
def polygon_to_cells(polygons, res, flags=0):
    polygons = json.loads(polygons) if isinstance(polygons, str) else polygons
    return sanitize_types(list(h3.polygon_to_cells(polygons, res, flags)))


###############################################################################
# H3 v4 Aliases (new names for renamed functions)
###############################################################################

latlng_to_cell = geo_to_h3
cell_to_latlng = h3_to_geo
cell_to_boundary = h3_to_geo_boundary
get_resolution = h3_get_resolution
get_base_cell_number = h3_get_base_cell
str_to_int = string_to_h3
int_to_str = h3_to_string
is_valid_cell = h3_is_valid
is_res_class_III = h3_is_res_class_III
is_pentagon = h3_is_pentagon
get_icosahedron_faces = h3_get_faces
grid_disk = k_ring
grid_ring = hex_ring
grid_path_cells = h3_line
grid_distance = h3_distance
cell_to_local_ij = experimental_h3_to_local_ij
local_ij_to_cell = experimental_local_ij_to_h3
cell_to_parent = h3_to_parent
cell_to_children = h3_to_children
cell_to_center_child = h3_to_center_child
compact_cells = compact
uncompact_cells = uncompact
geo_to_cells = polyfill
cells_to_geo = h3_set_to_multi_polygon
are_neighbor_cells = h3_indexes_are_neighbors
cells_to_directed_edge = get_h3_unidirectional_edge
is_valid_directed_edge = h3_unidirectional_edge_is_valid
get_directed_edge_origin = get_origin_h3_index_from_unidirectional_edge
get_directed_edge_destination = get_destination_h3_index_from_unidirectional_edge
directed_edge_to_cells = get_h3_indexes_from_unidirectional_edge
origin_to_directed_edges = get_h3_unidirectional_edges_from_hexagon
directed_edge_to_boundary = get_h3_unidirectional_edge_boundary
average_hexagon_area = hex_area
average_hexagon_edge_length = edge_length
get_num_cells = num_hexagons
get_res0_cells = get_res0_indexes
get_pentagons = get_pentagon_indexes
great_circle_distance = point_dist
