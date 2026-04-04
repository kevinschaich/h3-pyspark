from inspect import getmembers, isfunction
import h3
import unittest

from src import h3_pyspark


# Functions not wrapped as PySpark UDFs because they operate on H3Shape Python
# objects or are internal/experimental utilities not suitable for distributed use.
blacklist = set([
    # H3Shape object functions (return or require Python-only H3Shape objects)
    "cells_to_h3shape",
    "geo_to_h3shape",
    "h3shape_to_cells",
    "h3shape_to_cells_experimental",
    "h3shape_to_geo",
    # Low-level / internal cell construction utilities
    "construct_cell",
    "deconstruct_cell",
    "get_index_digit",
    # Experimental (likely to change)
    "polygon_to_cells_experimental",
    # Library metadata
    "versions",
])


class TestCoverage(unittest.TestCase):
    def test_geometry_coverage(self):
        h3_functions = set([name for name, _ in getmembers(h3, isfunction) if "__" not in name])

        # Check that each non-blacklisted h3 function is accessible in h3_pyspark
        # (as a UDF wrapper or alias). We use hasattr rather than isfunction because
        # PySpark UDF wrappers are callable objects, not plain Python functions.
        missing = {name for name in h3_functions - blacklist if not hasattr(h3_pyspark, name)}

        self.assertEqual(missing, set())


if __name__ == "__main__":
    unittest.main()
