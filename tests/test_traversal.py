import unittest

from src.h3_pyspark import traversal


input_cells = ["81447ffffffffff", "81267ffffffffff", "8148bffffffffff"]
results = {
    # first cell results
    "81443ffffffffff",
    "8148bffffffffff",
    "8144fffffffffff",
    "81447ffffffffff",
    "8126fffffffffff",
    "81457ffffffffff",
    "81267ffffffffff",
    # second cell results
    "81263ffffffffff",
    "81277ffffffffff",
    "812abffffffffff",
    "8144fffffffffff",
    "81447ffffffffff",
    "8126fffffffffff",
    "81267ffffffffff",
    # third cell results
    "8149bffffffffff",
    "8148bffffffffff",
    "8148fffffffffff",
    "81483ffffffffff",
    "81447ffffffffff",
    "8126fffffffffff",
    "81457ffffffffff",
}


class TestTraversal(unittest.TestCase):
    def test_k_ring(self):
        actual = traversal._k_ring_distinct(input_cells)
        assert set(actual) == set(results)


if __name__ == "__main__":
    unittest.main()
