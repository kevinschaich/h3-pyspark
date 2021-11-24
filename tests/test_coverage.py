from inspect import getmembers, isfunction
import h3
from src import h3_pyspark
import unittest


blacklist = set(['h3_is_res_class_iii', 'polyfill_geojson', 'versions', 'polyfill_polygon'])


class TestCoverage(unittest.TestCase):


    def test_geometry_coverage(self):
        h3_functions = getmembers(h3, isfunction)
        h3_functions = set([x[0] for x in h3_functions if '__' not in x[0]])

        h3_pyspark_functions = getmembers(h3_pyspark, isfunction)
        h3_pyspark_functions = set([x[0] for x in h3_pyspark_functions if '__' not in x[0]])

        self.assertEqual(h3_functions - blacklist - h3_pyspark_functions, set())


if __name__ == '__main__':
    unittest.main()
