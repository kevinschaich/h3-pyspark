import h3
from pyspark.sql import functions as F, types as T
from pyspark.sql.column import Column
from typing import List


def _k_ring_distinct(cells: List[str], distance: int = 1):
    """
    Perform a k-ring operation on every input cell and return the distinct set of output cells.
    """
    result_set = set(cells)
    result_set = result_set.union(*[h3.k_ring(c, distance) for c in result_set])

    return list(result_set)


@F.udf(T.ArrayType(T.StringType()))
def k_ring_distinct(cells: Column, distance: Column):
    """
    Perform a k-ring operation on every input cell and return the distinct set of output cells.

    The schema of the output column will be `T.ArrayType(T.StringType())`, where each value in the array is an H3 cell.
    """
    return _k_ring_distinct(cells, distance)
