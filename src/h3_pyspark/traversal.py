import h3
from pyspark.sql import functions as F
from pyspark.sql.column import Column
from typing import List


def _k_ring_distinct(cells: List[str], distance: int = 1):
    """
    Perform a k-ring operation on every input cell and return the distinct set of output cells.
    """
    result_set = set(cells)
    result_set = result_set.union(*[h3.k_ring(c, distance) for c in result_set])  # noqa

    return list(result_set)


@F.udf
def k_ring_distinct(cells: Column, distance: Column):
    """
    Return the set of H3 cells at the specified resolution which completely cover the input shape.
    """
    return _k_ring_distinct(cells, distance)
