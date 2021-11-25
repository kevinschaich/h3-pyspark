<img align="right" src="https://uber.github.io/img/h3Logo-color.svg" alt="H3 Logo" width="200">

# **h3-pyspark**: Uber's H3 Hexagonal Hierarchical Geospatial Indexing System in PySpark

[![PyPI version](https://badge.fury.io/py/h3-pyspark.svg)](https://badge.fury.io/py/h3-pyspark)
[![PyPI downloads](https://pypip.in/d/h3-pyspark/badge.png)](https://pypistats.org/packages/h3-pyspark)
[![conda](https://img.shields.io/conda/vn/conda-forge/h3-pyspark.svg)](https://anaconda.org/conda-forge/h3-pyspark)
[![version](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/kevinschaich/h3-pyspark/blob/master/LICENSE)

[![Tests](https://github.com/kevinschaich/h3-pyspark/actions/workflows/tests.yml/badge.svg?branch=master)](https://github.com/kevinschaich/h3-pyspark/actions/workflows/tests.yml)
<!-- [![codecov](https://codecov.io/gh/kevinschaich/h3-pyspark/branch/master/graph/badge.svg)](https://codecov.io/gh/kevinschaich/h3-pyspark) -->

PySpark bindings for the [H3 core library](https://h3geo.org/).

For available functions, please see the vanilla Python binding documentation at:

- [uber.github.io/h3-py](https://uber.github.io/h3-py)

## Installation

From `PyPI`:

```console
pip install h3-pyspark
```

From `conda`

```console
conda config --add channels conda-forge
conda install h3-pyspark
```

## Usage

```python
>>> from pyspark.sql import SparkSession, functions as F
>>> import h3_pyspark
>>>
>>> spark = SparkSession.builder.getOrCreate()
>>> df = spark.createDataFrame([{"lat": 37.769377, "lng": -122.388903, 'resolution': 9}])
>>>
>>> df = df.withColumn('h3_9', h3_pyspark.geo_to_h3('lat', 'lng', 'resolution'))
>>> df.show()

+---------+-----------+----------+---------------+
|      lat|        lng|resolution|           h3_9|
+---------+-----------+----------+---------------+
|37.769377|-122.388903|         9|89283082e73ffff|
+---------+-----------+----------+---------------+
```

## Publishing

1. Bump version in `setup.cfg`
2. Publish:

```bash
python3 -m build
python3 -m twine upload --repository pypi dist/*
```
