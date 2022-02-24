import unittest
from src.h3_pyspark import indexing


point = '{ "type": "Point", "coordinates": [ -80.79617142677307, 32.131567579594716 ] }'
line = '{ "type": "LineString", "coordinates": [ [ -80.79708337783813, 32.13510176661157 ], [ -80.79504489898682, 32.13510176661157 ], [ -80.79440116882324, 32.13550151179293 ], [ -80.79315662384033, 32.13535615011151 ], [ -80.79259872436523, 32.13470201967832 ], [ -80.79141855239868, 32.13292130751054 ] ] }'
line2 = '{ "type": "LineString", "coordinates": [ [ -80.79768419265747, 32.13413873693519 ], [ -80.79171895980835, 32.132230817929354 ] ] }'
polygon = '{ "type": "Polygon", "coordinates": [ [ [ -80.79427242279051, 32.132866795365196 ], [ -80.79128980636597, 32.132866795365196 ], [ -80.79128980636597, 32.13479287140789 ], [ -80.79427242279051, 32.13479287140789 ], [ -80.79427242279051, 32.132866795365196 ] ] ] }'
polygon2 = '{ "type": "Polygon", "coordinates": [ [ [ -80.7916921377182, 32.13222627521743 ], [ -80.79402565956116, 32.135074511194496 ], [ -80.79768419265747, 32.13414327955186 ], [ -80.7916921377182, 32.13222627521743 ] ] ] }'
multipoint = '{ "type": "MultiPoint", "coordinates":[ [ -80.7935643196106, 32.135755894178004 ], [ -80.79058170318604, 32.1330848437511 ]] }'
multilinestring = '{ "type": "MultiLineString", "coordinates": [ [[ -80.7945728302002, 32.13577406432124 ], [ -80.79319953918457, 32.135010915189675 ]], [ [ -80.79257726669312, 32.13395703208247 ], [ -80.7915472984314, 32.13315752643055 ] ] ] }'
multipolygon = '{ "type": "MultiPolygon", "coordinates": [ [ [ [ -80.79442262649536, 32.13522895845023 ], [ -80.79298496246338, 32.13522895845023 ], [ -80.79298496246338, 32.13602844594619 ], [ -80.79442262649536, 32.13602844594619 ], [ -80.79442262649536, 32.13522895845023 ] ] ], [ [ [ -80.7923412322998, 32.1330848437511 ], [ -80.79073190689087, 32.1330848437511 ], [ -80.79073190689087, 32.13375715632646 ], [ -80.7923412322998, 32.13375715632646 ], [ -80.7923412322998, 32.1330848437511 ] ] ] ] }'


class TestIndexing(unittest.TestCase):
    def test_h3_index_point(self):
        actual = indexing._index_shape(point, 9)
        expected = ["8944d551007ffff"]
        assert set(actual) == set(expected)

    def test_h3_index_line(self):
        actual = indexing._index_shape(line, 9)
        expected = ["8944d551073ffff", "8944d551077ffff", "8944d55103bffff"]
        assert set(actual) == set(expected)

    def test_h3_index_line_2(self):
        actual = indexing._index_shape(line2, 9)
        expected = ["8944d551073ffff", "8944d55103bffff", "8944d55100fffff"]
        assert set(actual) == set(expected)

    def test_h3_index_polygon(self):
        actual = indexing._index_shape(polygon, 9)
        expected = ["8944d551077ffff", "8944d55100fffff", "8944d551073ffff"]
        assert set(actual) == set(expected)

    def test_h3_index_polygon2(self):
        actual = indexing._index_shape(polygon2, 9)
        expected = ["8944d551077ffff", "8944d55100fffff", "8944d551073ffff", "8944d55103bffff"]
        assert set(actual) == set(expected)

    def test_h3_index_multipoint(self):
        actual = indexing._index_shape(multipoint, 9)
        expected = ["8944d551077ffff", "8944d551073ffff"]
        assert set(actual) == set(expected)

    def test_h3_index_multiline(self):
        actual = indexing._index_shape(multilinestring, 9)
        expected = ["8944d551077ffff", "8944d551073ffff"]
        assert set(actual) == set(expected)

    def test_h3_index_multipolygon(self):
        actual = indexing._index_shape(multipolygon, 9)
        expected = ["8944d551077ffff", "8944d551073ffff"]
        assert set(actual) == set(expected)


if __name__ == "__main__":
    unittest.main()
