import functools
import json
import logging
import unittest

import overpy
import shapely.geometry

import converters.overpyshapely

logging.basicConfig(level=logging.INFO)


class OverpyShapely(unittest.TestCase):
    @functools.lru_cache(2)
    def get_overpy_result(self):
        with open("test_overpyshapely_testdata1.json") as f:
            return overpy.Result.from_json(json.load(f))
        # return overpy.Overpass().query("[out:json];relation(2984792);out;>;out;")#.get_relation(2984792)

    def test1(self):
        res = self.get_overpy_result()
        os = converters.overpyshapely.OverToShape(res)
        shape = os.get_relation_feature().geometry
        print(json.dumps(shapely.geometry.mapping(shape)))
        borders = shapely.geometry.asShape(json.loads("""
{
        "type": "Polygon",
        "coordinates": [
          [
            [
              19.29473876953125,
              53.495807229228646
            ],
            [
              19.29473876953125,
              53.78402078201105
            ],
            [
              19.815902709960938,
              53.78402078201105
            ],
            [
              19.815902709960938,
              53.495807229228646
            ],
            [
              19.29473876953125,
              53.495807229228646
            ]
          ]
        ]
}
"""))
        self.assertTrue(shape.within(borders))

        inner = shapely.geometry.asShape(json.loads("""
        {
        "type": "Polygon",
        "coordinates": [
          [
            [
              19.557659626007077,
              53.591689719761675
            ],
            [
              19.557659626007077,
              53.59510280117961
            ],
            [
              19.56493377685547,
              53.59510280117961
            ],
            [
              19.56493377685547,
              53.591689719761675
            ],
            [
              19.557659626007077,
              53.591689719761675
            ]
          ]
        ]
      }"""))
        self.assertFalse(shape.contains(inner))
