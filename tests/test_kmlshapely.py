import logging
import unittest

import shapely.geometry

import converters.kmlshapely

logging.basicConfig(level=logging.INFO)


class KmlShapelyTests(unittest.TestCase):
    def test1(self):
        with open("example.kml") as f:
            content = f.read()
            obj = converters.kmlshapely.kml_to_shapely(content)
            self.assertEqual("layer_miejscowosci_granica.1488043", obj[0].get_tag('name'))
            self.assertEqual("2005062", obj[0].get_tag('TERYT_JEDNOSTKI'))
            self.assertEqual("2005062", obj[0].get_tag('TERYT_JEDNOSTKI'))
            self.assertTrue("0029720", obj[0].get_tag("TERYT_MIEJSCOWOSCI"))
            self.assertTrue(
                shapely.geometry.Polygon(
                    [
                        (20.7055986, 52.406360),
                        (20.7055986, 52.776607),
                        (23.5372864, 52.776607),
                        (23.5372864, 52.406360),
                        (20.7055986, 52.406360)
                    ]
                ).contains(
                    obj[0].geometry
                )
            )

    def test_multi_geometry(self):
        with open("multipart_geometry.kml") as f:
            features = converters.kmlshapely.kml_to_shapely(f.read())
        for feature in features:
            self.assertTrue(isinstance(feature.geometry, shapely.geometry.base.BaseMultipartGeometry),
                            "Geometry {0} is not multipart for Feature {1}".format(type(feature.geometry), feature))
