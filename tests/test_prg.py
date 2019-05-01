import logging
import unittest

import shapely.geometry

import converters.prg


logging.basicConfig(level=logging.INFO)


class PrgTests(unittest.TestCase):
    def test_process(self):
        rv = converters.prg.process_layer('wojewodztwa', 'JPT_KOD_JE', 'woj_jedn_adm.zip')
        self._test_woj_contents(rv)


    def _test_woj_contents(self, rv):
        self.assertEqual(rv['02']['properties']['JPT_NAZWA_'], 'dolnośląskie')
        self.assertEqual(rv['04']['properties']['JPT_NAZWA_'], 'kujawsko-pomorskie')
        self.assertEqual(rv['06']['properties']['JPT_NAZWA_'], 'lubelskie')
        self.assertEqual(rv['08']['properties']['JPT_NAZWA_'], 'lubuskie')
        self.assertEqual(rv['10']['properties']['JPT_NAZWA_'], 'łódzkie')
        self.assertEqual(rv['12']['properties']['JPT_NAZWA_'], 'małopolskie')
        self.assertEqual(rv['14']['properties']['JPT_NAZWA_'], 'mazowieckie')
        self.assertEqual(rv['16']['properties']['JPT_NAZWA_'], 'opolskie')
        self.assertEqual(rv['18']['properties']['JPT_NAZWA_'], 'podkarpackie')
        self.assertEqual(rv['20']['properties']['JPT_NAZWA_'], 'podlaskie')
        self.assertEqual(rv['22']['properties']['JPT_NAZWA_'], 'pomorskie')
        self.assertEqual(rv['24']['properties']['JPT_NAZWA_'], 'śląskie')
        self.assertEqual(rv['26']['properties']['JPT_NAZWA_'], 'świętokrzyskie')
        self.assertEqual(rv['28']['properties']['JPT_NAZWA_'], 'warmińsko-mazurskie')
        self.assertEqual(rv['30']['properties']['JPT_NAZWA_'], 'wielkopolskie')
        self.assertEqual(rv['32']['properties']['JPT_NAZWA_'], 'zachodniopomorskie')
        center = shapely.geometry.shape(rv['02']['geometry']).centroid
        self.assertAlmostEqual(center.x, 16.4106, delta=0.1)
        self.assertAlmostEqual(center.y, 51.0895, delta=0.1)

    def test_cache(self):
        rv = converters.prg.process_layer('wojewodztwa', 'JPT_KOD_JE', 'woj_jedn_adm.zip')

        class TestWojewodztwaCache(converters.prg.WojewodztwaCache):
            def _get_cache_data(self, *args, **kwargs):
                return rv

        TestWojewodztwaCache().create_cache()
        cache = TestWojewodztwaCache().get_cache()
        # assert no exception
        cache.keys()

    # @unittest.skip
    def test_get_gminy(self):
        entry = converters.prg.GminyCache().get_cache().get('0402011')
        self.assertEqual("Brodnica", entry['properties']['JPT_NAZWA_'])
        center = shapely.geometry.shape(entry['geometry']).centroid
        self.assertAlmostEqual(center.x, 19.400574, delta=0.1)
        self.assertAlmostEqual(center.y, 53.255072, delta=0.1)

    # @unittest.skip
    def test_get_powiat(self):
        entry = converters.prg.PowiatyCache().get_cache().get('0402')
        self.assertEqual("powiat brodnicki", entry['properties']['JPT_NAZWA_'])
        center = shapely.geometry.shape(entry['geometry']).centroid
        self.assertAlmostEqual(center.x, 19.428240, delta=0.1)
        self.assertAlmostEqual(center.y, 53.261699, delta=0.1)

    # @unittest.skip
    def test_get_wojewodztwo(self):
        entry = converters.prg.WojewodztwaCache().get_cache().get('04')
        self.assertEqual("kujawsko-pomorskie", entry['properties']['JPT_NAZWA_'])
        center = shapely.geometry.shape(entry['geometry']).centroid
        self.assertAlmostEqual(center.x, 18.488192, delta=0.1)
        self.assertAlmostEqual(center.y, 53.072692, delta=0.1)
