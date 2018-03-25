import functools
import logging
import unittest

import shapely.geometry

import converters.prg


logging.basicConfig(level=logging.INFO)


class PrgTests(unittest.TestCase):
    def test_process(self):
        rv = converters.prg.process_layer('województwa', 'jpt_kod_je', 'woj_jedn_adm.zip')
        self._test_woj_contents(rv)


    def _test_woj_contents(self, rv):
        self.assertEqual(rv['02']['properties']['jpt_nazwa_'], 'dolnośląskie')
        self.assertEqual(rv['04']['properties']['jpt_nazwa_'], 'kujawsko-pomorskie')
        self.assertEqual(rv['06']['properties']['jpt_nazwa_'], 'lubelskie')
        self.assertEqual(rv['08']['properties']['jpt_nazwa_'], 'lubuskie')
        self.assertEqual(rv['10']['properties']['jpt_nazwa_'], 'łódzkie')
        self.assertEqual(rv['12']['properties']['jpt_nazwa_'], 'małopolskie')
        self.assertEqual(rv['14']['properties']['jpt_nazwa_'], 'mazowieckie')
        self.assertEqual(rv['16']['properties']['jpt_nazwa_'], 'opolskie')
        self.assertEqual(rv['18']['properties']['jpt_nazwa_'], 'podkarpackie')
        self.assertEqual(rv['20']['properties']['jpt_nazwa_'], 'podlaskie')
        self.assertEqual(rv['22']['properties']['jpt_nazwa_'], 'pomorskie')
        self.assertEqual(rv['24']['properties']['jpt_nazwa_'], 'śląskie')
        self.assertEqual(rv['26']['properties']['jpt_nazwa_'], 'świętokrzyskie')
        self.assertEqual(rv['28']['properties']['jpt_nazwa_'], 'warmińsko-mazurskie')
        self.assertEqual(rv['30']['properties']['jpt_nazwa_'], 'wielkopolskie')
        self.assertEqual(rv['32']['properties']['jpt_nazwa_'], 'zachodniopomorskie')
        center = shapely.geometry.shape(rv['02']['geometry']).centroid
        self.assertAlmostEqual(center.x, 16.4106, delta=0.1)
        self.assertAlmostEqual(center.y, 51.0895, delta=0.1)

    def test_get_gminy(self):
        self.assertEqual(
            "Brodnica",
            converters.prg.GminyCache().get_cache().get('0402011')['properties']['jpt_nazwa_']
        )

    def test_get_powiat(self):
        self.assertEqual(
            "brodnicki",
            converters.prg.PowiatyCache().get_cache().get('0402')['properties']['jpt_nazwa_']
        )

    def test_get_wojewodztwo(self):
        self.assertEqual(
            "kujawsko-pomorskie",
            converters.prg.WojewodztwaCache().get_cache().get('04')['properties']['jpt_nazwa_']
        )
