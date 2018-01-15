import logging
import unittest

import datetime
from xml.etree import ElementTree as ET

import converters.teryt

logging.basicConfig(level=logging.INFO)
#logging.getLogger("converters.teryt.UlicMultiEntry").setLevel(logging.DEBUG)
#logging.getLogger("converters.teryt.UlicEntry").setLevel(logging.DEBUG)
#logging.getLogger("converters.teryt.UlicCache").setLevel(logging.DEBUG)
#logging.getLogger("converters.teryt.TerytCache").setLevel(logging.DEBUG)
#logging.getLogger("converters.teryt.SimcCache").setLevel(logging.DEBUG)
#logging.getLogger("converters.teryt.SimcEntry").setLevel(logging.DEBUG)
#logging.getLogger("converters.teryt.ToFromJsonSerializer").setLevel(logging.DEBUG)



class TerytTests(unittest.TestCase):
    def test_version_to_str(self):
        ret = converters.teryt._int_to_datetime(1507483327)
        self.assertEqual(ret, datetime.date(2017, 10, 8))

    def test_str_to_version(self):
        ret = converters.teryt._date_to_int(datetime.datetime(2017, 10, 8, 0, 0, 0))
        self.assertEqual(ret, 1507413600)

    def test_update(self):
        ret = converters.teryt._ulic_update_binary(datetime.date(2017, 10, 4),
                                                   datetime.date(2017, 10, 6))
        tree = ET.fromstring(ret)
        print(ET.tostring(tree).decode('utf-8'))

    def test_create_simc(self):
        converters.teryt.simc_cache().create_cache()
        ret = converters.teryt.simc().get('0982954')
        self.assertEqual(ret.nazwa, 'Brodnica')


    def test_access_simc(self):
        ret = converters.teryt.simc().get('0982954')
        self.assertEqual(ret.nazwa, 'Brodnica')

    def test_get_version(self):
        converters.teryt._ulic_version()

    def test_init(self):
        converters.teryt.init()

    def test_ulic_update(self):
        converters.teryt._ulic_create(converters.teryt._date_to_int(datetime.date(2017, 10, 4)))
        converters.teryt._ulic_update(datetime.date(2017, 10, 4), datetime.date(2017, 10, 6))

    def test_ser_ulic(self):
        entry = converters.teryt.UlicEntry.from_dict({'sym': 982954,
                                                      'symul': 21447,
                                                      'cecha': 'ul.',
                                                      'nazwa_1': 'Stycznia',
                                                      'nazwa_2': '18',
                                                      'terc': 402011})
        multi_entry = converters.teryt.UlicMultiEntry.from_list([entry, ])
        serial = converters.teryt.ToFromJsonSerializer(
            converters.teryt.UlicMultiEntry,
            converters.teryt.UlicMultiEntry_pb
        )
        ret = serial.deserialize(serial.serialize(multi_entry))
        self.assertEqual(ret.symul, multi_entry.sym_ul)
        self.assertEqual(ret.nazwa, multi_entry.nazwa)
        self.assertEqual(ret.cecha, multi_entry.cecha)
        self.assertEqual(ret.entries.keys(), multi_entry.entries.keys())
        for i in multi_entry.entries:
            self.assertEqual(ret.entries[i].cecha, multi_entry.entries[i].cecha)
            self.assertEqual(ret.entries[i].miejscowosc, multi_entry.entries[i].miejscowosc)
            self.assertEqual(ret.entries[i].nazwa, multi_entry.entries[i].nazwa)
            self.assertEqual(ret.entries[i].symul, multi_entry.entries[i].symul)
            self.assertEqual(ret.entries[i].sym, multi_entry.entries[i].sym)
            self.assertEqual(ret.entries[i].terc, multi_entry.entries[i].terc)

    def test_teryt_ulic_update(self):
        from converters.tools import CacheNotInitialized
        try:
            converters.teryt.TerytCache().get(allow_stale=True)
        except CacheNotInitialized:
            converters.teryt.TerytCache().create_cache()
        try:
            converters.teryt.SimcCache().get(allow_stale=True)
        except CacheNotInitialized:
            converters.teryt.SimcCache().create_cache()

        # test
        with open("ulic_1515369600.xml", "rb") as f:
            data = f.read()
        converters.teryt.UlicCache().create_cache(version=1515369600, data=data)
        del data
        converters.teryt.UlicCache().get(allow_stale=False, version=1515974400)
        converters.teryt.UlicCache().verify()

    def test_teryt_ulic_verify(self):
        converters.teryt.UlicCache().verify()

    def test_teryt_simc_update(self):
        from converters.tools import CacheNotInitialized
        try:
            converters.teryt.TerytCache().get(allow_stale=True)
        except CacheNotInitialized:
            converters.teryt.TerytCache().create_cache()

        # test
        with open("simc_1483228800.xml", "rb") as f:
            data = f.read()
        converters.teryt.SimcCache().create_cache(version=1483228800, data=data)
        del data
        converters.teryt.SimcCache().get(allow_stale=False, version=1514851200)
        converters.teryt.SimcCache().verify()

    def test_teryt_terc_update(self):
        from converters.tools import CacheNotInitialized

        # test
        with open("terc_1483228800.xml", "rb") as f:
            data = f.read()
        converters.teryt.TerytCache().create_cache(version=1483228800, data=data)
        del data
        converters.teryt.TerytCache().get(allow_stale=False, version=1514851200)
        converters.teryt.TerytCache().verify()

