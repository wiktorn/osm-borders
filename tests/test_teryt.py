import logging
import unittest

import datetime
from xml.etree import ElementTree as ET

import converters.teryt
from converters.tools import groupby

logging.basicConfig(level=logging.INFO)


# logging.getLogger("converters.teryt.UlicMultiEntry").setLevel(logging.DEBUG)
# logging.getLogger("converters.teryt.UlicEntry").setLevel(logging.DEBUG)
# logging.getLogger("converters.teryt.UlicCache").setLevel(logging.DEBUG)
# logging.getLogger("converters.teryt.TerytCache").setLevel(logging.DEBUG)
# logging.getLogger("converters.teryt.SimcCache").setLevel(logging.DEBUG)
# logging.getLogger("converters.teryt.SimcEntry").setLevel(logging.DEBUG)
# logging.getLogger("converters.teryt.ToFromJsonSerializer").setLevel(logging.DEBUG)


class TerytTests(unittest.TestCase):
    def test_version_to_str(self):
        ret = converters.teryt._int_to_datetime(1507483327)
        self.assertEqual(ret, datetime.date(2017, 10, 8))

    def test_str_to_version(self):
        ret = converters.teryt._date_to_int(datetime.date(2017, 10, 8))
        self.assertEqual(1507420800, ret)

    def test_update(self):
        data = converters.teryt.UlicCache()._get_updates(
            converters.teryt._date_to_int(datetime.date(2017, 10, 4)),
            converters.teryt._date_to_int(datetime.date(2017, 10, 6))
        )
        tree = ET.fromstring(data)
        self.assertEqual(90, len(tree.findall('zmiana')))

    def test_create_simc(self):
        converters.teryt.SimcCache().create_cache()
        ret = converters.teryt.simc().get('0982954')
        self.assertEqual('Brodnica', ret.nazwa)

    def test_access_simc(self):
        ret = converters.teryt.simc().get('0982954')
        self.assertEqual('Brodnica', ret.nazwa)

    def test_get_version(self):
        converters.teryt.UlicCache().current_cache_version()

    def test_init(self):
        converters.teryt.init()

    def test_ulic_update(self):
        ulic_cache = converters.teryt.UlicCache()
        ulic_cache.create_cache(version=converters.teryt._date_to_int(datetime.date(2017, 10, 4)))
        ulic_cache.update_cache(from_version=converters.teryt._date_to_int(datetime.date(2017, 10, 4)),
                                 target_version=converters.teryt._date_to_int(datetime.date(2017, 10, 6)))

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
        self.assertEqual(ret.sym_ul, multi_entry.sym_ul)
        self.assertEqual(ret.nazwa, multi_entry.nazwa)
        self.assertEqual(ret.cecha, multi_entry.cecha)
        self.assertEqual(ret.entries.keys(), multi_entry.entries.keys())
        for i in multi_entry.entries:
            self.assertEqual(ret.entries[i].cecha, multi_entry.entries[i].cecha)
            self.assertEqual(ret.entries[i].miejscowosc, multi_entry.entries[i].miejscowosc)
            self.assertEqual(ret.entries[i].nazwa, multi_entry.entries[i].nazwa)
            self.assertEqual(ret.entries[i].sym_ul, multi_entry.entries[i].sym_ul)
            self.assertEqual(ret.entries[i].sym, multi_entry.entries[i].sym)
            self.assertEqual(ret.entries[i].terc, multi_entry.entries[i].terc)

    def test_teryt_ulic_update(self):
        from converters.tools import CacheNotInitialized
        try:
            converters.teryt.TerytCache().get_cache(allow_stale=True)
        except CacheNotInitialized:
            converters.teryt.TerytCache().create_cache()
        try:
            converters.teryt.SimcCache().get_cache(allow_stale=True)
        except CacheNotInitialized:
            converters.teryt.SimcCache().create_cache()

        # test
        with open("ulic_1515369600.xml", "rb") as f:
            tree = ET.fromstring(f.read())
            grouped = groupby(
                (converters.teryt.UlicEntry(converters.teryt._row_as_dict(x)) for x in tree.find('catalog').iter('row')),
                lambda x: x.sym_ul
            )

            data = dict((key, converters.teryt.UlicMultiEntry.from_list(value)) for key, value in grouped.items())
        converters.teryt.UlicCache().create_cache(version=1515369600, data=data)
        del data
        converters.teryt.UlicCache().get_cache(allow_stale=False, version=1515974400)
        converters.teryt.UlicCache().verify()

    def test_teryt_ulic_verify(self):
        converters.teryt.UlicCache().verify()

    def test_teryt_simc_update(self):
        from converters.tools import CacheNotInitialized
        try:
            converters.teryt.TerytCache().get_cache(allow_stale=True)
        except CacheNotInitialized:
            converters.teryt.TerytCache().create_cache()

        # test
        with open("simc_1483228800.xml", "rb") as f:
            data = converters.teryt.SimcCache()._data_to_dict(f.read(), converters.teryt.SimcEntry)
        converters.teryt.SimcCache().create_cache(version=1483228800, data=data)
        converters.teryt.SimcCache().get_cache(allow_stale=False, version=1514851200)
        converters.teryt.SimcCache().verify()

    def test_teryt_terc_update(self):

        # test
        with open("terc_1483228800.xml", "rb") as f:
            data = converters.teryt.TerytCache()._data_to_dict(f.read(), converters.teryt.TercEntry)
        converters.teryt.TerytCache().create_cache(version=1483228800, data=data)
        del data
        converters.teryt.TerytCache().get_cache(allow_stale=False, version=1514851200)
        converters.teryt.TerytCache().verify()

    def test_multple_access(self):
        self.assertEqual('Brodnica', converters.teryt.simc().get('0982954').nazwa)
        self.assertEqual('Brodnica', converters.teryt.teryt().get('0402011').nazwa)
        self.assertEqual('Ulica 15 Lipca', converters.teryt.ulic().get('11097').nazwa)
