import logging
import unittest

import datetime
from xml.etree import ElementTree as ET

import converters.teryt

logging.basicConfig(level=logging.INFO)


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
        self.assertEqual(ret.symul, multi_entry.symul)
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
