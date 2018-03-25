import unittest

from converters import emuia


class EmuiaTests(unittest.TestCase):
    def test_fetch(self):
        adresy = emuia.get_addresses("0601032")
        self.assertEqual(3415, len(adresy))