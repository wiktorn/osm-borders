import base64
import io
import logging
import typing
import zipfile
from xml.etree import ElementTree as ET

import zeep
from zeep.wsse.username import UsernameToken

from .tools import CachedDictionary, groupby

__log = logging.getLogger(__name__)

# structure:
# województwo:
#   - id == terc
#   - terc == terc
#   - value == nazwa
# powiat:
#   - id == terc
#   - parent == województwo.terc
#   - terc == terc
#   - value == nazwa
# gmina:
#   - id == terc
#   - parnet == powiat.terc
#   - terc == terc
#   - value == nazwa
# miejscowość:
#   - id == simc
#   - parent == gmina.terc lub miejscowość.simc
#   - simc
#   - value - nazwa
#   - wojeództwo, powiat, gmina
# ulica:
#   - id = symul
#   - sim = lista [simc]
#   - nazwa


_TERC_RODZAJ_MAPPING = {
    '1': 'gmina miejska',
    '2': 'gmina wiejska',
    '3': 'gmina miejsko-wiejska',
    '4': 'miasto w gminie miejsko-wiejskiej',
    '5': 'obszar wiejski w gminie miejsko-wiejskiej',
    '8': 'dzielnica m.st. Warszawa',
    '9': 'delegatury w gminach miejskich',
}

_ULIC_CECHA_MAPPING = {
    'UL.': 'Ulica',
    'AL.': 'Aleja',
    'PL.': 'Plac',
    'SKWER': 'Skwer',
    'BULW.': 'Bulwar',
    'RONDO': 'Rondo',
    'PARK': 'Park',
    'RYNEK': 'Rynek',
    'SZOSA': 'Szosa',
    'DROGA': 'Droga',
    'OS.': 'Osiedle',
    'OGRÓD': 'Ogród',
    'WYSPA': 'Wyspa',
    'WYB.': 'Wybrzeże',
    'INNE': ''
}


class TercEntry(object):
    nazwa = None

    def __init__(self, dct):
        self.woj = dct.get('woj')
        self.powiat = dct.get('pow')
        self.gmi = dct.get('gmi')
        self.rodz = dct.get('rodz')
        self.rodz_nazwa = _TERC_RODZAJ_MAPPING.get(dct.get('rodz'), '') if self.rodz else \
            {2: 'województwo', 4: 'powiat'}[len(self.terc)]
        self.nazwadod = dct.get('nazwadod', '')
        self.nazwa = dct.get('nazwa')

    @property
    def terc_base(self):
        return (y for y in
                (self.woj, self.powiat) + ((self.gmi, self.rodz) if self.gmi else ())
                if y
                )

    @property
    def terc(self):
        return "".join(y for y in
                       (self.woj, self.powiat) + ((self.gmi, self.rodz) if self.gmi else ())
                       if y
                       )

    @property
    def parent_terc(self):
        if self.gmi:
            return "".join((self.woj, self.powiat))
        if self.powiat:
            return "".join((self.woj,))
        return ""

    @property
    def json(self):
        return (
            "add", {
                "doc": {
                    'id': "terc_" + self.terc,
                    'parent': ("terc_" + self.parent_terc) if self.parent_terc else '',
                    'terc': self.terc,
                    'rodzaj': self.rodz_nazwa,
                    'value': (self.nazwadod + ' ' + self.nazwa).strip(),
                    'typ': 'terc',
                },
                'boost': 7 - len(self.parent_terc)
            }
        )


class SimcEntry(object):
    terc = None
    nazwa = None

    def __init__(self, dct: dict):
        self.terc = dct.get('woj') + dct.get('pow') + dct.get('gmi') + dct.get('rodz_gmi')
        self.rm = wmrodz[dct.get('rm')]
        self.nazwa = dct.get('nazwa')
        self.sym = dct.get('sym')
        self.parent = None
        if dct.get('sym') != dct.get('sympod'):
            self.parent = dct.get('sympod')

    @property
    def json(self):
        return (
            "add", {
                "doc": {
                    'id': 'simc_' + self.sym,
                    'parent': ('simc_' + self.parent) if self.parent else ('terc_' + self.terc),
                    # 'terc': self.terc,
                    'rodzaj': self.rm,
                    'value': self.nazwa,
                    'simc': self.sym,
                    'wojewodztwo': self.woj,
                    'powiat': self.powiat,
                    'gmina': self.gmi,
                    'typ': 'simc',
                }
            }
        )

    @property
    def gmi(self):
        return teryt[self.terc].nazwa

    @property
    def woj(self):
        return teryt[self.terc[:2]].nazwa

    @property
    def powiat(self):
        return teryt[self.terc[:4]].nazwa


class BasicEntry(object):
    def __init__(self, dct):
        for i in dct.keys():
            setattr(self, i, dct[i])


def _clean_street_name(cecha: str, nazwa1: str, nazwa2: str):
    def mapper(name: str):
        if name.casefold().startswith(cecha.casefold()):
            return name[len(cecha):].strip()
        elif name.casefold().startswith(_ULIC_CECHA_MAPPING[cecha.upper()].casefold()):
            return name[len(_ULIC_CECHA_MAPPING.get(cecha.upper())):].strip()
        return name

    nazwa1 = mapper(nazwa1)
    nazwa2 = mapper(nazwa2)
    return " ".join((x for x in (_ULIC_CECHA_MAPPING.get(cecha.upper()), nazwa1, nazwa2) if x))


class UlicEntry(object):
    def __init__(self, dct: dict):
        self.sym = dct.get('sym')
        self.symul = dct.get('sym_ul')
        self.cecha = _ULIC_CECHA_MAPPING[dct.get('cecha').upper()]
        self.nazwa = _clean_street_name(dct.get('cecha', ''), dct.get('nazwa_2', ''), dct.get('nazwa_1', ''))
        assert self.terc == dct.get('woj') + dct.get('pow') + dct.get('gmi') + dct.get('rodz_gmi')

    @property
    def json(self):
        return (
            "add", {
                "doc": {
                    'id': 'ulic:' + self.sym + self.symul,
                    'parent': 'simc_' + self.sym,
                    'terc': self.terc,
                    'value': self.nazwa,
                    'cecha': self.cecha,
                    'symul': self.symul,
                    'wojewodztwo': self.woj,
                    'powiat': self.powiat,
                    'gmina': self.gmi,
                    'miejscowosc': self.miejscowosc
                }
            }
        )

    @property
    def woj(self):
        return teryt[self.terc[:2]].nazwa

    @property
    def powiat(self):
        return teryt[self.terc[:4]].nazwa

    @property
    def gmi(self):
        return teryt[self.terc].nazwa

    @property
    def miejscowosc(self):
        return simc[self.sym].nazwa

    @property
    def terc(self):
        return simc[self.sym].terc


class UlicMultiEntry(object):
    __log = logging.getLogger(__name__)

    def __init__(self, ulic: UlicEntry):
        self.symul = ulic.symul
        self.cecha = ulic.cecha
        self.nazwa = ulic.nazwa
        self.entries = {ulic.sym: ulic}

    def __getitem__(self, item: str):
        return self.entries[item]

    def add_entry(self, value: UlicEntry):
        if self.symul != value.symul:
            raise ValueError("Symul {0} different than expected {1}".format(value.symul, self.symul))

        if self.cecha != value.cecha:
            self.__log.info("Different CECHA {0} for street {1} in {2} [TERYT: {3}] than expected {4}".format(
                value.cecha, self.nazwa, value.miejscowosc, value.terc, self.cecha))

        if self.nazwa != value.nazwa:
            self.__log.info("Different NAZWA {0} for street {1} in {2} [TERYT: {3}] than expected {4}".format(
                value.nazwa, self.symul, value.miejscowosc, value.terc, self.nazwa
            ))

        self.entries[value.sym] = value

    @staticmethod
    def from_list(lst: typing.List[UlicEntry]):
        if len(lst) < 1:
            raise ValueError("At least one entry is needed")
        rv = UlicMultiEntry(lst[0])
        if len(lst) > 1:
            for entry in lst:
                rv.add_entry(entry)
        return rv


TYPE_DICT_ENTRIES = typing.TypeVar('V', TercEntry, BasicEntry, SimcEntry, UlicMultiEntry)


def _zip_read(binary: bytes) -> bytes:
    dictionary_zip = zipfile.ZipFile(io.BytesIO(binary))
    dicname = [x for x in dictionary_zip.namelist() if x.endswith(".xml")][0]
    return dictionary_zip.read(dicname)


def _row_as_dict(elem: ET.Element):
    return dict(
        (x.tag.lower(), x.text.strip()) for x in elem.iter() if x.text
    )


def _get_teryt_client():
    __log.info("Connecting to TERYT web service")
    wsdl = 'https://uslugaterytws1.stat.gov.pl/wsdl/terytws1.wsdl'
    wsse = UsernameToken('osmaddrtools', '#06JWOWutt4')
    return zeep.Client(wsdl=wsdl, wsse=wsse)


def __get_dict(fetcher: typing.Callable[[], bytes], cls: typing.ClassVar) -> typing.Iterable[TYPE_DICT_ENTRIES]:
    tree = ET.fromstring(_zip_read(fetcher()))
    return (cls(_row_as_dict(x)) for x in tree.find('catalog').iter('row'))


def __WMRODZ_binary() -> bytes:
    client = _get_teryt_client()
    data = client.service.PobierzDateAktualnegoKatSimc()
    __log.info("Downloading WMRODZ dictionary")
    dane = client.service.PobierzKatalogWMRODZ(data)
    __log.info("Downloading WMRODZ dictionary - done")
    return base64.decodebytes(dane.plik_zawartosc.encode('utf-8'))


def __wmrodz_create() -> typing.Dict[str, str]:
    return dict((x.rm, x.nazwa_rm) for x in __get_dict(__WMRODZ_binary, BasicEntry))


wmrodz = CachedDictionary('osm_teryt_wmrodz_v1', __wmrodz_create)


def __TERC_binary() -> bytes:
    client = _get_teryt_client()
    data = client.service.PobierzDateAktualnegoKatTerc()
    __log.info("Downloading TERC dictionary")
    dane = client.service.PobierzKatalogTERC(data)
    __log.info("Downloading TERC dictionary - done")
    return base64.decodebytes(dane.plik_zawartosc.encode('utf-8'))


def __teryt_create() -> typing.Dict[str, TercEntry]:
    return dict(((x.terc, x) for x in __get_dict(__TERC_binary, TercEntry)))


teryt = CachedDictionary('osm_teryt_teryt_v1', __teryt_create)


def __SIMC_binary() -> bytes:
    client = _get_teryt_client()
    data = client.service.PobierzDateAktualnegoKatSimc()
    __log.info("Downloading SIMC dictionary")
    dane = client.service.PobierzKatalogSIMC(data)
    __log.info("Downloading SIMC dictionary - done")
    return base64.decodebytes(dane.plik_zawartosc.encode('utf-8'))


def __simc_create() -> typing.Dict[str, SimcEntry]:
    return dict((x.sym, x) for x in __get_dict(__SIMC_binary, SimcEntry))


simc = CachedDictionary('osm_teryt_simc_v1', __simc_create)


def __ULIC_binary() -> bytes:
    client = _get_teryt_client()
    data = client.service.PobierzDateAktualnegoKatUlic()
    __log.info("Downloading ULIC dictionary")
    dane = client.service.PobierzKatalogULIC(data)
    __log.info("Downloading ULIC dictionary - done")
    return base64.decodebytes(dane.plik_zawartosc.encode('utf-8'))


def __ulic_create() -> typing.Dict[str, UlicMultiEntry]:
    grouped = groupby(__get_dict(__ULIC_binary, UlicEntry), lambda x: x.symul)
    return dict((key, UlicMultiEntry.from_list(value)) for key, value in grouped.items())


ulic = CachedDictionary('osm_teryt_ulic_v1', __ulic_create)
