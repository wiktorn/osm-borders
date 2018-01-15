import base64
import calendar
import datetime
import functools
import io
import logging
import time
import typing
import zipfile
from xml.etree import ElementTree as ET
from xml.etree.ElementTree import Element
from xml.etree.ElementTree import tostring

import tqdm
import zeep
from google.protobuf.message import Message
from zeep.wsse.username import UsernameToken

from .teryt_pb2 import \
    TercEntry as TercEntry_pb, \
    SimcEntry as SimcEntry_pb, \
    UlicMultiEntry as UlicMultiEntry_pb
from .tools import groupby, get_cache_manager, CacheExpired, ProtoSerializer, Cache

TERYT_SIMC_DB = 'osm_teryt_simc_v1'

TERYT_WMRODZ_DB = 'osm_teryt_wmrodz_v1'

TERYT_TERYT_DB = 'osm_teryt_teryt_v1'

TERYT_ULIC_DB = 'osm_teryt_ulic_v1'

Version = typing.NewType('Version', int)

T = typing.TypeVar('T')


def ensure_2_digits(o) -> str:
    return "{0:02}".format(int(o))


def _zip_read(binary: bytes) -> bytes:
    dictionary_zip = zipfile.ZipFile(io.BytesIO(binary))
    dicname = [x for x in dictionary_zip.namelist() if x.endswith(".xml")][0]
    return dictionary_zip.read(dicname)


def _row_as_dict(elem: ET.Element) -> typing.Dict[str, str]:
    return dict(
        (x.tag.lower(), x.text.strip()) for x in elem.iter() if x.text
    )


def _get_teryt_client() -> zeep.Client:
    __log = logging.getLogger(__name__ + '.get_teryt_client')
    __log.info("Connecting to TERYT web service")
    wsdl = 'https://uslugaterytws1.stat.gov.pl/wsdl/terytws1.wsdl'
    wsse = UsernameToken('osmaddrtools', '#06JWOWutt4')
    return zeep.Client(wsdl=wsdl, wsse=wsse)


def _get_dict(data: bytes, cls: typing.Type[T]) -> typing.Iterable[T]:
    tree = ET.fromstring(data)
    return (cls(_row_as_dict(x)) for x in tree.find('catalog').iter('row'))


def update_record_to_dict(obj: Element, suffix: str, exceptions: typing.Iterable[str] = ()) -> typing.Dict[str, str]:
    all_tags = dict((x.tag, x.text.strip() if x.text else x.text) for x in obj.iter())

    ret = dict(
        (key.lower()[:-len(suffix)], value) for key, value in all_tags.items() if key.endswith(suffix)
    )

    for key in exceptions:
        ret[key.lower()] = all_tags[key]

    return ret


def nvl(obj, substitute):
    if isinstance(obj, type(None)):
        return substitute
    return obj


def _int_to_datetime(x: Version) -> datetime.date:
    return datetime.date.fromtimestamp(x)


def _date_to_int(version: datetime.date) -> Version:
    return calendar.timegm(version.timetuple())


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


class ToFromJsonSerializer(ProtoSerializer, typing.Generic[T]):
    __log = logging.getLogger(__name__ + '.ToFromJsonSerializer')

    def __init__(self, cls: typing.Type[T], pb_cls):
        super(ToFromJsonSerializer, self).__init__(pb_cls)
        self.cls = cls

    def deserialize(self, data: bytes) -> T:
        self.__log.debug("deserialize input data %s", str(data))
        deser = super(ToFromJsonSerializer, self).deserialize(data)
        self.__log.debug("deserialize after deserialize: %s", str(deser))
        ret = self.cls.from_dict(deser)
        self.__log.debug("deserialize return value: %s", ret)
        return ret
        # return self.cls.from_dict(super(ToFromJsonSerializer, self).deserialize(data))

    def serialize(self, dct: T) -> bytes:
        self.__log.debug("serialize input data %s", str(dct))
        as_dct = self.cls.to_dict(dct)
        self.__log.debug("serialize input as dict: %s", str(as_dct))
        ret = super(ToFromJsonSerializer, self).serialize(as_dct)
        self.__log.debug("serialize ret: %s", str(ret))
        self.__log.debug("deserialized: %s", self.deserialize(ret))
        if self.deserialize(ret) != dct:
            raise AssertionError("ret != dct")
        return ret


class TercEntry(object):
    __log = logging.getLogger(__name__ + '.TercEntry')

    def __init__(self, dct: typing.Dict[str, str]):
        self.woj = dct.get('woj')
        self.powiat = dct.get('pow')
        self.gmi = dct.get('gmi')
        self.rodz = dct.get('rodz')
        self.nazwadod = nvl(dct.get('nazwadod'), '')
        self.nazwa = dct.get('nazwa')

    def __str__(self):
        return "TercEntry({{woj: {}, pow: {}, gmi: {}, rodz: {}, nazwadod: {}, nazwa: {}}})".format(
            self.woj, self.powiat, self.gmi, self.rodz, self.nazwadod, self.nazwa
        )

    def update_from(self, other: typing.Dict[str, str]):
        for attr in ('woj', 'pow', 'rodz', 'nazwadod', 'nazwa'):
            new_val = other.get(attr)
            if new_val:
                setattr(self, attr, new_val)

    @property
    def cache_key(self):
        return self.terc

    @property
    def rodz_nazwa(self):
        return _TERC_RODZAJ_MAPPING.get(self.rodz) if self.rodz else {2: 'województwo', 4: 'powiat'}[len(self.terc)]

    @property
    def terc_base(self) -> typing.Iterable[str]:
        return (y for y in
                (self.woj, self.powiat) + ((self.gmi, self.rodz) if self.gmi else ())
                if y
                )

    @property
    def terc(self) -> str:
        return "".join(y for y in
                       (self.woj, self.powiat) + ((self.gmi, self.rodz) if self.gmi else ())
                       if y
                       )

    @property
    def parent_terc(self) -> str:
        if self.gmi:
            return "".join((self.woj, self.powiat))
        if self.powiat:
            return "".join((self.woj,))
        return ""

    @property
    def solr_json(self) -> tuple:
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

    def to_dict(self) -> typing.Dict[str, str]:
        ret = {
            'woj': int(self.woj),
            'nazwadod': self.nazwadod,
            'nazwa': self.nazwa
        }
        if self.powiat:
            ret['powiat'] = int(self.powiat)
        if self.gmi:
            ret['gmi'] = int(self.gmi)
            ret['rodz'] = int(self.rodz)
        return ret

    @staticmethod
    def from_dict(dct: dict) -> 'TercEntry':
        ret = TercEntry({
            'woj': ensure_2_digits(dct['woj']),
            'pow': ensure_2_digits(dct['powiat']) if dct.get('powiat') else '',
            'gmi': ensure_2_digits(dct['gmi']) if dct.get('gmi') else '',
            'rodz': "{0:1}".format(int(dct['rodz'])) if dct.get('rodz') else '',
            'nazwadod': dct.get('nazwadod'),
            'nazwa': dct['nazwa']
        })
        TercEntry.__log.debug("From dictionary: {} created {}".format(dct, str(ret)))
        return ret

    @staticmethod
    def from_update_dict(dct: dict) -> 'TercEntry':
        ret = TercEntry({
            'woj': ensure_2_digits(dct['woj']),
            'pow': ensure_2_digits(dct['pow']) if dct.get('pow') else '',
            'gmi': ensure_2_digits(dct['gmi']) if dct.get('gmi') else '',
            'rodz': "{0:1}".format(int(dct['rodz'])) if dct.get('rodz') else '',
            'nazwadod': dct.get('nazwadod'),
            'nazwa': dct['nazwa']
        })
        TercEntry.__log.debug("From dictionary: {} created {}".format(dct, str(ret)))
        return ret

    def __eq__(self, other):
        if isinstance(other, TercEntry):
            return self.to_dict() == other.to_dict()
        return False


class SimcEntry(object):
    """
    Full dump looks like:
        <row>
          <WOJ>06</WOJ>
          <POW>04</POW>
          <GMI>05</GMI>
          <RODZ_GMI>2</RODZ_GMI>
          <RM>01</RM>
          <MZ>1</MZ>
          <NAZWA>Borsuk</NAZWA>
          <SYM>0894990</SYM>
          <SYMPOD>0894990</SYMPOD>
          <STAN_NA>2018-01-02</STAN_NA>
        </row>

        Sample zmiana object:
<zmiana>
    <TypKorekty>D</TypKorekty>
    <Identyfikator>1067325</Identyfikator>
    <WojPrzed>
    </WojPrzed>
    <PowPrzed>
    </PowPrzed>
    <GmiPrzed>
    </GmiPrzed>
    <RodzPrzed>
    </RodzPrzed>
    <NazwaPrzed>
    </NazwaPrzed>
    <RodzajMiejscowosciPrzed>
    </RodzajMiejscowosciPrzed>
    <CzyNazwaZwyczajowaPrzed>
    </CzyNazwaZwyczajowaPrzed>
    <IdentyfikatorMiejscowosciPodstawowejPrzed>
    </IdentyfikatorMiejscowosciPodstawowejPrzed>
    <StanPrzed>2017-01-01</StanPrzed>
    <WojPo>12</WojPo>
    <PowPo>10</PowPo>
    <GmiPo>15</GmiPo>
    <RodzPo>2</RodzPo>
    <NazwaPo>Potok Kordowiec</NazwaPo>
    <RodzajMiejscowosciPo>00</RodzajMiejscowosciPo>
    <CzyNazwaZwyczajowaPo>1</CzyNazwaZwyczajowaPo>
    <IdentyfikatorMiejscowosciPodstawowejPo>0459550</IdentyfikatorMiejscowosciPodstawowejPo>
    <WyodrebnionoZIdentyfikatora1>
    </WyodrebnionoZIdentyfikatora1>
    <WyodrebnionoZIdentyfikatora2>
    </WyodrebnionoZIdentyfikatora2>
    <WyodrebnionoZIdentyfikatora3>
    </WyodrebnionoZIdentyfikatora3>
    <WyodrebnionoZIdentyfikatora4>
    </WyodrebnionoZIdentyfikatora4>
    <WlaczonoDoIdentyfikatora1>
    </WlaczonoDoIdentyfikatora1>
    <WlaczonoDoIdentyfikatora2>
    </WlaczonoDoIdentyfikatora2>
    <WlaczonoDoIdentyfikatora3>
    </WlaczonoDoIdentyfikatora3>
    <WlaczonoDoIdentyfikatora4>
    </WlaczonoDoIdentyfikatora4>
    <StanPo>2018-01-01</StanPo>
  </zmiana>

    """
    __log = logging.getLogger(__name__ + '.SimcEntry')

    def __init__(self, dct: dict = None):
        self.terc = None
        self.nazwa = None

        if dct:
            self.terc = dct.get('woj') + dct.get('pow') + dct.get('gmi') + dct.get('rodz_gmi')
            self.rm_id = dct.get('rm')
            self.nazwa = dct.get('nazwa')
            self.sym = dct.get('sym')
            self.parent = None
            if dct.get('sym') != dct.get('sympod'):
                self.parent = dct.get('sympod')
        else:
            self.terc = None
            self.rm_id = None
            self.nazwa = None
            self.sym = None
            self.parent = None

    def __str__(self):
        return "SimcEntry({{terc: {}, rm_id: {}, nazwa: {}, sym: {}, parent: {}}})".format(
            self.terc, self.rm_id, self.nazwa, self.sym, self.parent
        )

    @property
    def cache_key(self):
        return self.sym

    def to_dict(self) -> typing.Dict[str, str]:
        ret = {
            'terc': int(self.terc),
            'rm': int(self.rm_id),
            'nazwa': self.nazwa,
            'sym': int(self.sym),
        }
        if self.parent:
            ret['parent'] = int(self.parent)
        return ret

    @staticmethod
    def from_dict(dct: dict) -> 'SimcEntry':
        ret = SimcEntry()
        ret.terc = "{0:07}".format(dct['terc'])
        ret.rm_id = ensure_2_digits(dct.get('rm', 0))
        ret.nazwa = dct['nazwa']
        ret.sym = "{0:07}".format(dct['sym'])
        ret.parent = "{0:07}".format(dct['parent']) if 'parent' in dct else None
        SimcEntry.__log.debug("[from_dict]: From dictionary: {} created {}".format(dct, str(ret)))
        return ret

    @staticmethod
    def from_update_dict(dct: dict) -> 'SimcEntry':
        ret = SimcEntry()
        ret.terc = ensure_2_digits(dct['woj']) + ensure_2_digits(dct['pow']) + ensure_2_digits(dct['gmi']) + \
                   "{0:1}".format(dct['rodz'])
        ret.rm_id = ensure_2_digits(dct.get('rodzajmiejscowosci', 0))
        ret.nazwa = dct.get('nazwa')
        ret.sym = "{0:07}".format(int(dct['identyfikator']))
        ret.parent = "{0:07}".format(int(dct['identyfikatormiejscowoscipodstawowej'])) if \
            'identyfikatormiejscowoscipodstawowej' in dct else None
        if ret.sym == ret.parent:
            ret.parent = None
        SimcEntry.__log.debug("[from_update_dict] From dictionary: {} created {}".format(dct, str(ret)))
        return ret

    def __eq__(self, other):
        if isinstance(other, SimcEntry):
            return self.to_dict() == other.to_dict()
        return False

    @property
    def solr_json(self) -> tuple:
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
    def gmi(self) -> str:
        return teryt()[self.terc].nazwa

    @property
    def woj(self) -> str:
        return teryt()[self.terc[:2]].nazwa

    @property
    def powiat(self) -> str:
        return teryt()[self.terc[:4]].nazwa

    @property
    def rm(self) -> str:
        return wmrodz()[self.rm_id]

    def update_from(self, new: typing.Dict[str, str]):
        if 'woj' in new:
            self.terc = ensure_2_digits(new['woj']) + ensure_2_digits(new['pow']) + ensure_2_digits(new['gmi']) + \
                        "{0:1}".format(new['rodz'])

        if 'rodzajmiejscowosci' in new:
            self.rm_id = ensure_2_digits(new.get('rodzajmiejscowosci', 0))
        if 'nazwa' in new:
            self.nazwa = new['nazwa']

        if 'identyfikatormiejscowoscipodstawowej' in new:
            self.parent = str(new['identyfikatormiejscowoscipodstawowej'])
            if self.parent == self.sym:
                self.parent = None


class BasicEntry(object):
    def __init__(self, dct):
        for i in dct.keys():
            setattr(self, i, dct[i])


def _clean_street_name(cecha: str, nazwa1: str, nazwa2: str) -> str:
    def mapper(name: str):
        if name and name.casefold().startswith(cecha.casefold()):
            return name[len(cecha):].strip()
        elif name and name.casefold().startswith(_ULIC_CECHA_MAPPING[cecha.upper()].casefold()):
            return name[len(_ULIC_CECHA_MAPPING.get(cecha.upper())):].strip()
        return name.strip() if isinstance(name, str) else name

    nazwa1 = mapper(nazwa1)
    nazwa2 = mapper(nazwa2)
    if not nazwa1 and not nazwa2:
        return ""
    return " ".join((x for x in (_ULIC_CECHA_MAPPING.get(cecha.upper()), nazwa1, nazwa2) if x))


class UlicEntry(object):
    __log = logging.getLogger(__name__ + '.UlicEntry')
    _init_to_update_map = {
        'woj': 'woj',
        'pow': 'pow',
        'gmi': 'gmi',
        'rodz_gmi': 'rodz',
        'sym': 'identyfikatormiejscowosci',
        'sym_ul': 'identyfikatornazwyulicy',
        'cecha': 'cecha',
        'nazwa_1': 'nazwa1',
        'nazwa_2': 'nazwa2',
        'stan_na': 'stan',
    }
    _update_to_init_map = dict((v, k) for (k, v) in _init_to_update_map.items())

    def __init__(self, dct: typing.Dict[str, str]):
        self.sym = dct.get('sym')
        self.sym_ul = dct.get('sym_ul')
        self.cecha_orig = dct.get('cecha')
        self.nazwa_1 = nvl(dct.get('nazwa_1'), '')
        self.nazwa_2 = nvl(dct.get('nazwa_2'), '')
        self.terc = dct.get('woj') + dct.get('pow') + dct.get('gmi') + dct.get('rodz_gmi')
        # assert self.terc == dct.get('woj') + dct.get('pow') + dct.get('gmi') + \
        #    dct.get('rodz_gmi'), "City terc code: {0} != {1} (terc code from ulic".format(
        #    self.terc, dct.get('woj') + dct.get('pow') + dct.get('gmi') + dct.get('rodz_gmi'))

    @property
    def nazwa_1(self):
        return self._nazwa_1

    @nazwa_1.setter
    def nazwa_1(self, value):
        if value is None or value.endswith(' '):
            raise ValueError("Invalid value: {}".format(value))
        self._nazwa_1 = value

    @property
    def nazwa_2(self):
        return self._nazwa_2

    @nazwa_2.setter
    def nazwa_2(self, value):
        if value is None or value.endswith(' '):
            raise ValueError("Invalid value: {}".format(value))
        self._nazwa_2 = value

    def __str__(self):
        return "UlicEntry({{sym: {}, symul: {}, cecha_orig: {}, nazwa_1: {}, nazwa_2: {}, terc: {}}})".format(
            self.sym, self.sym_ul, self.cecha_orig, self.nazwa_1, self.nazwa_2, self.terc
        )

    @property
    def cache_key(self):
        return self.sym_ul

    def update_from(self, obj: typing.Dict[str, str]):
        dct = dict(
            (UlicEntry._update_to_init_map.get(k, k), v) for (k, v) in obj.items()
        )
        for attr in ('sym', 'sym_ul'):
            new_value = dct.get(attr)
            if new_value:
                setattr(self, attr, new_value)
        if dct.get('cecha'):
            self.cecha_orig = dct.get('cecha')
        # undocumented but looks like needs to get all nazwa elements on name change
        if any(dct.get(x) for x in ('nazwa_1', 'nazwa_2')):
            for attr in ('nazwa_1', 'nazwa_2'):
                setattr(self, attr, nvl(dct.get(attr, ''), ''))
        if any(dct.get(x) for x in ('woj', 'pow', 'gmi', 'rodz_gmi')):
            self.terc = dct.get('woj') + dct.get('pow') + dct.get('gmi') + dct.get('rodz_gmi')

    def to_dict(self) -> typing.Dict[str, str]:
        return {
            'sym': int(self.sym),
            'symul': int(self.sym_ul),
            'cecha': self.cecha_orig,
            'nazwa_1': self.nazwa_1,
            'nazwa_2': self.nazwa_2,
            'terc': int(self.terc)
        }

    @staticmethod
    def from_dict(dct: dict) -> 'UlicEntry':
        terc = "{0:07}".format(dct['terc'])
        return UlicEntry({
            'sym': "{0:07}".format(int(dct['sym'])),
            'sym_ul': "{0:05}".format(int(dct['symul'])),
            'cecha': dct['cecha'],
            'nazwa_1': dct['nazwa_1'],
            'nazwa_2': nvl(dct.get('nazwa_2'), ''),
            'woj': terc[:2],
            'pow': terc[2:4],
            'gmi': terc[4:6],
            'rodz_gmi': terc[6]
        })

    @staticmethod
    def from_update(dct: dict) -> 'UlicEntry':
        ret = UlicEntry(dict(
            (UlicEntry._update_to_init_map.get(k, k), v) for (k, v) in dct.items()
        ))
        UlicEntry.__log.debug("From dictionary: {} created {}".format(dct, str(ret)))
        return ret

    @property
    def solr_json(self) -> tuple:
        return (
            "add", {
                "doc": {
                    'id': 'ulic:' + self.sym + self.sym_ul,
                    'parent': 'simc_' + self.sym,
                    'terc': self.terc,
                    'value': self.nazwa,
                    'cecha': self.cecha,
                    'symul': self.sym_ul,
                    'wojewodztwo': self.woj,
                    'powiat': self.powiat,
                    'gmina': self.gmi,
                    'miejscowosc': self.miejscowosc
                }
            }
        )

    def __eq__(self, other):
        if isinstance(other, UlicEntry):
            return self.to_dict() == other.to_dict()
        return False

    @property
    def woj(self) -> str:
        return teryt()[self.terc[:2]].nazwa

    @property
    def powiat(self) -> str:
        return teryt()[self.terc[:4]].nazwa

    @property
    def gmi(self) -> str:
        return teryt()[self.terc].nazwa

    @property
    def miejscowosc(self) -> str:
        return simc()[self.sym].nazwa

    @property
    def nazwa(self) -> str:
        return _clean_street_name(self.cecha_orig, self.nazwa_2, self.nazwa_1)

    @property
    def cecha(self) -> str:
        return _ULIC_CECHA_MAPPING[self.cecha_orig.upper()]


class UlicMultiEntry(object):
    __log = logging.getLogger(__name__ + '.UlicMultiEntry')

    def __init__(self, entry: UlicEntry):
        self.sym_ul = entry.sym_ul
        self.cecha = entry.cecha
        self.nazwa = entry.nazwa
        self.entries = {entry.sym: entry}

    def __str__(self):
        return "UlicMultiEntry({{symul: {}, cecha: {}, nazwa: {}, entries: {}}})".format(
            self.sym_ul,
            self.cecha,
            self.nazwa,
            "{" + ", ".join((str(k) + ": " + str(v)) for (k, v) in self.entries.items()) + "}")

    def __getitem__(self, item: str):
        return self.entries[item]

    @property
    def cache_key(self):
        return self.sym_ul

    def add_entry(self, value: UlicEntry):
        if self.sym_ul != value.sym_ul:
            raise ValueError("Symul {0} different than expected {1}".format(value.sym_ul, self.sym_ul))

        if self.cecha != value.cecha:
            self.__log.info("Different CECHA {0} for street {1} in {2} [TERYT: {3}] than expected {4}".format(
                value.cecha, self.nazwa, value.miejscowosc, value.terc, self.cecha))

        if self.nazwa != value.nazwa:
            self.__log.info("Different NAZWA {0} for street {1} in {2} [TERYT: {3}] than expected {4}".format(
                value.nazwa, self.sym_ul, value.miejscowosc, value.terc, self.nazwa
            ))

        self.entries[value.sym] = value

    def get_by_sym(self, sym: str) -> UlicEntry:
        return self.entries[sym]

    def remove_by_sym(self, sym: str):
        del self.entries[sym]

    def __len__(self) -> int:
        return len(self.entries)

    def get_all(self) -> typing.Iterable[UlicEntry]:
        return self.entries.values()

    def to_dict(self) -> typing.Dict[str, str]:
        if not all(x.sym_ul == self.sym_ul for x in self.entries.values()):
            raise ValueError("Inconsistent object")
        return {
            'symul': int(self.sym_ul),
            'cecha': self.cecha,
            'nazwa': self.nazwa,
            'entries': [self.entries[key].to_dict() for key in sorted(self.entries.keys())]
        }

    def __eq__(self, other):
        if isinstance(other, UlicMultiEntry):
            return self.to_dict() == other.to_dict()
        return False

    @staticmethod
    def from_dict(dct: dict) -> 'UlicMultiEntry':
        ret = UlicMultiEntry.from_list([UlicEntry.from_dict(x) for x in dct['entries']])

        assert int(ret.sym_ul) == dct['symul']
        assert ret.cecha == dct.get('cecha') or (not ret.cecha and not dct.get('cecha')) or ret.cecha is None
        assert ret.nazwa == dct.get('nazwa') or (not ret.nazwa and not dct.get('nazwa')) or ret.nazwa is None
        return ret

    @staticmethod
    def from_list(lst: typing.List[UlicEntry]) -> 'UlicMultiEntry':
        if len(lst) < 1:
            raise ValueError("At least one entry is needed")
        rv = UlicMultiEntry(lst[0])
        if len(lst) > 1:
            for entry in lst:
                rv.add_entry(entry)
        return rv

    def update_from_entries(self):
        new_cecha = None
        new_nazwa = None
        for ulic_entry in self.entries.values():
            assert not (new_cecha and new_cecha != ulic_entry.cecha)
            assert not (new_nazwa and new_nazwa != ulic_entry.nazwa)
            new_cecha = ulic_entry.cecha
            new_nazwa = ulic_entry.nazwa
        self.cecha = new_cecha
        self.nazwa = new_nazwa


def _wmrodz_binary(version: datetime.date) -> bytes:
    __log = logging.getLogger(__name__ + '._wmrodz_binary')
    client = _get_teryt_client()
    __log.info("Downloading WMRODZ dictionary")
    dane = client.service.PobierzKatalogWMRODZ(version)
    __log.info("Downloading WMRODZ dictionary - done")
    return _zip_read(base64.decodebytes(dane.plik_zawartosc.encode('utf-8')))


def __wmrodz_create():
    __log = logging.getLogger(__name__ + '._wmrodz_create')
    version = TerytCache().cache_version()
    data = _wmrodz_binary(_int_to_datetime(version))
    cache = get_cache_manager().create_cache(TERYT_WMRODZ_DB)
    cache.reload(dict((x.rm, x.nazwa_rm) for x in _get_dict(data, BasicEntry)))
    get_cache_manager().mark_ready(TERYT_WMRODZ_DB, version)
    __log.info("WMRODZ dictionary created")


def wmrodz() -> Cache[str]:
    return get_cache_manager().get_cache(TERYT_WMRODZ_DB)


class BaseTerytCache(typing.Generic[T]):
    __log = logging.getLogger(__name__ + '.BaseTerytCache')
    change_handlers = dict()

    def __init__(self, path: str, entry_class: typing.Type[T], protobuf_class: typing.Type[Message]):
        self.entry_class = entry_class
        self.protobuf_class = protobuf_class
        self.path = path
        self.version_ttl = 0
        self._version = Version(0)

    def _get_cache(self, cache_version: Version = None) -> Cache[T]:
        if cache_version:
            return get_cache_manager().get_cache(
                self.path,
                version=cache_version,
                serializer=ToFromJsonSerializer(self.entry_class, self.protobuf_class)
            )
        return get_cache_manager().get_cache(
            self.path,
            serializer=ToFromJsonSerializer(SimcEntry, SimcEntry_pb)
        )

    def get(self, allow_stale: bool = False, version: int = None) -> Cache[T]:
        if not version:
            version = self.cache_version()
        try:
            return self._get_cache(version)
        except CacheExpired:
            if allow_stale:
                self.__log.warning("Using stale version of cache: %s (%s). Consider updating.",
                                   self.path, self.entry_class)
                return self._get_cache(cache_version=-1)
            cache_version = get_cache_manager().version(self.path)
            current_version = version
            self._cache_update(_int_to_datetime(cache_version), _int_to_datetime(current_version))
            self.__log.info("Updated dictionary %s from %s to %s", self.path, cache_version, current_version)
            get_cache_manager().mark_ready(self.path, current_version)
            return self.get()

    def cache_version(self) -> Version:
        if time.time() > self.version_ttl:
            self._version = _date_to_int(self._real_version_call())
            self.version_ttl = time.time() + 3600
        return self._version

    def _get_binary(self, version: Version) -> bytes:
        self.__log.info("Downloading %s dictionary", self.path)
        dane = self._get_binary_real_call(_int_to_datetime(version))
        self.__log.info("Downloading %s dictionary - done", self.path)
        return _zip_read(base64.decodebytes(dane.plik_zawartosc.encode('utf-8')))

    def create_cache(self, version: int = None, data=None):
        if not version:
            version = self.cache_version()
        if not data:
            data = self._get_binary(version)
        # with open("/tmp/test_data_{}_{}".format(self.path, version), "wb+") as f:
        #    f.write(data)
        cache = get_cache_manager().create_cache(self.path,
                                                 serializer=ToFromJsonSerializer(self.entry_class, self.protobuf_class))
        cache.reload(self._data_to_cache_contents(data))
        get_cache_manager().mark_ready(self.path, version)
        self.__log.info("%s dictionary created", self.path)

    def _data_to_cache_contents(self, data: bytes):
        return dict((x.cache_key, x) for x in _get_dict(data, self.entry_class))

    def _cache_update_binary(self, start: datetime.date, end: datetime.date) -> bytes:
        """
        :param start:
        :param end:
        :return:

        """
        self.__log.info("Downloading %s dictionary update from: %s to %s", self.path, start, end)
        dane = self._get_update_real_call(start, end)
        self.__log.info("Downloading %s dictionary update - done", self.path)
        return _zip_read(base64.decodebytes(dane.plik_zawartosc.encode('utf-8')))

    def _cache_update(self, cache_version: datetime.date, current_version: datetime.date):
        data = self._cache_update_binary(cache_version, current_version)
        # with open("/tmp/test_data_update_{}_from_{}_to_{}".format(
        #         self.path,
        #         cache_version,
        #         current_version)
        #         , "wb+") as f:
        #     f.write(data)
        tree = ET.fromstring(data)
        cache = self._get_cache(_date_to_int(cache_version))

        for zmiana in tree.iter('zmiana'):
            operation = zmiana.find('TypKorekty').text
            handler = self.change_handlers.get(operation)
            if not handler:
                raise ValueError("Unkown TypKorekty: %s, expected one of: %s.",
                                 operation,
                                 ", ".join(self.change_handlers.keys()))
            handler(self, cache, zmiana)

    def _real_version_call(self) -> datetime.date:
        raise NotImplementedError

    def _get_binary_real_call(self, version: datetime.date):  # TODO: return type
        raise NotImplementedError

    def _get_update_real_call(self, start: datetime.date, end: datetime.date):  # TODO: return type
        raise NotImplementedError

    def verify(self):
        cache = self._get_cache(cache_version=-1)
        errors = 0
        for key, value in tqdm.tqdm(self._data_to_cache_contents(self._get_binary(self.cache_version())).items()):
            cache_entry = cache.get(key)
            if not cache_entry == value:
                self.__log.warning(
                    "Elements doesn't match: (original) {} != {} (cache)".format(str(value), str(cache_entry)))
                errors += 1
        self.__log.error("Total mismatched elements: {}".format(errors))
        if errors > 0:
            raise ValueError("Some elements didn't match")


class SimcCache(BaseTerytCache):
    __log = logging.getLogger(__name__ + '.SimcCache')

    def __init__(self):
        super(SimcCache, self).__init__(TERYT_SIMC_DB, SimcEntry, SimcEntry_pb)

    def _handle_d(self, cache: Cache[SimcEntry], obj: Element):
        """
        D - dopisanie nowej miejscowości
            - wypełnione wszystkie pola "po modyfikacji"
            - brak pól "przed modyfikacją"
        :param obj:
        :return:
        """
        self.__log.debug("handle_d object: %s", tostring(obj, 'utf-8').decode('utf-8'))
        new = SimcEntry.from_update_dict(update_record_to_dict(obj, 'Po', ('Identyfikator',)))

        cache.add(new.sym, new)

    def _handle_u(self, cache: Cache[SimcEntry], obj: Element):
        """
        U - usunięcie istniejącej miejscowości
            - wypełnione wszystkie pola "przed modyfikacją"
        """
        self.__log.debug("handle_u object: %s", tostring(obj, 'utf-8').decode('utf-8'))
        old = SimcEntry.from_update_dict(update_record_to_dict(obj, 'Przed', ('Identyfikator',)))
        cache.delete(old.sym)

    def _handle_z(self, cache: Cache[SimcEntry], obj: Element):
        """
            Z - zmiana atrybutów dla istniejącej miejscowości
                - wypełnione tylko te pola "po modyfikacji", które się zmieniły
                - wypełnione wszystkie pola "przed modyfikacją"
        :param obj:
        :return:
        """
        self.__log.debug("handle_z object: %s", tostring(obj, 'utf-8').decode('utf-8'))
        old = SimcEntry.from_update_dict(update_record_to_dict(obj, 'Przed', ('Identyfikator',)))

        cache_entry = cache.get(old.sym)
        if cache_entry:
            cache_entry.update_from(update_record_to_dict(obj, 'Po', ('Identyfikator',)))
            if old.sym != cache_entry.sym:
                cache.delete(old.sym)
            cache.add(cache_entry.sym, cache_entry)
        else:
            # TODO: issue warning
            # raise ValueError("Modification of non-existing record: {}".format(str(old)))
            pass

    def _handle_p(self, cache: Cache[SimcEntry], obj: Element):
        """
            P - przeniesienie miejscowości do innej jednostki administracyjnej (województwa, powiatu, gminy)
                - wypełnione tylko te pola "po modyfikacji", które się zmieniły, choć w przypadku zmiany identyfikatora
                    gminy,uzupełnione jest wszystko
                - wypełnione wszystkie pola "przed modyfikacją"
        :param obj:
        :return:
        """
        self._handle_z(cache, obj)

    change_handlers = {
        'D': _handle_d,
        'U': _handle_u,
        'Z': _handle_z,
        'P': _handle_p,
    }

    def _real_version_call(self) -> datetime.date:
        return _get_teryt_client().service.PobierzDateAktualnegoKatSimc()

    def _get_binary_real_call(self, version: datetime.date):  # TODO: return type
        self.__log.debug("_get_binary_real_call(version=%s)", version)
        return _get_teryt_client().service.PobierzKatalogSIMC(version)

    def _get_update_real_call(self, start: datetime.date, end: datetime.date):  # TODO: return type
        self.__log.debug("_get_update_real_call(start=%s, end=%s)", start, end)
        return _get_teryt_client().service.PobierzZmianySimcUrzedowy(start, end)


@functools.lru_cache(maxsize=1)
def simc() -> Cache[SimcEntry]:
    return SimcCache().get()


class TerytCache(BaseTerytCache):
    __log = logging.getLogger(__name__ + '.TerytCache')

    def __init__(self):
        super(TerytCache, self).__init__(TERYT_TERYT_DB, TercEntry, TercEntry_pb)

    def _handle_d(self, cache: Cache[TercEntry], obj: Element):
        """
        D - dopisanie nowej jednostki
            - wypełnione wszystkie pola "po modyfikacji"
            - brak pól "przed modyfikacją"
        :param obj:
        :return:
        """
        self.__log.debug("handle_d object: %s", tostring(obj, 'utf-8').decode('utf-8'))
        new = TercEntry.from_update_dict(update_record_to_dict(obj, 'Po'))
        cache.add(new.terc, new)

    def _handle_u(self, cache: Cache[TercEntry], obj: Element):
        """
        U - usunięcie istniejącej jednostki i dołączenie do innej
            - wypełnione wszystkie pola "przed modyfikacją"
        """
        self.__log.debug("handle_u object: %s", tostring(obj, 'utf-8').decode('utf-8'))
        old = TercEntry.from_update_dict(update_record_to_dict(obj, 'Przed'))
        cache.delete(old.terc)

    def _handle_m(self, cache: Cache[TercEntry], obj: Element):
        """
            M - zmiana nazwy lub/i identyfikatora
                - wypełnione tylko te pola "po modyfikacji", które się zmieniły
                - wypełnione wszystkie pola "przed modyfikacją"
        :param obj:
        :return:
        """
        self.__log.debug("handle_m object: %s", tostring(obj, 'utf-8').decode('utf-8'))
        old = TercEntry.from_update_dict(update_record_to_dict(obj, 'Przed'))

        cache_entry = cache.get(old.terc)
        if cache_entry:
            cache_entry.update_from(update_record_to_dict(obj, 'Po'))
            if old.terc != cache_entry.terc:
                cache.delete(old.terc)
            cache.add(cache_entry.terc, cache_entry)
        else:
            # TODO: issue warning
            # raise ValueError("Modification of non-existing record: {}".format(str(old)))
            pass

    change_handlers = {
        'D': _handle_d,
        'U': _handle_u,
        'M': _handle_m,
    }

    def _real_version_call(self) -> datetime.date:
        return _get_teryt_client().service.PobierzDateAktualnegoKatTerc()

    def _get_binary_real_call(self, version: datetime.date):  # TODO: return type
        return _get_teryt_client().service.PobierzKatalogTERC(version)

    def _get_update_real_call(self, start: datetime.date, end: datetime.date):  # TODO: return type
        return _get_teryt_client().service.PobierzZmianyTercUrzedowy(start, end)


@functools.lru_cache(maxsize=1)
def teryt() -> Cache[TercEntry]:
    return TerytCache().get(allow_stale=True)


class UlicCache(BaseTerytCache):
    __log = logging.getLogger(__name__ + '.UlicCache')

    def __init__(self):
        super(UlicCache, self).__init__(TERYT_ULIC_DB, UlicMultiEntry, UlicMultiEntry_pb)

    def _handle_d(self, cache: Cache[UlicMultiEntry], obj: Element):
        """
            D - dopisanie nowej ulicy
            - przed zmianą powinno być puste
            - po zmianie powinno być w całości uzupełnione

        :param cache:
        :param obj:
        :return:
        """
        self.__log.debug("UlicCache.handle_d: Processing element: %s", tostring(obj, 'utf-8').decode('utf-8'))

        to = UlicEntry.from_update(update_record_to_dict(obj, 'Po'))
        cache_entry = cache.get(to.sym_ul)
        if cache_entry:
            cache_entry.add_entry(to)
            cache.add(to.sym_ul, cache_entry)
        else:
            cache.add(to.sym_ul, UlicMultiEntry(to))

    def _handle_m(self, cache: Cache[UlicMultiEntry], obj: Element):
        """
            M - zmiana parametrów ulicy
                - przed zmianą powinno być w całości wypełnione
                - po zmianie - tylko to, co się zmieniło
        :param cache:
        :param obj:
        :return:
        """
        self.__log.debug("UlicCache.handle_m: Processing element: %s", tostring(obj, 'utf-8').decode('utf-8'))
        old = UlicEntry.from_update(update_record_to_dict(obj, 'Przed'))
        self.__log.debug("Old object: %s", old)
        if old.sym_ul:
            cache_entry = cache.get(old.sym_ul)
        else:
            cache_entry = None
        if cache_entry:
            self.__log.debug("Cache entry: %s", cache_entry)
            ulic_entry = cache_entry.get_by_sym(old.sym)
            ulic_entry.update_from(update_record_to_dict(obj, 'Po'))
            self.__log.debug(old)
            self.__log.debug(ulic_entry)

            if ulic_entry.sym != old.sym:
                cache_entry.add_entry(ulic_entry)
                cache_entry.remove_by_sym(old.sym)

            if old.sym_ul != ulic_entry.sym_ul:
                self.__log.debug(" ============ changing symul ==========")
                # update old entry (remove if empty)
                cache_entry.remove_by_sym(old.sym)
                if len(cache_entry) == 0:
                    cache.delete(old.sym_ul)
                else:
                    cache.add(old.sym_ul, cache_entry)
                # update new entry (create if not existing)
                cache_entry = cache.get(ulic_entry.sym_ul)
                if not cache_entry:
                    cache_entry = UlicMultiEntry(ulic_entry)
                else:
                    cache_entry.add_entry(ulic_entry)
            cache_entry.update_from_entries()
            self.__log.debug(cache_entry)
            cache.add(cache_entry.sym_ul, cache_entry)
        else:
            # TODO: issue warning
            # raise ValueError("Modification of non-existing record: {}".format(str(old)))
            pass

    def _handle_u(self, cache: Cache[UlicMultiEntry], obj: Element):
        """
            U - usunięcie istniejącej ulicy
                - przed zmianą powinno być w całości wypełnione
                - po zmianie - puste
        :param cache:
        :param obj:
        :return:
        """
        self.__log.debug("UlicCache.handle_u: Processing element: %s", tostring(obj, 'utf-8').decode('utf-8'))
        old = UlicEntry.from_update(update_record_to_dict(obj, 'Przed'))
        cache_entry = cache.get(old.sym_ul)
        cache_entry.remove_by_sym(old.sym)
        if len(cache_entry) == 0:
            cache.delete(old.sym_ul)
        else:
            cache.add(cache_entry.sym_ul, cache_entry)

    def _handle_z(self, cache: Cache[UlicMultiEntry], obj: Element):
        self.__log.debug("UlicCache.handle_z: Processing element: %s", tostring(obj, 'utf-8').decode('utf-8'))
        old = UlicEntry.from_update(update_record_to_dict(obj, 'Przed'))
        cache_entry = cache.get(old.sym_ul)
        new_dict = update_record_to_dict(obj, 'Po')
        for ulic_entry in cache_entry.get_all():
            ulic_entry.update_from(new_dict)

        cache_entry.sym_ul = ulic_entry.sym_ul
        cache_entry.cecha = ulic_entry.cecha
        cache_entry.nazwa = ulic_entry.nazwa
        cache.add(cache_entry.sym_ul, cache_entry)

    change_handlers = {
        'D': _handle_d,
        'M': _handle_m,
        'U': _handle_u,
        'Z': _handle_z,
    }

    def _real_version_call(self) -> datetime.date:
        return _get_teryt_client().service.PobierzDateAktualnegoKatUlic()

    def _get_binary_real_call(self, version: datetime.date):  # TODO: return type
        return _get_teryt_client().service.PobierzKatalogULIC(version)

    def _get_update_real_call(self, start: datetime.date, end: datetime.date):  # TODO: return type
        return _get_teryt_client().service.PobierzZmianyUlicUrzedowy(start, end)

    def _data_to_cache_contents(self, data: bytes):
        grouped = groupby(_get_dict(data, UlicEntry), lambda x: x.sym_ul)
        return dict((key, UlicMultiEntry.from_list(value)) for key, value in grouped.items())


@functools.lru_cache(maxsize=1)
def ulic() -> Cache[UlicMultiEntry]:
    return UlicCache().get()


def init():
    __wmrodz_create()
    TerytCache().create_cache()
    SimcCache().create_cache()
    UlicCache().create_cache()


def update():
    TerytCache().get()
    SimcCache().get()
    UlicCache().get()


def verify():
    TerytCache().verify()
    SimcCache().verify()
    UlicCache().verify()
