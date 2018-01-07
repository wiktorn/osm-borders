import base64
import calendar
import io
import logging
import time
import typing
import zipfile
from xml.etree import ElementTree as ET

import functools

import datetime
from xml.etree.ElementTree import Element

import zeep
from google.protobuf.message import Message

from .teryt_pb2 import \
    TercEntry as TercEntry_pb, \
    SimcEntry as SimcEntry_pb, \
    UlicMultiEntry as UlicMultiEntry_pb

from zeep.wsse.username import UsernameToken

from .tools import groupby, get_cache_manager, CacheExpired, CacheNotInitialized, ProtoSerializer, Cache

TERYT_SIMC_DB = 'osm_teryt_simc_v1'

TERYT_WMRODZ_DB = 'osm_teryt_wmrodz_v1'

TERYT_TERYT_DB = 'osm_teryt_teryt_v1'

TERYT_ULIC_DB = 'osm_teryt_ulic_v1'

__log = logging.getLogger(__name__)

Version = typing.NewType('Version', int)

T = typing.TypeVar('T')


def _zip_read(binary: bytes) -> bytes:
    dictionary_zip = zipfile.ZipFile(io.BytesIO(binary))
    dicname = [x for x in dictionary_zip.namelist() if x.endswith(".xml")][0]
    return dictionary_zip.read(dicname)


def _row_as_dict(elem: ET.Element) -> typing.Dict[str, str]:
    return dict(
        (x.tag.lower(), x.text.strip()) for x in elem.iter() if x.text
    )


def _get_teryt_client() -> zeep.Client:
    __log.info("Connecting to TERYT web service")
    wsdl = 'https://uslugaterytws1.stat.gov.pl/wsdl/terytws1.wsdl'
    wsse = UsernameToken('osmaddrtools', '#06JWOWutt4')
    return zeep.Client(wsdl=wsdl, wsse=wsse)


def _get_dict(data: bytes, cls: typing.Type[T]) -> typing.Iterable[T]:
    tree = ET.fromstring(data)
    return (cls(_row_as_dict(x)) for x in tree.find('catalog').iter('row'))


def update_record_to_dict(obj: Element, suffix: str) -> typing.Dict[str, str]:
    return dict(
        (x.tag.lower()[:-len(suffix)], x.text) for x in obj.iter() if x.tag.endswith(suffix)
    )


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
    def __init__(self, cls: typing.Type[T], pb_cls):
        super(ToFromJsonSerializer, self).__init__(pb_cls)
        self.cls = cls

    def deserialize(self, data: bytes) -> T:
        return self.cls.from_dict(super(ToFromJsonSerializer, self).deserialize(data))

    def serialize(self, dct: T) -> bytes:
        return super(ToFromJsonSerializer, self).serialize(self.cls.to_dict(dct))


class TercEntry(object):
    nazwa = None

    def __init__(self, dct: typing.Dict[str, str]):
        self.woj = dct.get('woj')
        self.powiat = dct.get('pow')
        self.gmi = dct.get('gmi')
        self.rodz = dct.get('rodz')
        self.nazwadod = nvl(dct.get('nazwadod'), '')
        self.nazwa = dct.get('nazwa')

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
        return TercEntry(**{
            'woj': dct['woj'],
            'powiat': dct['powiat'] if dct['powiat'] else '',
            'gmi': dct['gmi'] if dct['gmi'] else '',
            'rodz': dct['rodz'] if dct['rodz'] else '',
            'nazwadod': dct['nazwadod'],
            'nazwa': dct['nazwa']
        })


class SimcEntry(object):
    terc = None
    nazwa = None

    def __init__(self, dct: dict = None):
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
        ret.terc = dct['terc']
        ret.rm_id = dct.get('rm', 0)
        ret.nazwa = dct['nazwa']
        ret.sym = dct['sym']
        ret.parent = dct['parent'] if 'parent' in dct else None
        return ret

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
        for attr in ('terc', 'rm_id', 'nazwa', 'sym', 'parent'):
            new_value = new.get(attr)
            if new_value:
                setattr(self, attr, new_value)

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
        self.symul = dct.get('sym_ul')
        self.cecha_orig = dct.get('cecha')
        self.nazwa_1 = nvl(dct.get('nazwa_1'), '')
        self.nazwa_2 = nvl(dct.get('nazwa_2'), '')
        self.terc = dct.get('woj') + dct.get('pow') + dct.get('gmi') + dct.get('rodz_gmi')
        # assert self.terc == dct.get('woj') + dct.get('pow') + dct.get('gmi') + \
        #    dct.get('rodz_gmi'), "City terc code: {0} != {1} (terc code from ulic".format(
        #    self.terc, dct.get('woj') + dct.get('pow') + dct.get('gmi') + dct.get('rodz_gmi'))

    def __str__(self):
        return "UlicEntry({{sym: {}, symul: {}, cecha_orig: {}, nazwa_1: {}, nazwa_2: {}, terc: {}}})".format(
            self.sym, self.symul, self.cecha_orig, self.nazwa_1, self.nazwa_2, self.terc
        )

    @property
    def cache_key(self):
        return self.symul

    def update_from(self, obj: typing.Dict[str, str]):
        dct = dict(
            (UlicEntry._update_to_init_map.get(k, k), v) for (k, v) in obj.items()
        )
        for attr in ('sym', 'sym_ul', 'cecha', 'nazwa_1', 'nazwa_2'):
            new_value = dct.get(attr)
            if new_value:
                setattr(self, attr, new_value)
        if any(dct.get(x) for x in ('woj', 'pow', 'gmi', 'rodz_gmi')):
            self.terc = dct.get('woj') + dct.get('pow') + dct.get('gmi') + dct.get('rodz_gmi')

    def to_dict(self) -> typing.Dict[str, str]:
        return {
            'sym': int(self.sym),
            'symul': int(self.symul),
            'cecha': self.cecha_orig,
            'nazwa_1': self.nazwa_1,
            'nazwa_2': self.nazwa_2,
            'terc': int(self.terc)
        }

    @staticmethod
    def from_dict(dct: dict) -> 'UlicEntry':
        terc = dct['terc']
        return UlicEntry({
            'sym': dct['sym'],
            'sym_ul': dct['symul'],
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
        ret = UlicEntry(
            dict(
                (UlicEntry._update_to_init_map.get(k, k), v) for (k, v) in dct.items()
            )
        )
        print("From dictionary: {} created {}".format(dct, str(ret)))
        return ret

    @property
    def solr_json(self) -> tuple:
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
    __log = logging.getLogger(__name__)

    def __init__(self, entry: UlicEntry):
        self.symul = entry.symul
        self.cecha = entry.cecha
        self.nazwa = entry.nazwa
        self.entries = {entry.sym: entry}

    def __getitem__(self, item: str):
        return self.entries[item]

    @property
    def cache_key(self):
        return self.symul

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

    def get_by_sym(self, sym: str) -> UlicEntry:
        return self.entries[sym]

    def remove_by_sym(self, sym: str):
        del self.entries[sym]

    def __len__(self) -> int:
        return len(self.entries)

    def get_all(self) -> typing.Iterable[UlicEntry]:
        return self.entries.values()

    def to_dict(self) -> typing.Dict[str, str]:
        if not all(x.symul == self.symul for x in self.entries.values()):
            raise ValueError("Inconsistent object")
        return {
            'symul': int(self.symul),
            'cecha': self.cecha,
            'nazwa': self.nazwa,
            'entries': [x.to_dict() for x in self.entries.values()]
        }

    @staticmethod
    def from_dict(dct: dict) -> 'UlicMultiEntry':
        ret = UlicMultiEntry.from_list([UlicEntry.from_dict(x) for x in dct['entries']])
        assert int(ret.symul) == dct['symul']
        assert ret.cecha == dct['cecha']
        assert ret.nazwa == dct['nazwa']
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


def _wmrodz_binary(version: datetime.date) -> bytes:
    client = _get_teryt_client()
    __log.info("Downloading WMRODZ dictionary")
    dane = client.service.PobierzKatalogWMRODZ(version)
    __log.info("Downloading WMRODZ dictionary - done")
    return _zip_read(base64.decodebytes(dane.plik_zawartosc.encode('utf-8')))


def __wmrodz_create():
    version = TerytCache().cache_version()
    data = _wmrodz_binary(_int_to_datetime(version))
    cache = get_cache_manager().create_cache(TERYT_WMRODZ_DB)
    cache.reload(dict((x.rm, x.nazwa_rm) for x in _get_dict(data, BasicEntry)))
    get_cache_manager().mark_ready(TERYT_WMRODZ_DB, version)
    __log.info("WMRODZ dictionary created")


def wmrodz() -> Cache[str]:
    return get_cache_manager().get_cache(TERYT_WMRODZ_DB)


class BaseTerytCache(typing.Generic[T]):
    __log = logging.getLogger(__name__)
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

    def get(self) -> Cache[T]:
        try:
            return self._get_cache(self.cache_version())
        except CacheExpired:
            cache_version = get_cache_manager().version(self.path)
            current_version = self.cache_version()
            self._cache_update(_int_to_datetime(cache_version), _int_to_datetime(current_version))
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

    def create_cache(self):
        version = self.cache_version()
        data = self._get_binary(version)
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

    def _get_binary_real_call(self, version: datetime.date): # TODO: return type
        raise NotImplementedError

    def _get_update_real_call(self, start: datetime.date, end: datetime.date): # TODO: return type
        raise NotImplementedError


class SimcCache(BaseTerytCache):

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
        new = SimcEntry.from_dict(update_record_to_dict(obj, 'Po'))
        cache.add(new.sym, new)

    def _handle_u(self, cache: Cache[SimcEntry], obj: Element):
        """
        U - usunięcie istniejącej miejscowości
            - wypełnione wszystkie pola "przed modyfikacją"
        """
        old = SimcEntry.from_dict(update_record_to_dict(obj, 'Przed'))
        cache.delete(old.sym)

    def _handle_z(self, cache: Cache[SimcEntry], obj: Element):
        """
            Z - zmiana atrybutów dla istniejącej miejscowości
                - wypełnione tylko te pola "po modyfikacji", które się zmieniły
                - wypełnione wszystkie pola "przed modyfikacją"
        :param obj:
        :return:
        """
        old = SimcEntry.from_dict(update_record_to_dict(obj, 'Przed'))

        cache_entry = cache.get(old.sym)
        if cache_entry:
            cache_entry.update_from(update_record_to_dict(obj, 'Po'))
            if old.sym != cache_entry.sym:
                cache.delete(old.sym)
            cache.add(cache_entry.sym, cache_entry)
        else:
            # TODO: issue warning
            raise ValueError("Modification of non-existing record")

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

    def _get_binary_real_call(self, version: datetime.date): # TODO: return type
        return _get_teryt_client().service.PobierzKatalogSIMC(version)

    def _get_update_real_call(self, start: datetime.date, end: datetime.date): # TODO: return type
        return _get_teryt_client().service.PobierzZmianySimcUrzedowy(start, end)

@functools.lru_cache(maxsize=1)
def simc() -> Cache[SimcEntry]:
    return SimcCache().get()


class TerytCache(BaseTerytCache):
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
        new = TercEntry.from_dict(update_record_to_dict(obj, 'Po'))
        cache.add(new.terc, new)

    def _handle_u(self, cache: Cache[TercEntry], obj: Element):
        """
        U - usunięcie istniejącej jednostki i dołączenie do innej
            - wypełnione wszystkie pola "przed modyfikacją"
        """
        old = TercEntry.from_dict(update_record_to_dict(obj, 'Przed'))
        cache.delete(old.terc)

    def _handle_m(self, cache: Cache[TercEntry], obj: Element):
        """
            M - zmiana nazwy lub/i identyfikatora
                - wypełnione tylko te pola "po modyfikacji", które się zmieniły
                - wypełnione wszystkie pola "przed modyfikacją"
        :param obj:
        :return:
        """
        old = TercEntry.from_dict(update_record_to_dict(obj, 'Przed'))

        cache_entry = cache.get(old.terc)
        if cache_entry:
            cache_entry.update_from(update_record_to_dict(obj, 'Po'))
            if old.terc != cache_entry.terc:
                cache.delete(old.terc)
            cache.add(cache_entry.terc, cache_entry)
        else:
            # TODO: issue warning
            raise ValueError("Modification of non-existing record")

    change_handlers = {
        'D': _handle_d,
        'U': _handle_u,
        'M': _handle_m,
    }

    def _real_version_call(self) -> datetime.date:
        return _get_teryt_client().service.PobierzDateAktualnegoKatTerc()

    def _get_binary_real_call(self, version: datetime.date): # TODO: return type
        return _get_teryt_client().service.PobierzKatalogTERC(version)

    def _get_update_real_call(self, start: datetime.date, end: datetime.date): # TODO: return type
        return _get_teryt_client().service.PobierzZmianyTercUrzedowy(start, end)


@functools.lru_cache(maxsize=1)
def teryt() -> Cache[TercEntry]:
    return TerytCache().get()


class UlicCache(BaseTerytCache):
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
        to = UlicEntry.from_update(update_record_to_dict(obj, 'Po'))
        cache_entry = cache.get(to.symul)
        if cache_entry:
            cache_entry.add_entry(to)
            cache.add(to.symul, cache_entry)
        else:
            cache.add(to.symul, UlicMultiEntry(to))

    def _handle_m(self, cache: Cache[UlicMultiEntry], obj: Element):
        """
            M - zmiana parametrów ulicy
                - przed zmianą powinno być w całości wypełnione
                - po zmianie - tylko to, co się zmieniło
        :param cache:
        :param obj:
        :return:
        """
        old = UlicEntry.from_update(update_record_to_dict(obj, 'Przed'))
        if old.symul:
            cache_entry = cache.get(old.symul)
        else:
            cache_entry = None
        if cache_entry:
            ulic_entry = cache_entry.get_by_sym(old.sym)
            ulic_entry.update_from(update_record_to_dict(obj, 'Po'))
            if old.symul != ulic_entry.symul:
                # update old entry (remove if empty)
                cache_entry.remove_by_sym(old.sym)
                if len(cache_entry) == 0:
                    cache.delete(old.symul)
                else:
                    cache.add(old.symul, cache_entry)
                # update new entry (create if not existing)
                cache_entry = cache.get(ulic_entry.symul)
                if not cache_entry:
                    cache_entry = UlicMultiEntry(ulic_entry)
                else:
                    cache_entry.add_entry(ulic_entry)
            cache.add(cache_entry.symul, cache_entry)
        else:
            # TODO: issue warning
            raise ValueError("Modification of non-existing record: %s", str(old))

    def _handle_u(self, cache: Cache[UlicMultiEntry], obj: Element):
        """
            U - usunięcie istniejącej ulicy
                - przed zmianą powinno być w całości wypełnione
                - po zmianie - puste
        :param cache:
        :param obj:
        :return:
        """
        old = UlicEntry.from_update(update_record_to_dict(obj, 'Przed'))
        cache_entry = cache.get(old.symul)
        cache_entry.remove_by_sym(old.sym)
        if len(cache_entry) == 0:
            cache.delete(old.symul)
        else:
            cache.add(cache_entry.symul, cache_entry)

    def _handle_z(self, cache: Cache[UlicMultiEntry], obj: Element):
        old = UlicEntry.from_update(update_record_to_dict(obj, 'Przed'))
        cache_entry = cache.get(old.symul)
        new_dict = update_record_to_dict(obj, 'Po')
        for ulic_entry in cache_entry.get_all():
            ulic_entry.update_from(new_dict)

        cache_entry.symul = ulic_entry.symul
        cache_entry.cecha = ulic_entry.cecha
        cache_entry.nazwa = ulic_entry.nazwa
        cache.add(cache_entry.symul, cache_entry)

    change_handlers = {
        'D': _handle_d,
        'M': _handle_m,
        'U': _handle_u,
        'Z': _handle_z,
    }

    def _real_version_call(self) -> datetime.date:
        return _get_teryt_client().service.PobierzDateAktualnegoKatUlic()

    def _get_binary_real_call(self, version: datetime.date): # TODO: return type
        return _get_teryt_client().service.PobierzKatalogULIC(version)

    def _get_update_real_call(self, start: datetime.date, end: datetime.date): # TODO: return type
        return _get_teryt_client().service.PobierzZmianyUlicUrzedowy(start, end)

    def _data_to_cache_contents(self, data: bytes):
        grouped = groupby(_get_dict(data, UlicEntry), lambda x: x.symul)
        return dict((key, UlicMultiEntry.from_list(value)) for key, value in grouped.items())


@functools.lru_cache(maxsize=1)
def ulic() -> Cache[UlicMultiEntry]:
    return UlicCache().get()


def init():
    __wmrodz_create()
    TerytCache().create_cache()
    SimcCache().create_cache()
    UlicCache().create_cache()
