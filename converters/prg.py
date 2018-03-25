import atexit
import calendar
import functools
import logging
import os
import shutil
import tempfile
import typing
import urllib.request
import zipfile

import bs4
import fiona
import geobuf
import pyproj
import requests
import time
import tqdm

from converters.tools import Version, VersionedCache, T, Serializer

_GMINY_CACHE_NAME = 'osm_prg_gminy_v1'
_POWIATY_CACHE_NAME = 'osm_prg_powiaty_v1'
_WOJEWODZTWA_CACHE_NAME = 'osm_prg_wojewodztwa_v1'


class GeoSerializer(Serializer):
    def deserialize(self, data: bytes) -> dict:
        return geobuf.decode(data)

    def serialize(self, dct: dict) -> bytes:
        return geobuf.encode(dct)


class BasePrgCache(VersionedCache):
    """
    Class for caching PRG borders. All borders are stored as GeoJSON dicts
    """
    __log = logging.getLogger(__name__ + '.BasePrgCache')
    change_handlers = dict()

    def _get_cache_data(self, version: Version) -> typing.Dict[str, T]:
        raise NotImplementedError

    def _get_serializer(self):
        return GeoSerializer()

    def current_cache_version(self) -> Version:
        return get_prg_filename()[1]

    def update_cache(self, from_version: Version, target_version: Version):
        return self.create_cache(target_version)


class GminyCache(BasePrgCache):
    def __init__(self):
        super(GminyCache, self).__init__(_GMINY_CACHE_NAME, dict)

    def _get_cache_data(self, version: Version) -> typing.Dict[str, dict]:
        return get_layer('gminy', 'jpt_kod_je')


class PowiatyCache(BasePrgCache):
    def __init__(self):
        super(PowiatyCache, self).__init__(_POWIATY_CACHE_NAME, dict)

    def _get_cache_data(self, version: Version) -> typing.Dict[str, dict]:
        return get_layer('powiaty', 'jpt_kod_je')


class WojewodztwaCache(BasePrgCache):
    def __init__(self):
        super(WojewodztwaCache, self).__init__(_WOJEWODZTWA_CACHE_NAME, dict)

    def _get_cache_data(self, version: Version) -> typing.Dict[str, dict]:
        return get_layer('województwa', 'jpt_kod_je')


__log = logging.getLogger(__name__)


def init():
    GminyCache().create_cache()
    PowiatyCache().create_cache()
    WojewodztwaCache().create_cache()


class TqdmUpTo(tqdm.tqdm):
    def update_to(self, b=1, bsize=1, tsize=None):
        if tsize is not None:
            self.total = tsize
        self.update(b*bsize - self.n)


@functools.lru_cache(maxsize=1)
def get_prg_filename() -> typing.Tuple[str, int]:
    resp = requests.get("http://www.gugik.gov.pl/geodezja-i-kartografia/pzgik/dane-bez-oplat/"
                        "dane-z-panstwowego-rejestru-granic-i-powierzchni-jednostek-podzialow-terytorialnych-kraju-prg")
    soup = bs4.BeautifulSoup(resp.text, "html.parser")
    link = soup.find("a", text="*PRG – jednostki administracyjne")
    version = link.parent.parent.parent.parent.find_all('td')[-1].text
    return link.get('href'), calendar.timegm(time.strptime(version, '%d-%m-%Y'))


@functools.lru_cache(maxsize=1)
def download_prg_file() -> str:
    path = tempfile.mkdtemp(prefix="prg")
    file_name = os.path.join(path, 'prg_file.zip')
    __log.info("Downloading PRG archive")
    url, version = get_prg_filename()
    with TqdmUpTo(unit='B', unit_scale=True, miniters=1, desc=url) as t:
        urllib.request.urlretrieve(url, filename=file_name, reporthook=t.update_to)
    __log.info("Downloading PRG archive - done")
    atexit.register(shutil.rmtree, path)
    return file_name


def project(transform, geojson: dict) -> dict:
    typ = geojson['geometry']['type']
    if typ == 'Polygon':
        geojson['geometry']['coordinates'] = [
            [transform(*y) for y in x] for x in geojson['geometry']['coordinates']
            ]
        return geojson
    if typ == 'MultiPolygon':
        geojson['geometry']['coordinates'] = [
            [[transform(*z) for z in y] for y in x] for x in geojson['geometry']['coordinates']
            ]
        return geojson

    else:
        raise ValueError("Unsupported geometry type: {0}".format(typ))


def process_layer(layer_name: str, key: str, filepath: str) -> typing.Dict[str, dict]:
    with zipfile.ZipFile(filepath, 'r') as zfile:
        dir_names = set([os.path.dirname(x.filename) for x in zfile.infolist() if x.filename.endswith('.shp')])
    if len(dir_names) != 1:
        raise ValueError("Can't guess the directory inside zipfile. Candidates: {0}".format(", ".join(dir_names)))

    with fiona.drivers():
        dir_name = "/" + dir_names.pop()
        __log.info("Converting PRG data")
        with fiona.open(path=dir_name, vfs="zip://" + filepath, layer=layer_name, mode="r", encoding='cp1250') as data:
            transform = functools.partial(pyproj.transform, pyproj.Proj(data.crs), pyproj.Proj(init="epsg:4326"))
            __log.info("Converting PRG data")
            rv = dict(
                (x['properties'][key], project(transform, x)) for x in tqdm.tqdm(data)
            )
            __log.info("Converting PRG data - done")
            return rv


def get_layer(layer_name: str, key: str) -> typing.Dict[str, dict]:
    local_file = download_prg_file()
    return process_layer(layer_name, key, local_file)
