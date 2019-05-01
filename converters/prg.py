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

from converters.tools import Version, VersionedCache, T, Serializer, synchronized

__WGS84 = pyproj.Proj(proj='latlong', datum='WGS84')
__EPSG2180 = pyproj.Proj(init="epsg:2180")

if hasattr(pyproj, "Transformer"):
    def get_transformer(from_srs: typing.Dict[str, str], to_srs: str) -> typing.Callable[[typing.Any], typing.Tuple[float, float]]:
        transformer = pyproj.Transformer.from_proj(from_srs, to_srs).transform

        def wrapper(*args):
            return list(reversed(transformer(*args)))

        return wrapper
else:
    def get_transformer(from_srs: typing.Dict[str, str], to_srs:str) -> typing.Callable[[typing.Any], typing.Tuple[float, float]]:
        return functools.partial(pyproj.transform, pyproj.Proj(**from_srs), pyproj.Proj(init=to_srs))


_GMINY_CACHE_NAME = 'osm_prg_gminy_v1'
_POWIATY_CACHE_NAME = 'osm_prg_powiaty_v1'
_WOJEWODZTWA_CACHE_NAME = 'osm_prg_wojewodztwa_v1'


class GeoSerializer(Serializer):
    def deserialize(self, data: bytes) -> dict:
        return geobuf.decode(data)

    def serialize(self, dct: dict) -> bytes:
        return geobuf.encode(dct)


class BasePrgCache(VersionedCache[T]):
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
        return Version(get_prg_filename()[1])

    def update_cache(self, from_version: Version, target_version: Version):
        return self.create_cache(target_version)


class GminyCache(BasePrgCache[typing.Dict]):
    def __init__(self):
        super(GminyCache, self).__init__(_GMINY_CACHE_NAME)

    def _get_cache_data(self, version: Version) -> typing.Dict[str, dict]:
        return get_layer('gminy', 'JPT_KOD_JE')


class PowiatyCache(BasePrgCache[typing.Dict]):
    def __init__(self):
        super(PowiatyCache, self).__init__(_POWIATY_CACHE_NAME)

    def _get_cache_data(self, version: Version) -> typing.Dict[str, dict]:
        return get_layer('powiaty', 'JPT_KOD_JE')


class WojewodztwaCache(BasePrgCache[typing.Dict]):
    def __init__(self):
        super(WojewodztwaCache, self).__init__(_WOJEWODZTWA_CACHE_NAME)

    def _get_cache_data(self, version: Version) -> typing.Dict[str, dict]:
        return get_layer('województwa', 'JPT_KOD_JE')


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


@synchronized
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

    with fiona.Env():
        dir_name = "/" + dir_names.pop() + "/" if len(dir_names) > 0 else "/"
        __log.info("Converting PRG data")
        # with fiona.open(dir_name, vfs="zip://" + filepath, layer=layer_name, mode="r", encoding='utf-8') as data:
        with fiona.open("zip://" + filepath, layer=layer_name, mode="r", encoding='utf-8') as data:
            transform = get_transformer(data.crs, "epsg:4326")
            rv = dict(
                (x['properties'][key], project(transform, x)) for x in tqdm.tqdm(data, desc="Converting PRG data")
            )
            return rv


def get_layer(layer_name: str, key: str) -> typing.Dict[str, dict]:
    local_file = download_prg_file()
    return process_layer(layer_name, key, local_file)
