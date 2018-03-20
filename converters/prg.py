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

from .tools import get_cache_manager, Serializer

__GMINY_CACHE_NAME = 'osm_prg_gminy_v1'
__WOJEWODZTWA_CACHE_NAME = 'osm_prg_wojewodztwa_v1'


class GeoSerializer(Serializer):
    def deserialize(self, data: bytes) -> dict:
        return geobuf.decode(data)

    def serialize(self, dct: dict) -> bytes:
        return geobuf.encode(dct)


def gminy():
    return get_cache_manager().get_cache(__GMINY_CACHE_NAME,
                                         version=get_prg_filename()[1],
                                         serializer=GeoSerializer())


def wojewodztwa():
    return get_cache_manager().get_cache(__WOJEWODZTWA_CACHE_NAME,
                                         version=get_prg_filename()[1],
                                         serializer=GeoSerializer())


def init():
    cm = get_cache_manager()

    foo, version = get_prg_filename()
    gminy = cm.create_cache(__GMINY_CACHE_NAME, serializer=GeoSerializer())
    gminy.reload(get_layer('gminy', 'jpt_kod_je'))
    cm.mark_ready(__GMINY_CACHE_NAME, version)

    # wojewodztwa = cm.create_cache(__WOJEWODZTWA_CACHE_NAME)
    # wojewodztwa.reload(get_layer('województwa', 'jpt_kod_je'))
    # cm.mark_ready(__WOJEWODZTWA_CACHE_NAME)


__log = logging.getLogger(__name__)


@functools.lru_cache(maxsize=1)
def get_prg_filename() -> typing.Tuple[str, int]:
    #resp = requests.get("http://www.codgik.gov.pl/index.php/darmowe-dane/prg.html")
    resp = requests.get("http://www.gugik.gov.pl/geodezja-i-kartografia/pzgik/dane-bez-oplat/dane-z-panstwowego-rejestru-granic-i-powierzchni-jednostek-podzialow-terytorialnych-kraju-prg")
    soup = bs4.BeautifulSoup(resp.text, "html.parser")
    link = soup.find("a", text="*PRG – jednostki administracyjne")
    version = link.parent.parent.parent.parent.find_all('td')[-1].text
    return link.get('href'), calendar.timegm(time.strptime(version, '%d-%m-%Y'))


class TqdmUpTo(tqdm.tqdm):
    def update_to(self, b=1, bsize=1, tsize=None):
        if tsize is not None:
            self.total = tsize
        self.update(b*bsize - self.n)

@functools.lru_cache(maxsize=1)
def download_prg_file() -> str:
    #return "/tmp/prgxbdnufnb/prg_file.zip"
    dir = tempfile.mkdtemp(prefix="prg")
    fname = os.path.join(dir, 'prg_file.zip')
    __log.info("Downloading PRG archive")
    url, version = get_prg_filename()
    with TqdmUpTo(unit='B', unit_scale=True, miniters=1, desc=url) as t:
        urllib.request.urlretrieve(url, filename=fname, reporthook=t.update_to)
    __log.info("Downloading PRG archive - done")
    atexit.register(shutil.rmtree, dir)
    return fname


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
        dirnames = set([os.path.dirname(x.filename) for x in zfile.infolist() if x.filename.endswith('.shp')])
    if len(dirnames) != 1:
        raise ValueError("Can't guess the directory inside zipfile. Candidates: {0}".format(", ".join(dirnames)))

    with fiona.drivers():
        dirname = "/" + dirnames.pop()
        __log.info("Converting PRG data")
        with fiona.open(path=dirname, vfs="zip://" + filepath, layer=layer_name, mode="r", encoding='cp1250') as data:
            transform = functools.partial(pyproj.transform, pyproj.Proj(data.crs), pyproj.Proj(init="epsg:4326"))
            rv = dict(
                (x['properties'][key], project(transform, x)) for x in tqdm.tqdm(data)
            )
            __log.info("Converting PRG data - done")
            return rv


def get_layer(layer_name: str, key: str) -> typing.Dict[str, dict]:
    localfile = download_prg_file()
    return process_layer(layer_name, key, localfile)

