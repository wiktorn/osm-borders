import functools
import logging
import os
import tempfile
import typing
import urllib.request
import zipfile

import bs4
import fiona
import pyproj
import requests

from .tools import CachedDictionary

__log = logging.getLogger(__name__)


def get_prg_filename():
    resp = requests.get("http://www.codgik.gov.pl/index.php/darmowe-dane/prg.html")
    soup = bs4.BeautifulSoup(resp.text, "html.parser")
    link = soup.find("a", text="PRG – jednostki administracyjne")
    return link.get('href')


@functools.lru_cache(maxsize=1)
def download_prg_file() -> str:
    dir = tempfile.mkdtemp(prefix="prg")
    fname = os.path.join(dir, 'prg_file.zip')
    __log.info("Downloading PRG archive")
    urllib.request.urlretrieve(get_prg_filename(), fname)
    __log.info("Downloading PRG archive - done")
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
        #layers = fiona.listlayers(dirname, "zip://" + filepath, encoding='cp1250')
        #logging.debug("Found layers: {0}".format(", ".join(layers)))
        __log.info("Converting PRG data")
        with fiona.open(path=dirname, vfs="zip://" + filepath, layer=layer_name, mode="r", encoding='cp1250') as data:
            transform = functools.partial(pyproj.transform, pyproj.Proj(data.crs), pyproj.Proj(init="epsg:4326"))
            rv = dict((x['properties'][key], project(transform, x)) for x in data)
            __log.info("Converting PRG data - done")
            return rv


def get_layer(layer_name: str, key: str) -> typing.Dict[str, dict]:
    localfile = download_prg_file()
    return process_layer(layer_name, key, localfile)


gminy = CachedDictionary("osm_prg_gminy_v1", functools.partial(get_layer, 'gminy', 'jpt_kod_je'))
# schema file for powiaty is broken, doesn't parse right now
# powiaty = CachedDictionary("osm-prg-gminy-v1", functools.partial(get_layer, 'powiaty', 'jpt_kod_je'))
wojewodztwa = CachedDictionary("osm_prg_wojewodztwa_v1", functools.partial(get_layer, 'województwa', 'jpt_kod_je'))
