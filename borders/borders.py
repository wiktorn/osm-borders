import itertools
import json
import logging
import math
import time
import typing
import xml.etree.ElementTree as ET

import cachetools.func
import requests
import shapely.geometry
import shapely.ops

from borders.geoutils import split_by_common_ways
from borders.wikidata import fetch_from_wikidata, WikidataSimcEntry
from converters.feature import ImmutableFeature, Feature
from converters.kmlshapely import kml_to_shapely
from converters.prg import gminy
from converters.teryt import simc as SIMC_DICT

__log = logging.getLogger(__name__)

@cachetools.func.ttl_cache(maxsize=128, ttl=600)
def get_adm_border(terc: str) -> shapely.geometry.base.BaseGeometry:
    GMINY_DICT = gminy()
    geojson = GMINY_DICT[terc]
    if geojson:
        return shapely.geometry.shape(geojson['geometry'])
    else:
        candidates = [x for x in GMINY_DICT.keys() if x.startswith(terc[:-1])]
        raise KeyError("Gmina o kodzie {0} nieznaleziona w PRG. Może jedna z: {1}".format(terc, ", ".join(candidates)))


TYPE_BBOX = typing.Tuple[float, float, float, float]


def geometry_as_geojson(o: shapely.geometry.base.BaseGeometry) -> str:
    return json.dumps(shapely.geometry.mapping(o))


def divide_bbox(bbox: TYPE_BBOX) -> typing.List[TYPE_BBOX]:
    (minx, miny, maxx, maxy) = bbox
    # EPSG:2180
    # __MAX_BBOX_X = 20000
    # __MAX_BBOX_Y = 45000
    # __PRECISION = 10
    __PRECISION = 1000000
    __MAX_BBOX_X = int(0.03 * __PRECISION)
    __MAX_BBOX_Y = int(0.04 * __PRECISION)
    rv = [
        (x / __PRECISION,
         y / __PRECISION,
         min(x / __PRECISION + __MAX_BBOX_X, maxx),
         min(y / __PRECISION + __MAX_BBOX_Y, maxy))
        for x in range(math.floor(minx * __PRECISION), math.ceil(maxx * __PRECISION), __MAX_BBOX_X * __PRECISION)
        for y in range(math.floor(miny * __PRECISION), math.ceil(maxy * __PRECISION), __MAX_BBOX_Y * __PRECISION)
        ]
    __log.info("Split bbox to {0} parts".format(len(rv)))
    return rv


@cachetools.func.ttl_cache(maxsize=128, ttl=24 * 3600)
def fetch_from_emuia_cached(bbox: TYPE_BBOX) -> str:
    try:
        __log.info("Downloading BBOX: {0} from EMUiA".format(bbox))
        resp = requests.get("http://emuia1.gugik.gov.pl/wmsproxy/emuia/wms",
                        params={
                            "FORMAT": "application/vnd.google-earth.kml+xml",
                            "VERSION": "1.1.1",
                            "SERVICE": "WMS",
                            "REQUEST": "GetMap",
                            # "LAYERS": "emuia:layer_adresy_labels",
                            "LAYERS": "emuia:layer_miejscowosci_granica",
                            "STYLES": "",
                            # "SRS": "EPSG:2180",
                            "SRS": "EPSG:4326",
                            "WIDTH": "16000",
                            "HEIGHT": "16000",
                            "BBOX": "{0},{1},{2},{3}".format(*bbox)
                        },
                        verify=False)
    except requests.exceptions.ConnectionError as e:
        raise requests.exceptions.ConnectionError(e.errno if e.errno else -1, "Problem connecting to EMUiA", e)
    try:
        ET.XML(resp.text)
    except Exception as e:
        if len(resp.text) < 1024:
            raise ValueError("Unexpected response from EMUiA. Not an XML: " + resp.text)
        else:
            raise ValueError("Unexpected response from EMUiA. Not an XML", e)
    return resp.text


def fetch_from_emuia(bbox: TYPE_BBOX) -> typing.List[Feature]:
    return kml_to_shapely(fetch_from_emuia_cached(bbox))


def get_borders(terc: str,
                filter:  typing.Callable[[Feature, ], bool] = lambda x: True,
                borders_mapping: typing.Callable[[typing.List[Feature], ], typing.List[Feature]] = split_by_common_ways,
                do_clean_borders: bool = True) -> bytes:
    adm_bound = get_adm_border(terc)
    borders = []
    __log.info("Downloading data from EMUiA")
    for bbox in divide_bbox(adm_bound.bounds):  # area we need to fetch from EMUiA
        borders.extend(fetch_from_emuia(bbox))
    wikidata = []
    __log.info("Downloading data from Wikidata")
    try:
        wikidata = fetch_from_wikidata(terc)
    except Exception as e:
        # ignore any exceptions
        __log.warning("Exception during fetch from Wikidata: {0}", e, exc_info=(type(e), e, e.__traceback__))
    __log.info("Processing data")
    return process(adm_bound = adm_bound,
                   borders = borders,
                   filter = filter,
                   borders_mapping=borders_mapping,
                   wikidata=wikidata,
                   do_clean_borders=do_clean_borders)


def clean_borders(borders: typing.List[Feature], do_clean: bool = True) -> None:
    for border in borders:
        simc_code = border.tags.get('TERYT_MIEJSCOWOSCI')
        parent_id = border.tags.get('IDENTYFIKATOR_NADRZEDNEJ')
        emuia_level = 10 if parent_id else 8

        simc_entry = SIMC_DICT().get(simc_code)
        if not simc_entry:
            __log.error(
                "No entry in TERYT dictionary for SIMC: {0}, name: {1}".format(simc_code, border.tags.get('NAZWA')))
            border.tags['admin_level'] = 'TODO'
            border.tags['fixme'] = "No entry in TERYT for this teryt:simc. EMUiA admin_level={0}".format(emuia_level)
            continue
        simc_level = 10 if simc_entry.parent else 8

        fixme = []
        level = simc_level

        if emuia_level == 10 and simc_level == 10:
            # verify that they have the same parent
            try:
                parent_border = [x for x in borders if x.tags.get('IDENTYFIKATOR_MIEJSCOWOSCI') == parent_id][0]
                if simc_entry.parent != parent_border.tags.get('TERYT_MIEJSCOWOSCI'):
                    fixme.append("Different parents. In EMUiA it is teryt:simc: {0}, name: {1}".format(
                        simc_entry.parent,
                        SIMC_DICT()[simc_entry.parent].nazwa))
            except IndexError:
                fixme.append("Missing parent border: {0}".format(parent_id))

        if emuia_level == 10 and simc_level == 8:
            # raise the border level to admin_level 8
            try:
                parent_border = [x for x in borders if x.tags.get('IDENTYFIKATOR_MIEJSCOWOSCI') == parent_id][0]
                new_geo = parent_border.geometry.difference(border.geometry)
                if not new_geo.is_empty and do_clean:
                    __log.info("Changing geometry (EMUiA = 10, TERC = 8) of {0} because of {1}. "
                               "{0} border dump: {2}".format(parent_border.tags.get("NAZWA"),
                                                             border.tags.get("NAZWA"),
                                                             parent_border)
                               )
                    parent_border.geometry = new_geo
                fixme.append("EMUiA points teryt:simc {0}, name: {1} as parent. In TERC this is standalone".format(
                    parent_border.tags.get('TERYT_MIEJSCOWOSCI'),
                    parent_border.tags.get('NAZWA')
                ))
            except IndexError:
                fixme.append("Missing parent border: {0}".format(parent_id))

        if emuia_level == 8 and simc_level == 10:
            fixme.append("TERC points this as part of teryt:simc={0}, name={1}".format(
                simc_entry.parent,
                SIMC_DICT()[simc_entry.parent].nazwa
            ))
            level = emuia_level
            try:
                if do_clean:
                    parent_border = [x for x in borders if x.tags.get('TERYT_MIEJSCOWOSCI') == simc_entry.parent][0]
                    __log.info("Changing geometry (EMUiA = 8, TERC = 10) of {0} because of {1}. "
                               "{0} border dump: {2}".format(parent_border.tags.get("NAZWA"),
                                                             border.tags.get("NAZWA"),
                                                             parent_border)
                               )
                    parent_border.geometry = parent_border.geometry.union(border.geometry)
                    level = simc_level
            except IndexError:
                fixme.append('Missing parent border: {0}'.format(simc_entry.parent))

        border.tags['admin_level'] = str(level)
        if fixme:
            border.tags['fixme'] = ", ".join(fixme)


def add_wikidata(wikidata: typing.List[WikidataSimcEntry], borders: typing.List[Feature]):
    rest = list(wikidata)
    todo = list(borders)
    border_iter = itertools.cycle(todo)

    def update_border(entry, border):
        entry = candidates[0]
        rest.remove(entry)
        border.tags['wikidata'] = entry.wikidata
        border.tags['wikipedia'] = entry.wikipedia
        if not border.tags['NAZWA'] in entry.miejscowosc:
            border.tags['fixme'] = "Check Wikipedia/Wikidata tags. In Wikipedia name is: {0}".format(
                entry.miejscowosc)

    loop_limit = 100

    while todo and loop_limit > 0:
        loop_limit -= 1
        border = next(border_iter)
        candidates = [x for x in rest if x.miejscowosc == border.tags['NAZWA']]
        if len(candidates) <= 1:
            if candidates:
                update_border(candidates[0], border)
                todo.remove(border)  # we will not need to process it again
                border_iter = itertools.cycle(todo)
            else:
                # no candidates by geometry
                candidates = [x for x in rest if
                              x.point.within(border.geometry) and border.tags['NAZWA'] in x.miejscowosc]
                if len(candidates) <= 1:
                    if candidates:
                        update_border(candidates[0], border)
                        todo.remove(border)  # we will not need to process it again
                        border_iter = itertools.cycle(todo)
                    else:
                        candidates = [x for x in rest if border.tags['NAZWA'] in x.miejscowosc]
                        if len(candidates) <= 1:
                            # no or one candidate by name and no candidate by geometry
                            # remove so we will not process it again
                            todo.remove(border)  # we will not need to process it again
                            border_iter = itertools.cycle(todo)
                            if candidates:
                                update_border(candidates[0], border)



def process(adm_bound: shapely.geometry.base.BaseGeometry,
            borders: typing.List[Feature],
            filter: typing.Callable[[Feature, ], bool] = lambda x: True,
            borders_mapping: typing.Callable[[typing.List[Feature], ],
                                             typing.List[Feature]] = split_by_common_ways,
            wikidata: typing.List[WikidataSimcEntry] = None,
            do_clean_borders: bool = True) -> bytes:
    """

    :param adm_bound: shape of the area that one should work on
    :param borders: list of features to process
    :param filter: output filtering function
    :param borders_mapping: function that converts all the features
    :param wikidata: wikidata information for this municipiality
    :return:
    """
    adm_bound = adm_bound.buffer(0.005)  # ~ 500m along meridian
    if not wikidata:
        wikidata = []

    def valid_border(x):
        rv = x.geometry.intersects(adm_bound) and (
        x.tags.get('DO') is None or int(x.tags.get('DO')) > time.time() * 1000)
        if not rv:
            msg = ", ".join("{0}: {1}".format(key, x.tags[key]) for key in sorted(x.tags.keys()))
            if not x.geometry.intersects(adm_bound):
                __log.debug("Removing border as it is outside working set: {0}".format(msg))
            else:
                __log.debug("Removing outdated border: {0}".format(msg))
        return rv

    __log.debug("Names before dedup: {0}".format(len(borders)))
    borders = [im.to_feature() for im in set(ImmutableFeature(x) for x in borders if valid_border(x))]
    __log.debug("Names after dedup: {0}".format(len(borders)))

    clean_borders(borders, do_clean=do_clean_borders)
    add_wikidata(wikidata, borders)

    for border in borders:
        # orient strings (counterclockwise) and then get its borders
        # use LineStrings instead of Polygons
        if isinstance(border.geometry, shapely.geometry.polygon.Polygon):
            border.geometry = shapely.geometry.polygon.orient(border.geometry).boundary
        elif isinstance(border.geometry, shapely.geometry.multipolygon.MultiPolygon):
            geoms = [shapely.geometry.polygon.orient(x) if isinstance(x, shapely.geometry.polygon.Polygon) else x
                     for x in border.geometry.geoms]
            border.geometry = shapely.geometry.asMultiPolygon(geoms).boundary
        else:
            border.geometry = border.geometry.boundary


    def tag_mapping(obj_type: str, tags: typing.Dict[str, str]) -> typing.Generator[typing.Tuple[str, str], None, None]:
        if obj_type == "relation":
            yield ('boundary', 'administrative')
            yield ('type', 'boundary')
            yield ('source:generator', 'osm-borders.py')
            yield ('admin_level', tags['admin_level'])
            yield ('name', tags['NAZWA'])
            if 'TERYT_MIEJSCOWOSCI' in tags:
                yield ('teryt:simc', tags['TERYT_MIEJSCOWOSCI'])
            yield ('name:prefix', tags['RODZAJ'].lower())
            for key in ('wikidata', 'wikipedia', 'fixme'):
                if key in tags:
                    yield (key, tags[key])
        elif obj_type == "way":
            yield ('source:geometry', tags['ZRODLO_GEOMETRII'])
            yield ('boundary', 'administrative')
        elif obj_type == "node":
            pass
        else:
            raise ValueError("Unknown object type: {0}".format(obj_type))

    def default_filter(feature: Feature) -> bool:
        if feature.geometry.within(adm_bound):
            if filter(feature):
                return True
            else:
                __log.debug("Filter function refused border: {0}".format(feature))
        else:
            __log.debug("Border is outside working area: {0}".format(feature))
        return False

    converter = FeatureToOsm(borders = borders,
                             tag_mapping= tag_mapping,
                             filter=default_filter,
                             borders_mapping = borders_mapping)
    return converter.tostring()


class FeatureToOsm:
    __log = logging.getLogger(__name__)

    def __init__(self,
                 borders: typing.List[Feature],
                 tag_mapping: typing.Callable[[str, typing.Dict[str, str]], typing.Generator] = lambda x, y: y,
                 filter: typing.Callable[[Feature], bool] = lambda x: True,
                 borders_mapping: typing.Callable[[typing.List[Feature], ], typing.List[Feature]] = split_by_common_ways
                 ):
        self.__object_store = {
            'way': {},
            'point': {},
            'relation': {}
        }
        self.id_ = itertools.count(-1, -1)
        self.borders = borders
        self.tag_mapping = tag_mapping
        self.filter = filter
        self.borders_mapping = borders_mapping

    def tostring(self) -> bytes:
        out_xml = ET.Element("osm", {'generator': 'osm-borders', 'version': '0.6', 'upload': 'false'})
        for border in self.borders_mapping(self.borders):
            if self.filter(border):
                self.dump_relation(out_xml, border)
            else:
                self.__log.debug("Filter excluded border: {0}".format(border))
        return ET.tostring(out_xml, encoding='utf-8')

    def dump_relation(self, tree, border: Feature) -> None:
        self.__log.debug("Dumping relation: {0}".format(border))
        rel = ET.SubElement(tree, "relation", {'id': str(next(self.id_))})
        (outer, inner) = self.dump_ways(tree, border)

        # false positive
        # noinspection PyTypeChecker
        for key, value in self.tag_mapping("relation", border.tags):
            ET.SubElement(rel, "tag",
                          {'k': key,
                           'v': value})

        for way in outer:
            ET.SubElement(rel, 'member', {'ref': str(way), 'role': 'outer', 'type': 'way'})

        for way in inner:
            ET.SubElement(rel, 'member', {'ref': str(way), 'role': 'inner', 'type': 'way'})

    def dump_ways(self, tree, border: Feature) -> typing.Tuple[typing.List[int], typing.List[int]]:
        outer = []
        inner = []
        geojson = shapely.geometry.mapping(border.geometry)

        def algo(way: typing.List[typing.Tuple[float, float]]):
            cached_way = self.__object_store['way'].get(way)
            if cached_way:
                return cached_way
            nodes = self.dump_points(tree, way, border.tags)
            current_id = next(self.id_)
            self.__object_store['way'][way] = current_id
            way = ET.SubElement(tree, "way", {'id': str(current_id)})

            # false positive
            # noinspection PyTypeChecker
            for key, value in self.tag_mapping("way", border.tags):
                ET.SubElement(way, "tag", {'k': key, 'v': value})
            for node in nodes:
                ET.SubElement(way, "nd", {'ref': str(node)})
            return current_id

        if geojson['type'] == 'Polygon':
            coords = geojson['coordinates']  # use outer coordinates
            outer.append(algo(coords[0]))
            if len(coords) > 1:
                for way in coords[1:]:
                    inner.append(algo(way))
        elif geojson['type'] == 'LineString':
            coords = geojson['coordinates']
            outer.append(algo(coords))
        elif geojson['type'] == 'MultiLineString':
            outer.extend(algo(x) for x in geojson['coordinates'])

        elif geojson['type'] == 'MultiPolygon':
            for polygon in geojson['coordinates']:
                outer.append(algo(polygon[0]))
                if len(polygon) > 1:
                    for way in polygon[1:]:
                        inner.append(algo(way))
        else:
            raise ValueError("Unkown GeoJSON Type found: {0}".format(geojson['type']))
        return outer, inner

    def dump_points(self, tree, points: list, tags: dict) -> typing.List[int]:
        rv = []
        for point in points:
            cached_point = self.__object_store['point'].get(point)
            if cached_point:
                current_id = cached_point
            else:
                current_id = next(self.id_)
                self.__object_store['point'][point] = current_id
                node = ET.SubElement(tree, "node", {'id': str(current_id), 'lon': str(point[0]), 'lat': str(point[1])})
                # false positive
                # noinspection PyTypeChecker
                for key, value in self.tag_mapping("node", tags):
                    ET.SubElement(node, "tag", {'k': key, 'v': value})
            rv.append(current_id)
        return rv


def gminy_prg_as_osm(terc: str):
    GMINY_DICT = gminy()
    borders = [Feature.from_geojson(GMINY_DICT[x]) for x in GMINY_DICT.keys() if x.startswith(terc)]

    for x in borders:
        x.geometry = x.geometry.boundary

    def tag_mapping(obj_type: str, tags: typing.Dict[str, str]) -> typing.Generator[typing.Tuple[str, str], None, None]:
        if obj_type == "relation":
            for key, value in tags.items():
                if value:
                    yield ('prg:' + key, str(value))
            yield ('boundary', 'administrative')
            yield ('type', 'boundary')
            yield ('source:generator', 'osm-borders.py')
            terc_len = len(tags['jpt_kod_je'])
            if terc_len == 4:
                yield ('admin_level', str(6))
            elif terc_len == 2:
                yield ('admin_level', str(4))
            elif terc_len == 7:
                yield ('admin_level', str(7))
            else:
                yield ('admin_level', 'TODO')
            yield ('name', tags.get('jpt_nazwa_', ''))
            yield ('teryt:simc', tags.get('jpt_kod_je', ''))
            for key in ('wikidata', 'wikipedia', 'fixme'):
                if key in tags:
                    yield (key, str(tags[key]))
        elif obj_type == "way":
            yield ('source:geometry', 'PRG')
            yield ('boundary', 'administrative')
        elif obj_type == "node":
            pass
        else:
            raise ValueError("Unknown object type: {0}".format(obj_type))

    converter = FeatureToOsm(borders=borders, tag_mapping=tag_mapping)
    return converter.tostring()
