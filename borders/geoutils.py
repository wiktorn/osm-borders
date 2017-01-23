import itertools
import logging
import typing
from collections import defaultdict

import shapely.geometry
import shapely.ops

from converters.feature import Feature

__log = logging.getLogger(__name__)

def get_raw_geometries(obj: shapely.geometry.base.BaseGeometry) -> typing.List[shapely.geometry.base.BaseGeometry]:
    if not obj.is_empty and isinstance(obj, shapely.geometry.base.BaseMultipartGeometry):
        return [x for x in obj.geoms]
    if obj.is_empty:
        return []
    return [obj, ]


# noinspection PyTypeChecker
def try_linemerge(obj: shapely.geometry.base.BaseGeometry) -> shapely.geometry.base.BaseGeometry:
    if not obj.is_empty \
            and isinstance(obj, shapely.geometry.base.BaseMultipartGeometry) \
            and len(obj) > 1 \
            and not isinstance(obj, shapely.geometry.MultiPoint):
        return shapely.ops.linemerge(
            shapely.geometry.MultiLineString([x for x in obj if not isinstance(x, shapely.geometry.Point)]))
    return obj


def create_multi_string(obj1: shapely.geometry.base.BaseGeometry,
                        obj2: shapely.geometry.base.BaseGeometry) -> shapely.geometry.MultiLineString:
    geoms = []

    geoms.extend(get_raw_geometries(obj1))
    geoms.extend(get_raw_geometries(obj2))
    return shapely.geometry.MultiLineString(geoms)


def split_intersec(intersec: shapely.geometry.base.BaseGeometry,
                   objs: typing.List[shapely.geometry.base.BaseGeometry]) -> shapely.geometry.base.BaseGeometry:
    rv = intersec
    for obj in objs:
        if not isinstance(obj, shapely.geometry.base.BaseMultipartGeometry):
            continue  # nothing to be done
        for geom in obj.geoms:
            small_intersec = try_linemerge(geom.intersection(intersec))
            if geom.intersects(intersec) and not isinstance(small_intersec,
                                                            (shapely.geometry.Point, shapely.geometry.MultiPoint)):
                rest = get_raw_geometries(rv.difference(small_intersec))
                if rest:
                    rv = shapely.geometry.MultiLineString((*get_raw_geometries(small_intersec), *rest))
    return rv


def split_by_common_ways(borders: typing.List[Feature]) -> typing.List[Feature]:
    for (border, other) in itertools.combinations(borders, 2):
        __log.debug("Processing border ({0}, {1})".format(borders.index(border), borders.index(other)))
        intersec = border.geometry.intersection(other.geometry)
        intersec_buffered = border.geometry.buffer(0.0000001).intersection(other.geometry.buffer(0.0000001))
        if intersec.is_empty:
            continue  # nothing will change anyway
        if isinstance(intersec, shapely.geometry.GeometryCollection):
            return split_by_ballon(intersec, intersec_buffered)
        if isinstance(intersec, (shapely.geometry.Point, shapely.geometry.MultiPoint)):
            intersec = shapely.geometry.LineString()  # empty geometry
        intersec = try_linemerge(intersec)
        intersec = split_intersec(intersec, [border.geometry, other.geometry])
        border.geometry = create_multi_string(intersec, border.geometry.difference(intersec))
        other.geometry = create_multi_string(intersec, other.geometry.difference(intersec))
    return borders


def split_by_ballon(intersec: shapely.geometry.base.BaseMultipartGeometry, ballon: shapely.geometry.base.BaseGeometry):
    if isinstance(ballon, shapely.geometry.Polygon):
        return [x for x in intersec.geoms if not isinstance(x, shapely.geometry.Point)]
    if isinstance(ballon, (shapely.geometry.MultiPolygon, shapely.geometry.GeometryCollection)):
        geom_by_ballon = defaultdict(shapely.geometry.LineString)
        for geom in intersec.geoms:
            for (ind, ballon_geom) in enumerate(ballon.geoms):
                if ballon_geom.contains(geom) and not isinstance(geom, shapely.geometry.Point):
                    geom_by_ballon[ind] = geom_by_ballon[ind].union(geom)
                    break
        return shapely.ops.cascaded_union([shapely.ops.linemerge(x) for x in geom_by_ballon.values()])
    print("Dupa totalna?")
    return intersec
