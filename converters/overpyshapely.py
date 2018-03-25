import overpy
import shapely.geometry
import shapely.ops

from .feature import Feature


class OverToShape(object):
    def __init__(self, result: overpy.Result):
        self._result = result

    @staticmethod
    def _default_id(lst: list) -> int:
        if not lst:
            raise ValueError("No relation")
        if len(lst) > 1:
            raise ValueError("Expected at most one relation, got: {0}".format(", ".join(lst)))
        return lst[0]

    def get_node_feature(self, id_: int = None) -> Feature:
        if not id_:
            id_ = OverToShape._default_id(self._result.node_ids)
        node = self._result.get_node(id_)
        return Feature(OverToShape._node_to_shapely(node), node.tags)

    def get_way_feature(self, id_: int = None) -> Feature:
        if not id_:
            id_ = OverToShape._default_id(self._result.way_ids)
        way = self._result.get_way(id_)
        return Feature(OverToShape._way_to_shapely(way), way.tags)

    def get_relation_feature(self, id_: int = None) -> Feature:
        if not id_:
            id_ = OverToShape._default_id(self._result.relation_ids)
        relation = self._result.get_relation(id_)
        return Feature(OverToShape._relation_to_shapely(relation), relation.tags)

    def get_relation_feature_multi(self, id_: int = None) -> Feature:
        if not id_:
            id_ = OverToShape._default_id(self._result.relation_ids)
        relation = self._result.get_relation(id_)
        return Feature(
            shapely.geometry.MultiLineString([OverToShape._obj_to_shapely(x) for x in relation.members]),
            relation.tags
        )

    @staticmethod
    def _obj_to_shapely(obj: overpy.Element) -> shapely.geometry.base.BaseGeometry:
        if isinstance(obj, overpy.Node):
            return OverToShape._node_to_shapely(obj)
        if isinstance(obj, overpy.Way):
            return OverToShape._way_to_shapely(obj)
        if isinstance(obj, overpy.Relation):
            return OverToShape._relation_to_shapely(obj)
        if isinstance(obj, (overpy.RelationNode, overpy.RelationWay)):
            return OverToShape._obj_to_shapely(obj.resolve())
        raise ValueError("Unexpected object type: {0}".format(type(obj)))

    @staticmethod
    def _node_to_shapely(obj: overpy.Node) -> shapely.geometry.Point:
        return shapely.geometry.Point([obj.lon, obj.lat])

    @staticmethod
    def _way_to_shapely(obj: overpy.Way) -> shapely.geometry.base.BaseGeometry:
        if obj.nodes[0].id == obj.nodes[-1].id:
            # closed way
            return shapely.geometry.Polygon([(x.lon, x.lat) for x in obj.get_nodes()])
        return shapely.geometry.LineString([OverToShape._node_to_shapely(x) for x in obj.get_nodes()])

    @staticmethod
    def _relation_to_shapely(obj: overpy.Relation) -> shapely.geometry.Polygon:
        if any(x.role == "inner" for x in obj.members) or any(x.role == "outer" for x in obj.members):
            inner = shapely.ops.cascaded_union(
                shapely.ops.polygonize_full(
                    [OverToShape._obj_to_shapely(x) for x in obj.members if x.role == "inner"]
                )
            )
            outer = shapely.ops.cascaded_union(
                shapely.ops.polygonize_full(
                    [OverToShape._obj_to_shapely(x) for x in obj.members if x.role == "outer"]
                )
            )
            return outer.difference(inner)

        # assume everything is outer
        return shapely.ops.cascaded_union(
            shapely.ops.polygonize_full([OverToShape._obj_to_shapely(x) for x in obj.members])
        )
