import collections
import dbm
import json
import logging
import os
import shelve
import tempfile
import time
import typing

import botocore.exceptions
import tqdm
from google.protobuf import message
from google.protobuf.descriptor import FieldDescriptor

T = typing.TypeVar('T')


class CacheError(Exception):
    pass


class CacheExpired(CacheError):
    pass


class CacheNotInitialized(CacheError):
    pass


class Serializer(object):
    def serialize(self, dct: dict) -> bytes:
        raise NotImplementedError

    def deserialize(self, data: bytes) -> dict:
        raise NotImplementedError


class JsonSerializer(Serializer):
    def serialize(self, dct: dict) -> bytes:
        return json.dumps(dct).encode('utf-8')

    def deserialize(self, data: bytes) -> dict:
        return json.loads(data.decode('utf-8'))


class ProtoSerializer(Serializer):
    def __init__(self, message_factory: typing.Callable[[], message.Message]):
        self.message_factory = message_factory

    def serialize(self, dct: dict):
        return dict_to_protobuf(dct, self.message_factory()).SerializeToString()

    def deserialize(self, data: bytes) -> dict:
        ret = self.message_factory()
        ret.ParseFromString(data)
        return protobuf_to_dict(ret)


class Cache(typing.Generic[T]):
    def get(self, name: str, default: T = None) -> typing.Optional[T]:
        raise NotImplementedError

    def add(self, name: str, value: T):
        raise NotImplementedError

    def delete(self, name: str):
        raise NotImplementedError

    def reload(self, contents: typing.Dict[str, T]):
        for key, value in contents.items():
            self.add(key, value)

    def __getitem__(self, item):
        return self.get(item)

    def __setitem__(self, key, value):
        self.add(key, value)

    def keys(self):
        raise NotImplementedError


class CacheDriver:
    def get_table(self, name: str, serializer: Serializer = JsonSerializer()) -> Cache:
        raise NotImplementedError

    def create(self, name: str, serializer: Serializer = JsonSerializer()) -> Cache:
        raise NotImplementedError

    def get_or_create(self, name: str, serializer: Serializer = JsonSerializer()) -> Cache:
        raise NotImplementedError


class ShelveCache(Cache):
    def __init__(self, shlv, serializer: Serializer):
        self.shelve = shlv
        self.serializer = serializer

    def get(self, name: str, default: dict = None) -> typing.Optional[dict]:
        ret = self.shelve.get(name)
        if ret:
            return self.serializer.deserialize(ret)
        if not ret and default:
            return default
        return None

    def add(self, name: str, value: dict):
        self.shelve[name] = self.serializer.serialize(value)

    def delete(self, name: str):
        del self.shelve[name]

    def keys(self):
        self.shelve.keys()


class ShelveCacheDriver(CacheDriver):
    def __init__(self):
        self.directory = os.path.join(tempfile.gettempdir(), "osm_cache")
        os.makedirs(self.directory, mode=0o755, exist_ok=True)

    def get_table(self, name: str, serializer: Serializer = JsonSerializer()) -> ShelveCache:
        try:
            ret = shelve.open(os.path.join(self.directory, name), flag='w')
            return ShelveCache(ret, serializer)
        except dbm.error:
            raise CacheNotInitialized(name)

    def create(self, name: str, serializer: Serializer = JsonSerializer()) -> ShelveCache:
        ret = shelve.open(os.path.join(self.directory, name), flag='n')
        return ShelveCache(ret, serializer)

    def get_or_create(self, name: str, serializer: Serializer = JsonSerializer()) -> ShelveCache:
        ret = shelve.open(os.path.join(self.directory, name), flag='c')
        return ShelveCache(ret, serializer)


class DynamoCache(Cache):
    _logger = logging.getLogger(__name__)

    def __init__(self, table, serializer: Serializer):
        self._table = table
        self.serializer = serializer

    def get(self, name: str, default: dict = None) -> typing.Optional[dict]:
        self._logger.info("Accessing key: %s from table: %s", name, self._table)
        ret = self._table.get_item(
            Key={
                'key': name
            }
        )
        if 'Item' in ret:
            ret = ret['Item']['value'].value
            if ret:
                return self.serializer.deserialize(ret)
        if default:
            return default
        return None

    def add(self, name: str, value: dict):
        self._table.put_item(
            Item={
                'key': name,
                'value': self.serializer.serialize(value)
            }
        )

    def delete(self, name: str):
        self._table.delete_item(
            Key={
                'key': name
            }

        )

    def reload(self, contents: dict):
        old_capacity = self._table.provisioned_throughput['WriteCapacityUnits']
        try:
            if old_capacity < 10:
                try:
                    self._set_write_capacity(10)
                except botocore.exceptions.ClientError:
                    pass
            with self._table.batch_writer() as batch:
                for k, v in tqdm.tqdm(contents.items()):
                    batch.put_item(
                        Item={
                            'key': k,
                            'value': self.serializer.serialize(v)
                        }
                    )
        finally:
            try:
                self._set_write_capacity(old_capacity)
            except botocore.exceptions.ClientError:
                pass

    def _set_write_capacity(self, capacity):
        self._table.meta.client.update_table(
            TableName=self._table.name,
            ProvisionedThroughput={
                'ReadCapacityUnits': self._table.provisioned_throughput['ReadCapacityUnits'],
                'WriteCapacityUnits': capacity
            })

    def keys(self):
        ret = self._table.scan(
            ProjectionExpression='#k',
            ExpressionAttributeNames={
                '#k': 'key'
            }
        )
        return (x['key'] for x in ret['Items'])


class DynamoCacheDriver(CacheDriver):
    def __init__(self, dynamodb):
        self.dynamodb = dynamodb

    def get_table(self, name: str, serializer: Serializer = JsonSerializer()) -> DynamoCache:
        ret = self.dynamodb.Table(name)
        # self.dynamodb.meta.client.describe_table(TableName=name)['Table']
        return DynamoCache(ret, serializer)

    def create(self, name: str, serializer: Serializer = JsonSerializer()) -> DynamoCache:
        ret = self.dynamodb.Table(name)
        if ret.item_count > 0:
            desc = self.dynamodb.meta.client.describe_table(TableName=name)['Table']
            desc = dict(
                (k, v) for (k, v) in desc.items() if k in
                ('AttributeDefinitions', 'TableName', 'KeySchema',
                 'LocalSecondaryIndexes', 'GlobalSecondaryIndexes',
                 'ProvisionedThroughput', 'StreamSpecification')
            )
            desc['ProvisionedThroughput'] = dict(
                (k, v) for k, v in desc['ProvisionedThroughput'].items() if k in
                ('ReadCapacityUnits', 'WriteCapacityUnits')
            )
            ret.meta.client.delete_table(TableName=name)
            waiter = self.dynamodb.meta.client.get_waiter('table_not_exists')
            waiter.wait(TableName=name)
            ret.meta.client.create_table(**desc)
            waiter = self.dynamodb.meta.client.get_waiter('table_exists')
            waiter.wait(TableName=name)
        return DynamoCache(ret, serializer)

    def get_or_create(self, name: str, serializer: Serializer = JsonSerializer()) -> DynamoCache:
        ret = self.dynamodb.Table(name)
        # self.dynamodb.meta.client.describe_table(TableName=name)['Table']
        return DynamoCache(ret, serializer)


class CacheManager(object):
    def __init__(self, cache_driver: CacheDriver):
        self.cache_driver = cache_driver
        meta = self.cache_driver.get_or_create('meta')

        if not meta:
            raise ValueError("Cache metadata not initialized")

        self.meta = meta

    def get_cache(self, name: str, version: int = None, serializer: Serializer = JsonSerializer()) \
            -> typing.Optional[Cache]:
        if name == "meta":
            raise ValueError("Forbidden cache name: meta")

        cache = self.meta.get(name)

        if not cache:
            raise CacheNotInitialized(name)

        if cache['status'] != 'ready':
            raise CacheNotInitialized(name)

        if (version and cache['version'] >= version) or not version:
            ret = self.cache_driver.get_table(name, serializer)
            return ret

        raise CacheExpired("Cache {0} not ready though metadata (status = {1}, version = {2} < requested {3} ".format(
            name, cache.get('status'), cache.get('version'), version))

    def create_cache(self, name: str, serializer: Serializer = JsonSerializer()) -> Cache:
        if name == "meta":
            raise ValueError("Forbidden cache name: meta")

        self.meta.add(name, {
            'status': 'creating',
            'updated': 0
        })
        return self.cache_driver.create(name, serializer)

    def mark_ready(self, name: str, version: int):
        desc = self.meta.get(name)
        desc['status'] = 'ready'
        desc['updated'] = time.time()
        desc['version'] = version
        self.meta.add(name, desc)

    def version(self, name: str):
        return self.meta.get(name, {}).get('version', -1)


if os.environ.get('USE_AWS'):
    import boto3

    __cache_manager = CacheManager(DynamoCacheDriver(boto3.resource('dynamodb')))
else:
    __cache_manager = CacheManager(ShelveCacheDriver())


def get_cache_manager():
    return __cache_manager


def groupby(lst: typing.Iterable, keyfunc=lambda x: x, valuefunc=lambda x: x):
    rv = collections.defaultdict(list)
    for i in lst:
        rv[keyfunc(i)].append(valuefunc(i))
    return rv


def parse_list(values: list, msg):
    """parse list to protobuf message"""
    if isinstance(values[0], dict):  # value needs to be further parsed
        for v in values:
            cmd = msg.add()
            parse_dict(v, cmd)
    else:  # value can be set
        msg.extend(values)


def parse_dict(values: dict, msg: message.Message):
    for k, v in values.items():
        if isinstance(v, dict):  # value needs to be further parsed
            parse_dict(v, getattr(msg, k))
        elif isinstance(v, list):
            parse_list(v, getattr(msg, k))
        else:  # value can be set
            setattr(msg, k, v)


def dict_to_protobuf(value, msg: message.Message) -> message.Message:
    parse_dict(value, msg)
    return msg


TYPE_CALLABLE_MAP = {
    FieldDescriptor.TYPE_DOUBLE: float,
    FieldDescriptor.TYPE_FLOAT: float,
    FieldDescriptor.TYPE_INT32: int,
    FieldDescriptor.TYPE_INT64: int,
    FieldDescriptor.TYPE_UINT32: int,
    FieldDescriptor.TYPE_UINT64: int,
    FieldDescriptor.TYPE_SINT32: int,
    FieldDescriptor.TYPE_SINT64: int,
    FieldDescriptor.TYPE_FIXED32: int,
    FieldDescriptor.TYPE_FIXED64: int,
    FieldDescriptor.TYPE_SFIXED32: int,
    FieldDescriptor.TYPE_SFIXED64: int,
    FieldDescriptor.TYPE_BOOL: bool,
    FieldDescriptor.TYPE_STRING: str,
    FieldDescriptor.TYPE_BYTES: lambda b: b.encode("base64"),
    FieldDescriptor.TYPE_ENUM: int,
}


def repeated(type_callable):
    return lambda value_list: [type_callable(value) for value in value_list]


def protobuf_to_dict(pb, type_callable_map=TYPE_CALLABLE_MAP):
    result_dict = {}
    for field, value in pb.ListFields():
        if field.type not in type_callable_map:
            raise TypeError("Field %s.%s has unrecognised type id %d" % (
                pb.__class__.__name__, field.name, field.type))
        type_callable = type_callable_map[field.type]
        if field.label == FieldDescriptor.LABEL_REPEATED:
            type_callable = repeated(type_callable)
        result_dict[field.name] = type_callable(value)
    return result_dict


# recursion, bitches.
TYPE_CALLABLE_MAP[FieldDescriptor.TYPE_MESSAGE] = protobuf_to_dict


# escaped split / join


def split(input: str, delimeter: str = ',', escape_char: str = '\\') -> typing.List[str]:
    ret = []
    n = 0
    start = 0

    def add_segment(s: str):
        ret.add(s.
                replace(escape_char+delimeter, delimeter).
                replace(escape_char+escape_char, escape_char)
                )

    while n < len(input):
        read_escape_char = False
        if input[n] == escape_char:
            read_escape_char = True
            n += 1
        if not read_escape_char and input[n] == delimeter:
            add_segment(input[start:n-1])
            start = n + 1
    add_segment(input[start:])
    return ret


def join(input: typing.List[str], delimeter: str = ',', escape_char: str = '\\') -> str:
    return delimeter.join(
        map(
            lambda x: x.
                replace(escape_char, escape_char + escape_char).
                replace(delimeter, escape_char + delimeter),
            input
        )
    )
