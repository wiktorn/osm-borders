import collections
import json
import logging
import os
import pickle
import shelve
import tempfile
import threading
import time
import typing

import botocore.exceptions
import tqdm
from lz4.block import compress, decompress

T = typing.TypeVar('T')


class Cache(object):

    def get(self, name: str):
        raise NotImplementedError

    def add(self, name: str, value: dict):
        raise NotImplementedError

    def delete(self, name: str):
        raise NotImplementedError

    def reload(self, contents: dict):
        for key, value in contents.items():
            self.add(key, value)

    def __getitem__(self, item):
        return self.get(item)

    def __setitem__(self, key, value):
        self.add(key, value)


class CacheDriver:
    def get_table(self, name: str) -> Cache:
        raise NotImplementedError

    def create(self, name: str) -> Cache:
        raise NotImplementedError

    def get_or_create(self, name: str) -> Cache:
        raise NotImplementedError


class ShelveCache(Cache):
    def __init__(self, shlv):
        self.shelve = shlv

    def get(self, name: str) -> dict:
        return self.shelve.get(str)

    def add(self, name: str, value: dict):
        self.shelve[str] = value

    def delete(self, name: str):
        del self.shelve[str]


class ShelveCacheDriver(CacheDriver):
    def __init__(self):
        self.directory = os.path.join(tempfile.gettempdir(), "osm_cache")
        os.makedirs(self.directory, mode=0o755, exist_ok=True)

    def get_table(self, name: str) -> ShelveCache:
        ret = shelve.open(os.path.join(self.directory, name), flag='w')
        return ShelveCache(ret)

    def create(self, name: str) -> ShelveCache:
        ret = shelve.open(os.path.join(self.directory, name), flag='n')
        return ShelveCache(ret)

    def get_or_create(self, name: str) -> ShelveCache:
        ret = shelve.open(os.path.join(self.directory, name), flag='c')
        return ShelveCache(ret)


class DynamoCache(Cache):
    def __init__(self, table):
        self._table = table

    def get(self, name: str) -> dict:
        return json.loads(
            decompress(
                    self._table.get_item(
                        Key={
                            'key':  name
                        }
                    )['Item']['value'].value).decode('utf-8')
            )

    def add(self, name: str, value: dict):
        self._table.put_item(
            Item={
                'key':  name,
                'value':
                    compress(
                        json.dumps(value).encode('utf-8'),
                        mode='high_compression')
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
                            'value': compress(
                                json.dumps(v).encode('utf-8'),
                                mode='high_compression')
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


class DynamoCacheDriver(CacheDriver):
    def __init__(self, dynamodb):
        self.dynamodb = dynamodb

    def get_table(self, name: str) -> DynamoCache:
        ret = self.dynamodb.Table(name)
        self.dynamodb.meta.client.describe_table(TableName=name)['Table']
        return DynamoCache(ret)

    def create(self, name: str) -> DynamoCache:
        ret = self.dynamodb.Table(name)
        if ret.item_count > 0:
            desc = self.dynamodb.meta.client.describe_table(TableName=name)['Table']
            desc = dict(
                (k,v) for (k,v) in desc.items() if k in
                ('AttributeDefinitions', 'TableName', 'KeySchema',
                 'LocalSecondaryIndexes', 'GlobalSecondaryIndexes',
                 'ProvisionedThroughput', 'StreamSpecification')
            )
            desc['ProvisionedThroughput'] = dict(
                (k, v) for k,v in desc['ProvisionedThroughput'].items() if k in
                ('ReadCapacityUnits', 'WriteCapacityUnits')
            )
            ret.meta.client.delete_table(TableName=name)
            waiter = self.dynamodb.meta.client.get_waiter('table_not_exists')
            waiter.wait(TableName=name)
            ret.meta.client.create_table(**desc)
            waiter = self.dynamodb.meta.client.get_waiter('table_exists')
            waiter.wait(TableName=name)
        return DynamoCache(ret)

    def get_or_create(self, name: str) -> DynamoCache:
        ret = self.dynamodb.Table(name)
        self.dynamodb.meta.client.describe_table(TableName=name)['Table']
        return DynamoCache(ret)


class CacheManager(object):
    def __init__(self, cache_driver: CacheDriver):
        self.cache_driver = cache_driver
        meta = self.cache_driver.get_or_create('meta')

        if not meta:
            raise ValueError("Cache metadata not initialized")

        self.meta = meta

    def get_cache(self, name: str, version: int) -> Cache:
        if name == "meta":
            raise ValueError("Forbidden cache name: meta")

        cache = self.meta.get(name)

        if not cache:
            return

        if cache['status'] == 'ready' and cache['version'] >= version:
            ret = self.cache_driver.get_table(name)
            if not ret:
                raise ValueError("Cache {0} not ready though metadata".format(name))
            return ret

        return

    def create_cache(self, name: str) -> Cache:
        if name == "meta":
            raise ValueError("Forbidden cache name: meta")

        self.meta.add(name, {
            'status': 'creating',
            'updated': 0
        })
        return self.cache_driver.create(name)

    def mark_ready(self, name: str, version: str):
        desc = self.meta.get(name)
        desc['status'] = 'ready'
        desc['updated'] = time.time()
        desc['version'] = version
        self.meta.add(name, desc)


if os.environ.get('AWS_DEFAULT_REGION'):
    import boto3
    __cache_manager = CacheManager(DynamoCacheDriver(boto3.resource('dynamodb')))
else:
    __cache_manager = CacheManager(ShelveCacheDriver())


def get_cache_manager():
    return __cache_manager


class CachedDictionary(typing.Generic[T]):
    __log = logging.getLogger(__name__)

    def __init__(self, name, func: typing.Callable[[], typing.Dict[str, T]], ttl=180 * 24 * 60 * 60):
        self.filename = os.path.join(tempfile.gettempdir(), name)
        self.func = func
        self.dct = None
        self.lock = threading.Lock()
        self.ttl = ttl

    def __getitem__(self, item):
        return self.__getitem_monkey_patch(item)

    def _monkey_patch(self):
        with self.lock:
            if not self.dct:  # check if we have initialized
                try:
                    with open(self.filename, "rb") as f:
                        data = pickle.load(f)
                except IOError:
                    self.__log.debug("Can't read a file: %s, starting with a new one", self.filename, exc_info=True)
                    data = {
                        'time': 0
                    }
                if data['time'] + self.ttl < time.time():
                    new = self.func()
                    data['time'] = time.time()
                    with shelve.open(self.filename + '.shlv', flag='n') as dct:
                        dct.update(new)
                    with open(self.filename, 'wb') as f:
                        pickle.dump(data, f)

                self.dct = shelve.open(self.filename + '.shlv', flag='r')
                # monkey patch the instance
                self.__getitem_monkey_patch = self._getitem___after
                self.keys = self.keys_after
                self.items = self.__items_after
                # free context, as it will be no longer needed
                self.func = None
                self.lock = None

    def __getitem_monkey_patch(self, item: str) -> T:
        self._monkey_patch()
        return self.__getitem__(item)

    def _getitem___after(self, item: str) -> T:
        if not item:
            # noinspection PyTypeChecker
            return None
        return self.dct[item]

    def get(self, item: str) -> T:
        try:
            return self[item]
        except KeyError:
            # noinspection PyTypeChecker
            return None

    def keys(self) -> typing.Iterable[str]:
        self._monkey_patch()
        return self.keys()

    def keys_after(self) -> typing.Iterable[str]:
        return self.dct.keys()

    def items(self) -> typing.ItemsView[str, T]:
        self._monkey_patch()
        return self.items()

    def __items_after(self) -> typing.ItemsView[str, T]:
        return self.dct.items()


def groupby(lst: typing.Iterable, keyfunc=lambda x: x, valuefunc=lambda x: x):
    rv = collections.defaultdict(list)
    for i in lst:
        rv[keyfunc(i)].append(valuefunc(i))
    return rv
