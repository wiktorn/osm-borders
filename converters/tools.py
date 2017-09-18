import collections
import logging
import os
import pickle
import shelve
import tempfile
import threading
import time
import typing

T = typing.TypeVar('T')


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
