import dbconnect
from datetime import datetime, date
from . import exceptions
from typing import Union, List, Type
from hashlib import sha1


__version__ = "1.8.5"


OPERATORS = {
    "==": "=",
    "<<": "<",
    ">>": ">",
    "<=": "<=",
    ">=": ">=",
    "IS": "IS NULL"
}


def intersect(*args) -> list:
    """
    Creates a conjunction between multiple lists
    :param args: Lists to join
    :return: A ∪ B ∪ C ∪ ... ∪ Z
    """
    _list = list(args)
    result = []
    length = len(_list)

    if length == 1:
        return _list[0]

    conjunction = list(set(_list[0]) & set(_list[1]))
    _list.pop(1); _list.pop(0)
    _list.insert(0, conjunction)
    result.append(conjunction)

    if length > 2:
        while len(_list) > 1:
            conjunction = list(set(_list[0]) & set(_list[1]))
            _list.pop(1); _list.pop(0)
            _list.insert(0, conjunction)
            result.append(conjunction)

    return result[0]


def searches(table: list, attr: str, value: any) -> list:
    """Searches through a list for given attributes"""
    return [x for x in table if getattr(x, attr) == value]


def search(table: list, attr: str, value: any) -> any:
    """Searches through a list for given attributes and returns one item"""
    lst = searches(table, attr, value)
    if len(lst) > 1:
        raise exceptions.ResponseAmbiguousError("There is more than one object matching the given description. Try using searches().")
    elif len(lst) < 1:
        raise KeyError("There is no object matching the given description.")
    return lst[0]


def sql_format(attr: any):
    if isinstance(attr, datetime) or isinstance(attr, date):
        return attr.strftime("%Y-%m-%d %H:%M:%S")
    return attr


def set_adapter(server: str, schema: str, verbose: bool) -> dbconnect.Adapter:
    if (server is Ellipsis) or (schema is Ellipsis) or (verbose is Ellipsis):
        return ...
    return dbconnect.Adapter(server, schema, verbose)


class ResponseObjectList(list):
    def __init__(self, _list: list):
        super().__init__(_list)
        self.data = _list
        types = list({type(x) for x in _list})
        if len(types) == 1:
            self.type: Type[SQLObject] = types[0]
        elif len(types) == 0:
            self.type = None
        else:
            raise ValueError("Objects in the list must be of one type only")

    def __getitem__(self, item):
        return self.data[item]

    def select(self, item):
        """
        Attemps to fetch an object with a primary key of the given value from the list.
        :param item: Value of the primary key
        :return: Object
        """
        if len(self.data) == 0:
            raise IndexError("The list is empty")
        return search(self.data, self.type.PRIMARY_KEY, item)

    def selectwhere(self, **kwargs) -> list:
        if len(self.data) == 0:
            raise IndexError("The list is empty")
        selections = []
        for k, v in kwargs.items():
            results = set(searches(self.data, k, v))
            selections.append(results)
        return list(intersect(*selections))


class SQLObject:
    SERVER_NAME: str = ...
    SCHEMA_NAME: str = ...
    TABLE_NAME: str = ...
    VERBOSE: bool = True

    SQL_KEYS: List[str] = ...
    PRIMARY_KEY: str = ...

    OPERATORS = OPERATORS

    CLS_ENV = {}
    DO_REFRESH: bool = True

    def __init__(self):
        self.ENV = {}
        self._cache = DictCache()

    @classmethod
    def _db(cls) -> dbconnect.Adapter:
        return set_adapter(cls.SERVER_NAME, cls.SCHEMA_NAME, cls.VERBOSE)

    @classmethod
    def _retrieve(cls, constrictions: dict = None):
        """Fetches data from the database under given constrictions"""
        where = "WHERE "
        if constrictions:
            for k, v in constrictions.items():
                operator = v[:2]
                if operator == "IS":
                    where += f"{k} {cls.OPERATORS[operator]} AND "
                else:
                    where += f"{k} {cls.OPERATORS[operator]} {v[2:]!r} AND "
            where = where.strip(" AND ")
        return cls._db().query(f"SELECT * FROM {cls.TABLE_NAME} {where}".strip("WHERE "))

    def primary_value(self):
        return getattr(self, self.PRIMARY_KEY)

    def argsdict(self) -> dict:
        """Creates a dictionary from all SQL keys"""
        return {k: getattr(self, k) for k in self.SQL_KEYS}

    def args(self, keys: Union[list, None] = None) -> tuple:
        """Returns a tuple of the object attributes represented as positional arguments

        :param keys: Which attributes to include. `None` if every attr should be included.
        :returns: `tuple`
        """
        keys = self.SQL_KEYS if keys is None else keys
        return tuple(getattr(self, key) for key in keys)

    def kwargs(self, keys: Union[list, None] = None) -> str:
        """DEPRECATED.
        Returns a comma separated list of the object attributes represented as SQL-style keyword arguments

        :param keys: Which attributes to include. `None` if every attr should be included.
        :returns: `str`
        """
        formatted_pairs = ""
        keys = self.SQL_KEYS if keys is None else keys
        for k in keys:
            attr = getattr(self, k)
            if attr is None:
                formatted_pairs += f"{k} = NULL, "
                continue
            formatted_pairs += f"{k} = {sql_format(attr)!r}, "
        return formatted_pairs.strip(", ")

    def __eq__(self, other):
        return self.primary_value() == other.primary_value()

    def __hash__(self):
        return int(sha1(f"{self.SERVER_NAME}/{self.SCHEMA_NAME}/{self.TABLE_NAME}/{self.PRIMARY_KEY}".encode()).hexdigest(), 16)

    @classmethod
    def _toenv(cls):
        """See PlWiki for documentation"""
        ...

    @classmethod
    def refresh_env(cls):
        return cls._toenv()

    @staticmethod
    def construct(response) -> list:
        """Takes in a SQL response and returns a list of objects"""
        raise NotImplementedError

    @classmethod
    def gets(cls, refresh: bool = True, **kwargs) -> ResponseObjectList:
        """Retrieves a list of objects from the database."""
        if refresh and cls.DO_REFRESH:
            cls._toenv()
        if not kwargs:
            return ResponseObjectList(cls.construct(cls._retrieve()))
        for k, v in kwargs.items():
            if v is None:
                kwargs[k] = "IS"
            elif str(v)[:2] not in cls.OPERATORS:
                kwargs[k] = "==" + str(v)
        return ResponseObjectList(cls.construct(cls._retrieve(kwargs)))

    @classmethod
    def get(cls, primary_value=None, refresh: bool = True, **kwargs):
        """Retrieves the object from the database if it has only one element."""
        if primary_value is not None:
            elements = cls.gets(refresh=refresh, **{cls.PRIMARY_KEY: primary_value}, **kwargs)
        else:
            elements = cls.gets(refresh=refresh, **kwargs)

        if len(elements) > 1:
            raise exceptions.ResponseAmbiguousError("There is more than one object matching the given description. Try using gets().")
        elif len(elements) < 1:
            raise KeyError("There is no object matching the given description.")
        return elements[0]

    def commit(self) -> None:
        keys_lst = [k for k in self.SQL_KEYS if getattr(self, k) is not Ellipsis]
        keys = ""
        for k in keys_lst:
            keys += (k + ", ")
        keys = keys.strip(", ")

        if not self.exists(self.primary_value()):
            self._db().query(f"INSERT INTO {self.TABLE_NAME} ({keys}) VALUES ({('%s, '*len(keys_lst)).strip(', ')})", self.args(keys_lst))
        else:
            kw_keys = ""
            for key in keys_lst:
                kw_keys += f"{key} = %s, "
            self._db().query(f"UPDATE {self.TABLE_NAME} SET {kw_keys.strip(', ')} WHERE {self.PRIMARY_KEY} = %s",
                             self.args(keys_lst) + (self.primary_value(),))

    @classmethod
    def get_next_id(cls):
        try:
            data = cls.gets()[-1]
        except IndexError:
            return 1
        result = data.primary_value() + 1
        try:
            return result
        except TypeError:
            raise TypeError(f"Primary key needs to be int, not {type(result)}.")

    @classmethod
    def get_increment(cls) -> int:
        """Checks for gaps in the sequence of primary values of objects.
        :return: The first missing primary value in the sequence.
        """
        objs = cls.gets()
        print(objs)
        i = None
        previous = None
        for obj in objs:
            if not previous:
                previous = obj
            expected = previous.primary_value() + 1
            i = expected
            print(expected)
            if not previous:
                continue
            if not expected == obj.primary_value():
                print(f"returning {expected}")
                return expected
            previous = obj
        print(f"found no gap, so {i + 1}")
        return i + 1

    @classmethod
    def exists(cls, value_primary: any):
        try:
            kwargs = {cls.PRIMARY_KEY: value_primary}
            cls.get(**kwargs)
            return True
        except KeyError:
            return False

    @classmethod
    def fetchs(cls, **kwargs) -> ResponseObjectList:
        """Retrieves a list of objects from the database. Returns [] if no matches are found.

        :param kwargs: Keyword Arguments
        :returns: `ResponseObjectList`
        """
        try:
            return cls.gets(**kwargs)
        except KeyError:
            return ResponseObjectList([])

    @classmethod
    def fetch(cls, primary_value=None, **kwargs) -> any:
        """Retrieves the object from the database if it has only one element.
        Does not raise error when no matches are found

        :param primary_value: (Optional) The value of the primary key
        :param kwargs: Keyword Arguments
        :returns: Object or `None`
        """
        try:
            if primary_value is not None:
                return cls.get(primary_value, **kwargs)
            else:
                return cls.get(**kwargs)
        except KeyError:
            return None

    def cache(self, key: any, func: callable) -> any:
        """Used to retrieve value from instance environment cache and assign it to cache if it is not already there.

        :param key: Key in the environment dictionary.
        :param func: Function that returns the wanted value
        :return: The value from cache.
        """
        return self._cache.cache(key, func)


class Cache:
    """DEPRECATED, use `SQLObject.cache` instead"""
    def __init__(self, stored_type, attr=None):
        self.cache = []
        self.cls = stored_type
        self.attr = self.cls.PRIMARY_KEY if attr is None else attr

    def __getitem__(self, item):
        try:
            return search(self.cache, self.attr, item)
        except KeyError:
            obj = self.cls.get(**{self.attr: item})
            self.cache.append(obj)
            return obj


class DictCache:
    def __init__(self):
        self._cache = {}

    def cache(self, key: any, func: callable) -> any:
        """Used to retrieve value from instance environment cache and assign it to cache if it is not already there.

        :param key: Key in the environment dictionary.
        :param func: Function that returns the wanted value
        :return: The value from cache.
        """
        try:
            return self._cache[key]
        except KeyError:
            self._cache[key] = func()
            return self._cache[key]

    def __repr__(self) -> str:
        return str(self._cache)
