import dbconnect
from datetime import datetime
from . import exceptions
from typing import Union, List


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
    if isinstance(attr, datetime):
        return attr.strftime("%Y-%m-%d %H:%M:%S")
    return attr


def set_adapter(server: str, schema: str, verbose: bool) -> dbconnect.Adapter:
    if (server is Ellipsis) or (schema is Ellipsis) or (verbose is Ellipsis):
        return ...
    return dbconnect.Adapter(server, schema, verbose)


class SQLObject:
    SERVER_NAME: str = ...
    SCHEMA_NAME: str = ...
    TABLE_NAME: str = ...
    VERBOSE: bool = True

    SQL_KEYS: List[str] = ...
    PRIMARY_KEY: str = ...

    OPERATORS = {
        "==": "=",
        "<<": "<",
        ">>": ">",
        "<=": "<=",
        ">=": ">="
    }


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
                where += f"{k} {cls.OPERATORS[operator]} {v[2:]}, "
            where = where.strip(", ")
        return cls._db().query(f"SELECT * FROM {cls.TABLE_NAME} {where}".strip("WHERE "))

    def primary_value(self):
        return getattr(self, self.PRIMARY_KEY)

    def argsdict(self) -> dict:
        """Creates a dictionary from all SQL keys"""
        return {k: getattr(self, k) for k in self.SQL_KEYS}

    def args(self, keys: Union[list, None] = None):
        formatted_pairs = ""
        keys = self.SQL_KEYS if keys is None else keys
        for k in keys:
            attr = getattr(self, k)
            if attr is None:
                formatted_pairs += f"NULL, "
                continue
            formatted_pairs += f"{sql_format(attr)!r}, "
        return formatted_pairs.strip(", ")

    def kwargs(self, keys: Union[list, None] = None):
        formatted_pairs = ""
        keys = self.SQL_KEYS if keys is None else keys
        for k in keys:
            attr = getattr(self, k)
            if attr is None:
                formatted_pairs += f"{k} = NULL, "
                continue
            formatted_pairs += f"{k} = {sql_format(attr)!r}, "
        return formatted_pairs.strip(", ")

    @staticmethod
    def construct(response) -> list:
        """TO BE OVERRIDEN: Takes in a SQL response and returns a list of objects"""
        pass

    @classmethod
    def gets(cls, **kwargs) -> List:
        """Retrieves a list of objects from the database."""
        if not kwargs:
            return cls.construct(cls._retrieve())
        for k, v in kwargs.items():
            if str(v)[:2] not in cls.OPERATORS:
                kwargs[k] = "==" + str(v)
        return cls.construct(cls._retrieve(kwargs))

    @classmethod
    def get(cls, **kwargs):
        """Retrieves the object from the database if it has only one element."""
        elements = cls.gets(**kwargs)
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

        if len(searches(self.gets(), self.PRIMARY_KEY, self.primary_value())) == 0:
            self._db().query(f"INSERT INTO {self.TABLE_NAME} ({keys}) VALUES ({self.args(keys_lst)})".strip(", "))
        else:
            self._db().query(f"UPDATE {self.TABLE_NAME} SET {self.kwargs(keys_lst)} WHERE {self.PRIMARY_KEY} = {self.primary_value()}")

    @classmethod
    def get_next_id(cls):
        try:
            data = cls.gets()[-1]
        except IndexError:
            return 0
        result = data.primary_value() + 1
        try:
            return result
        except TypeError:
            raise TypeError(f"Primary key needs to be int, not {type(result)}.")

    @classmethod
    def exists(cls, value_primary: any):
        try:
            kwargs = {cls.PRIMARY_KEY: value_primary}
            cls.get(**kwargs)
            return True
        except KeyError:
            return False


class Cache:
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
