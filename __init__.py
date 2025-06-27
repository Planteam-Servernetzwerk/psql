import dbconnect
from datetime import datetime, date
from . import exceptions
import typing as t
from typing import Generic, Union, List, Type, Callable
from hashlib import sha1
from functools import lru_cache
from types import EllipsisType


__version__ = "1.14"


SQLType = t.TypeVar("SQLType", bound="SQLObject")
T = t.TypeVar("T")
NullAllowed: t.TypeAlias = t.Union[T, EllipsisType]


OPERATORS = {
    "==": "=",
    "<<": "<",
    ">>": ">",
    "<=": "<=",
    ">=": ">=",
    "IS": "IS NULL"
}


def intersect(*args) -> list:
    """@brief Erstellt eine Konjunktion mindestens zweier Mengen.

    @param args Mengen
    @returns A ∪ B ∪ C ∪ ... ∪ Z
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


class ResponseObjectList(t.List[T], t.Generic[T]):
    """@brief Listenartiges Objekt für SQL-Rückgaben."""

    def __init__(self, _list: List[T]):
        """@brief Konstruktor.

        @param _list Liste an Objekten gleichen Typs.
        """
        super().__init__(_list)
        self.data = _list
        types = list({type(x) for x in _list})
        if len(types) == 1:
            self.type: Type[T] = types[0]
        elif len(types) == 0:
            self.type = None
        else:
            raise ValueError("Objects in the list must be of one type only")

    def __getitem__(self, item):
        return self.data[item]

    def select(self, item):
        """@brief Versucht, ein einziges Objekt eines angegebenen Wertes für den Primärschlüssel aus der Liste zu finden.

        @param item Wert des Primärschlüssels
        @returns Objekt
        @throws IndexError Falls die Liste leer ist.
        """
        if len(self.data) == 0:
            raise IndexError("The list is empty")
        return search(self.data, self.type.PRIMARY_KEY, item)

    def selectwhere(self, **kwargs) -> list:
        """@brief Versucht, alle Objekte aus der Liste zu finden, die die über die Keyword-Arguments übermittelten Werte haben.

        @param kwargs Erforderliche Werte (Values) für Spalten (Keys)
        @returns Alle in Frage kommenden Objekte
        @throws IndexError Falls die Liste leer ist.
        """
        if len(self.data) == 0:
            raise IndexError("The list is empty")
        result = []
        for datum in self.data:
            matches = True
            for k, v in kwargs.items():
                if getattr(datum, k) != v:
                    matches = False
                    break
            if matches:
                result.append(datum)
        return result


class SQLObject:
    """@brief Schablone für die Darstellung einer SQL-Tabelle. Von ihr muß geerbt werden."""

    ## @var SERVER_NAME
    #  @brief `dbconnect`-kompatible Adresse des SQL-Servers.
    SERVER_NAME: str = ...

    ## @var SCHEMA_NAME
    #  @brief Name der SQL-Datenbank.
    SCHEMA_NAME: str = ...

    ## @var TABLE_NAME
    #  @brief Name der SQL-Tabelle.
    TABLE_NAME: str = ...

    ## @var VERBOSE
    #  @brief Ob Ausgaben in `stdout` sichtbar (`True`) oder nicht (`False`) sein sollen.
    VERBOSE: bool = True


    ## @var SQL_KEYS
    #  @brief Liste sämtlicher Spaltennamen der SQL-Tabelle in richtiger Reihenfolge.
    #  Die Klasse muß jeden aufgeführten Spaltennamen in gleicher Schreibweise als Attribut, wie immer gearthet, definieren.
    SQL_KEYS: List[str] = ...

    ## @var PRIMARY_KEY
    #  @brief Spaltenname des Primärschlüssels.
    PRIMARY_KEY: str = ...

    
    ## @var OPERATORS
    #  Abbildung von SQL-Operatoren wie `=, >, <, >=, NOT` auf zweistellige Zeichenketten, um Ungleichungen in \ref gets umzusetzen.
    OPERATORS = OPERATORS


    ## @var CLS_ENV
    #  @brief Cache
    #  @deprecated Veraltet
    CLS_ENV = {}

    ## @var DO_REFRESH
    #  @brief Flagge zur Cache-Invalidierung
    #  @deprecated Veraltet
    DO_REFRESH: bool = True


    ## @var adapter
    #  @brief `dbconnect.Adapter`-Objekt, mit dem Abfragen durchgeführt werden.
    adapter = None

    def __init__(self):
        self.ENV = {}
        self._cache = DictCache()

    @classmethod
    def _db(cls) -> dbconnect.Adapter:
        """@returns `dbconnect.Adapter`-Objekt aus den vorher definierten Werten zu Serveradresse und Datenbankname"""
        if cls.adapter is None:
            cls.adapter = set_adapter(cls.SERVER_NAME, cls.SCHEMA_NAME, cls.VERBOSE)
        return cls.adapter


    @classmethod
    def db(cls) -> dbconnect.Adapter:
        """@brief Alias für \ref _db"""
        #cls.gets.cache_clear()
        return cls._db()

    @classmethod
    def _retrieve(cls, constrictions: dict = None):
        """@brief Gibt alle Spalten aus der Tabelle zurück, die ggf. gewisse Suchkriterien erfüllen.

        @param constrictions (optional) Suchkriterien im Key-Value-Format.
        """
        where = "WHERE "
        values = []
        if constrictions:
            for k, v in constrictions.items():
                if v is None:
                    where += f"{k} IS NULL AND "
                else:
                    values.append(v)
                    where += f"{k} = %s AND "
            where = where.strip(" AND ")
        return cls._db().query(f"SELECT * FROM {cls.TABLE_NAME} {where}".strip("WHERE "), tuple(values))

    def primary_value(self):
        """@returns Wert des Primärschlüssels"""
        return getattr(self, self.PRIMARY_KEY)

    def argsdict(self) -> dict:
        """@brief Stellt die SQL-Attribute des Objektes in einem Dictionary dar."""
        return {k: getattr(self, k) for k in self.SQL_KEYS}

    def args(self, keys: Union[list, None] = None) -> tuple:
        """@brief Erstellt ein Tupel, das alle SQL-Attribute in der in \ref SQL_KEYS angegebenen Reihenfolge enthält.

        @param keys (optional) Liste der Spalten, die berücksichtigt werden sollen. `None` für alle aus \ref SQL_KEYS.
        """
        keys = self.SQL_KEYS if keys is None else keys
        return tuple(getattr(self, key) for key in keys)

    def kwargs(self, keys: Union[list, None] = None) -> str:
        """@brief Returns a comma separated list of the object attributes represented as SQL-style keyword arguments
        @deprecated Veraltet

        @param keys (optional) Liste der Spalten, die berücksichtigt werden sollen. `None` für alle aus \ref SQL_KEYS.
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

    @staticmethod
    def construct(response) -> list:
        """@brief Erstellt eine Liste an eigenen Objekten aus einer SQL-Rückgabe."""
        raise NotImplementedError

    @classmethod
    #@lru_cache(1024)
    def gets(cls: t.Type[SQLType], refresh: bool = True, **kwargs) -> ResponseObjectList[SQLType]:
        """@brief Erstellt eine \ref ResponseObjectList aller Elemente der Tabelle. Wahlweise unter Berücksichtigung von Suchkriterien.

        @param kwargs (optional) Suchkriterien im Key-Value-Format
        @throws KeyError Falls es kein Element gibt, das den Suchkriterien entspricht.
        """
        if not kwargs:
            return ResponseObjectList(cls.construct(cls._retrieve()))
        return ResponseObjectList(cls.construct(cls._retrieve(kwargs)))

    @classmethod
    def get(cls: t.Type[SQLType], primary_value=None, refresh: bool = True, **kwargs) -> SQLType:
        """@brief Gibt, wenn es nur ein Element der angegebenen Suchkriterien in der Tabelle gibt, genau dieses zurück.

        @param primary_value (optional) Matcht nur Einträge mit diesem Wert als Primärschlüsselwert.
        @param kwargs (optional) Andere Suchkriterien im Key-Value-Format
        @throws ResponseAmbiguousError Wenn es mehr als ein solches Element gibt
        @throws KeyError Wenn es kein solches Element gibt
        """
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
        """@brief Aktualisiert die SQL-Tabelle und fügt das Objekt in sie ein bzw. verändert ein bestehendes Element mit demselben Primärschlüsselwert"""
        keys_lst = [k for k in self.SQL_KEYS if getattr(self, k) is not Ellipsis]
        keys = ""
        for k in keys_lst:
            keys += (k + ", ")
        keys = keys.strip(", ")

        insert = False

        if self.primary_value() is Ellipsis:
            insert = True
        else:
            if not self.exists(self.primary_value()):
                insert = True

        if insert:
            self.db().query(f"INSERT INTO {self.TABLE_NAME} ({keys}) VALUES ({('%s, '*len(keys_lst)).strip(', ')})", self.args(keys_lst))
        else:
            kw_keys = ""
            for key in keys_lst:
                kw_keys += f"{key} = %s, "
            self.db().query(f"UPDATE {self.TABLE_NAME} SET {kw_keys.strip(', ')} WHERE {self.PRIMARY_KEY} = %s",
                             self.args(keys_lst) + (self.primary_value(),))

    @classmethod
    def get_next_id(cls):
        """@brief Berechnet den höchsten in der Tabelle vertretene numerischen Primärschlüsselwert und inkrementiert diesen."""
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
        """@brief Bestimmt den ersten nicht in der Tabelle vertretenen numerischen Primärschlüssel"""
        objs = cls.gets()

        if len(objs) == 0:
            return 1

        primary_values = [x.primary_value() for x in objs]
        for i in range(1, primary_values[-1] + 1, 1):
            if i not in primary_values:
                return i
        return primary_values[-1] + 1

    @classmethod
    def exists(cls, value_primary: any) -> bool:
        """@brief Bestimmt, ob ein Element des angegebenen Primärschlüsselwertes existiert"""
        try:
            kwargs = {cls.PRIMARY_KEY: value_primary}
            cls.get(**kwargs)
            return True
        except KeyError:
            return False

    @classmethod
    def fetchs(cls: t.Type[SQLType], **kwargs) -> ResponseObjectList[SQLType]:
        """@brief Entspricht \ref gets ohne eine Exception zu werfen
        Anstatt eine Exception zu werfen, wird eine leere Liste zurückgegeben.
        """
        try:
            return cls.gets(**kwargs)
        except KeyError:
            return ResponseObjectList([])

    @classmethod
    def fetch(cls: t.Type[SQLType], primary_value=None, **kwargs) -> t.Optional[SQLType]:
        """@brief Entspricht \ref get ohne eine Exception zu werfen
        Anstatt eine Exception zu werfen, wird `None` zurückgegeben.
        """
        try:
            if primary_value is not None:
                return cls.get(primary_value, **kwargs)
            else:
                return cls.get(**kwargs)
        except KeyError:
            return None

    def cache(self, key: any, func: callable) -> any:
        """@brief Used to retrieve value from instance environment cache and assign it to cache if it is not already there.
        @deprecated Veraltet

        @param key Key in the environment dictionary.
        @param func Function that returns the wanted value
        @returns The value from cache.
        """
        return self._cache.cache(key, func)


class Lookup(Generic[SQLType]):
    """@brief Creates a dictionary-like lookup of a psql table with a defined key"""
    def __init__(self, table: Type[SQLType], key: Union[str, Callable] = lambda o: o.primary_value()) -> None:
        objs = table.gets()
        __key = key if callable(key) else lambda o: getattr(o, key)
        self.table = table
        self.lookup = {__key(obj): obj for obj in objs}

    def __getitem__(self, k) -> SQLType:
        if k is None:
            return None
        return self.lookup[k]

    def __repr__(self) -> str:
        return f"<psql.Lookup {self.lookup}>"


class Cache:
    """@deprecated Lange obsolet"""
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
    """@brief Dictionary-artiger Typ, der als Cache fungiert
    @deprecated Veraltet
    """

    def __init__(self):
        self._cache = {}

    def cache(self, key: any, func: callable) -> any:
        """@brief Used to retrieve value from instance environment cache and assign it to cache if it is not already there.
        @deprecated Veraltet

        @param key Key in the environment dictionary.
        @param func Function that returns the wanted value
        @returns The value from cache.
        """
        try:
            return self._cache[key]
        except KeyError:
            self._cache[key] = func()
            return self._cache[key]

    def __repr__(self) -> str:
        return str(self._cache)
