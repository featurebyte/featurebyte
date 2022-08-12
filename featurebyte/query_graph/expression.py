"""
Additional expression used to construct SQL query
"""
from sqlglot.expressions import Func


class Trim(Func):
    arg_types = {"this": True, "character": False}


class LTrim(Func):
    _sql_names = ["LTRIM"]
    arg_types = {"this": True, "character": False}


class RTrim(Func):
    _sql_names = ["RTRIM"]
    arg_types = {"this": True, "character": False}


class Replace(Func):
    arg_types = {"this": True, "pattern": True, "replacement": True}


class LPad(Func):
    _sql_names = ["LPAD"]
    arg_types = {"this": True, "length": True, "pad": True}


class RPad(Func):
    _sql_names = ["RPAD"]
    arg_types = {"this": True, "length": True, "pad": True}


class Contains(Func):
    arg_types = {"this": True, "pattern": True}
