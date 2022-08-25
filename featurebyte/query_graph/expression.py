"""
Additional expression used to construct SQL query
"""
from sqlglot.expressions import Func


class Trim(Func):
    """Trim function"""

    arg_types = {"this": True, "character": False}


class LTrim(Func):
    """LTrim function"""

    _sql_names = ["LTRIM"]
    arg_types = {"this": True, "character": False}


class RTrim(Func):
    """RTrim function"""

    _sql_names = ["RTRIM"]
    arg_types = {"this": True, "character": False}


class Replace(Func):
    """Replace function"""

    arg_types = {"this": True, "pattern": True, "replacement": True}


class LPad(Func):
    """LPad function"""

    _sql_names = ["LPAD"]
    arg_types = {"this": True, "length": True, "pad": True}


class RPad(Func):
    """RPad function"""

    _sql_names = ["RPAD"]
    arg_types = {"this": True, "length": True, "pad": True}


class Contains(Func):
    """Contains function"""

    arg_types = {"this": True, "pattern": True}


class Concat(Func):
    """Concat function"""

    arg_types = {"expressions": False}
    is_var_len_args = True


class CosineSim(Func):
    """Cosine similarity function"""

    _sql_names = ["F_COUNT_DICT_COSINE_SIMILARITY"]
    arg_types = {"this": True, "expression": True}
