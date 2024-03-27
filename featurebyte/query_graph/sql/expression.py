"""
Additional expression used to construct SQL query
"""

from typing import Optional

from sqlglot.expressions import Anonymous, Expression, Func


def make_trim_expression(this: Expression, character: Optional[Expression] = None) -> Expression:
    """Helper to create a Trim expression

    Defining a Trim class doesn't work because it conflicts with sqlglot's internal TRIM function
    that doesn't accept a custom character as parameter. Attempting to do so causes the provided
    character to be ignored.

    Parameters
    ----------
    this: Expression
        Expression for the input string to be trimmed
    character: Optional[Expression]
        Optional character to be used for trimming. If not provided, a whitespace will be used.

    Returns
    -------
    Expression
    """
    expressions = [this]
    if character is not None:
        expressions.append(character)
    return Anonymous(this="TRIM", expressions=expressions)


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


class Concat(Func):
    """Concat function"""

    arg_types = {"expressions": False}
    is_var_len_args = True


class CosineSim(Func):
    """Cosine similarity function"""

    _sql_names = ["F_COUNT_DICT_COSINE_SIMILARITY"]
    arg_types = {"this": True, "expression": True}
