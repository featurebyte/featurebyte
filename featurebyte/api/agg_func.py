"""
Aggregation method model
"""
from typing import TYPE_CHECKING, Any, ClassVar, Dict, Literal, Union
from typing_extensions import Annotated

from pydantic import Field, parse_obj_as

from featurebyte.enum import AggFunc, DBVarType
from featurebyte.models.base import FeatureByteBaseModel

AGG_FUNCS = []


class BaseAggFunc(FeatureByteBaseModel):
    """BaseAggMethod class"""

    type: AggFunc
    input_output_var_type_map: ClassVar[Dict[DBVarType, DBVarType]]

    def __init_subclass__(cls, **kwargs: Any):
        if repr(cls.__fields__["type"].type_).startswith("typing.Literal"):
            # only add agg method class to AGG_FUNCS if the type variable is a literal (to filter out base classes)
            AGG_FUNCS.append(cls)


class BaseNumAggFunc(BaseAggFunc):
    """BaseNumAggFunc class"""

    input_output_var_type_map = {DBVarType.INT: DBVarType.INT, DBVarType.FLOAT: DBVarType.FLOAT}


class SumAggFunc(BaseNumAggFunc):
    """SumAggFunc class"""

    type: Literal[AggFunc.SUM] = Field(AggFunc.SUM, const=True)


class AvgAggFunc(BaseNumAggFunc):
    """AvgAggFunc class"""

    type: Literal[AggFunc.AVG] = Field(AggFunc.AVG, const=True)


class StdAggFunc(BaseNumAggFunc):
    """StdAggFunc class"""

    type: Literal[AggFunc.STD] = Field(AggFunc.STD, const=True)


class BaseMaxMinAggFunc(BaseAggFunc):
    """BaseGeneralAggFunc class"""

    input_output_var_type_map = {
        DBVarType.BOOL: DBVarType.BOOL,
        DBVarType.CHAR: DBVarType.CHAR,
        DBVarType.DATE: DBVarType.DATE,
        DBVarType.FLOAT: DBVarType.FLOAT,
        DBVarType.INT: DBVarType.INT,
        DBVarType.TIME: DBVarType.TIME,
        DBVarType.TIMESTAMP: DBVarType.TIMESTAMP,
        DBVarType.VARCHAR: DBVarType.VARCHAR,
        DBVarType.TIMEDELTA: DBVarType.TIMEDELTA,
    }


class MaxAggFunc(BaseMaxMinAggFunc):
    """MaxAggFunc class"""

    type: Literal[AggFunc.MAX] = Field(AggFunc.MAX, const=True)


class MinAggFunc(BaseMaxMinAggFunc):
    """MinAggFunc class"""

    type: Literal[AggFunc.MIN] = Field(AggFunc.MIN, const=True)


class BaseCountAggFunc(BaseAggFunc):
    """BaseCountAggFunc class"""

    input_output_var_type_map = {var_type: DBVarType.FLOAT for var_type in DBVarType}


class CountAggFunc(BaseCountAggFunc):
    """CountAggFunc class"""

    type: Literal[AggFunc.COUNT] = Field(AggFunc.COUNT, const=True)


class NaCountAggFunc(BaseCountAggFunc):
    """NaCountAggFunc class"""

    type: Literal[AggFunc.NA_COUNT] = Field(AggFunc.NA_COUNT, const=True)


if TYPE_CHECKING:
    AggFuncType = BaseAggFunc
else:
    AggFuncType = Annotated[Union[tuple(AGG_FUNCS)], Field(discriminator="type")]


def construct_agg_func(agg_func: str) -> AggFuncType:
    """
    Construct agg method object based on agg_func enum value

    Parameters
    ----------
    agg_func: str
        Aggregation method

    Returns
    -------
    AggFuncType
    """
    agg_func_obj = parse_obj_as(AggFuncType, {"type": agg_func})
    return agg_func_obj
