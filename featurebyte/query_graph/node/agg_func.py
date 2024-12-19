"""
Aggregation method model
"""

from abc import abstractmethod
from typing import TYPE_CHECKING, Any, ClassVar, Dict, Optional, Union, cast

from pydantic import Field
from typing_extensions import Annotated, Literal

from featurebyte.common.model_util import construct_serialize_function
from featurebyte.enum import AggFunc, DBVarType
from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.query_graph.model.dtype import DBVarTypeInfo

AGG_FUNCS = []


class BaseAggFunc(FeatureByteBaseModel):
    """BaseAggMethod class"""

    type: AggFunc

    @classmethod
    def __pydantic_init_subclass__(cls, **kwargs: Any) -> None:
        if "Literal" in repr(cls.model_fields["type"].annotation):
            # only add agg method class to AGG_FUNCS if the type variable is a literal (to filter out base classes)
            AGG_FUNCS.append(cls)

    def derive_output_dtype_info(
        self, input_dtype_info: DBVarTypeInfo, category: Optional[str] = None
    ) -> DBVarTypeInfo:
        """
        Derive output var type based on input_dtype_info & aggregation method

        Parameters
        ----------
        input_dtype_info: DBVarTypeInfo
            Input variable type
        category : Optional[str]
            Optional category parameter to enable aggregation per category. It should be a column
            name in the View.

        Returns
        -------
        DBVarTypeInfo
        """
        if input_dtype_info.dtype == DBVarType.UNKNOWN:
            return DBVarTypeInfo(dtype=DBVarType.UNKNOWN)
        if category:
            return DBVarTypeInfo(dtype=DBVarType.OBJECT)
        return self._derive_output_dtype_info(input_dtype_info=input_dtype_info, category=category)

    @abstractmethod
    def _derive_output_dtype_info(
        self, input_dtype_info: DBVarTypeInfo, category: Optional[str] = None
    ) -> DBVarTypeInfo: ...

    @abstractmethod
    def is_var_type_supported(self, input_var_type: DBVarType) -> bool:
        """
        Check whether the input var type is supported

        Parameters
        ----------
        input_var_type: DBVarType
            Input variable type

        Returns
        -------
        bool
        """


class SumAggFunc(BaseAggFunc):
    """SumAggFunc class"""

    type: Literal[AggFunc.SUM] = AggFunc.SUM
    _var_type_map: ClassVar[Dict[DBVarType, DBVarType]] = {
        DBVarType.INT: DBVarType.INT,
        DBVarType.FLOAT: DBVarType.FLOAT,
        DBVarType.ARRAY: DBVarType.ARRAY,
        DBVarType.EMBEDDING: DBVarType.EMBEDDING,
    }

    def _derive_output_dtype_info(
        self, input_dtype_info: DBVarTypeInfo, category: Optional[str] = None
    ) -> DBVarTypeInfo:
        return DBVarTypeInfo(dtype=self._var_type_map[input_dtype_info.dtype])

    def is_var_type_supported(self, input_var_type: DBVarType) -> bool:
        return input_var_type in self._var_type_map


class BaseNumAggFunc(BaseAggFunc):
    """BaseNumAggFunc class"""

    _var_type_map: ClassVar[Dict[DBVarType, DBVarType]] = {
        DBVarType.INT: DBVarType.FLOAT,
        DBVarType.FLOAT: DBVarType.FLOAT,
        DBVarType.TIMEDELTA: DBVarType.FLOAT,
        DBVarType.ARRAY: DBVarType.ARRAY,
        DBVarType.EMBEDDING: DBVarType.EMBEDDING,
    }

    def _derive_output_dtype_info(
        self, input_dtype_info: DBVarTypeInfo, category: Optional[str] = None
    ) -> DBVarTypeInfo:
        return DBVarTypeInfo(dtype=self._var_type_map[input_dtype_info.dtype])

    def is_var_type_supported(self, input_var_type: DBVarType) -> bool:
        return input_var_type in self._var_type_map


class AvgAggFunc(BaseNumAggFunc):
    """AvgAggFunc class"""

    type: Literal[AggFunc.AVG] = AggFunc.AVG


class StdAggFunc(BaseNumAggFunc):
    """StdAggFunc class"""

    type: Literal[AggFunc.STD] = AggFunc.STD


class MatchingVarTypeAggFunc(BaseAggFunc):
    """MatchingVarTypeAggFunc class where output type is the same as input type"""

    def _derive_output_dtype_info(
        self, input_dtype_info: DBVarTypeInfo, category: Optional[str] = None
    ) -> DBVarTypeInfo:
        return input_dtype_info

    def is_var_type_supported(self, input_var_type: DBVarType) -> bool:
        return True


class MaxAggFunc(MatchingVarTypeAggFunc):
    """MaxAggFunc class"""

    type: Literal[AggFunc.MAX] = AggFunc.MAX


class MinAggFunc(MatchingVarTypeAggFunc):
    """MinAggFunc class"""

    type: Literal[AggFunc.MIN] = AggFunc.MIN


class BaseCountAggFunc(BaseAggFunc):
    """BaseCountAggFunc class"""

    def _derive_output_dtype_info(
        self, input_dtype_info: DBVarTypeInfo, category: Optional[str] = None
    ) -> DBVarTypeInfo:
        return DBVarTypeInfo(dtype=DBVarType.INT)

    def is_var_type_supported(self, input_var_type: DBVarType) -> bool:
        return True


class CountAggFunc(BaseCountAggFunc):
    """CountAggFunc class"""

    type: Literal[AggFunc.COUNT] = AggFunc.COUNT


class CountDistinctAggFunc(BaseCountAggFunc):
    """CountDistinctAggFunc class"""

    type: Literal[AggFunc.COUNT_DISTINCT] = AggFunc.COUNT_DISTINCT


class NaCountAggFunc(BaseCountAggFunc):
    """NaCountAggFunc class"""

    type: Literal[AggFunc.NA_COUNT] = AggFunc.NA_COUNT


class LatestAggFunc(MatchingVarTypeAggFunc):
    """LatestAggFunc class"""

    type: Literal[AggFunc.LATEST] = AggFunc.LATEST


if TYPE_CHECKING:
    AggFuncType = BaseAggFunc
else:
    AggFuncType = Annotated[Union[tuple(AGG_FUNCS)], Field(discriminator="type")]


def construct_agg_func(agg_func: AggFunc) -> AggFuncType:
    """
    Construct agg method object based on agg_func enum value

    Parameters
    ----------
    agg_func: AggFunc
        Aggregation method

    Returns
    -------
    AggFuncType
    """
    construct_func = construct_serialize_function(
        all_types=AGG_FUNCS,
        annotated_type=AggFuncType,
        discriminator_key="type",
    )
    return cast(AggFuncType, construct_func(type=agg_func))
