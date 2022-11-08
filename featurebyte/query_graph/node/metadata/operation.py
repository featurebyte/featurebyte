"""
This module contains models used to store node output operation info
"""
# pylint: disable=too-few-public-methods
from typing import (
    Any,
    Dict,
    Generic,
    List,
    Literal,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
    Union,
    cast,
)
from typing_extensions import Annotated

from bson import json_util
from pydantic import BaseModel, Field, validator

from featurebyte.enum import AggFunc, StrEnum, TableDataType
from featurebyte.models.base import PydanticObjectId
from featurebyte.query_graph.enum import NodeOutputType, NodeType


class NodeOutputCategory(StrEnum):
    """NodeOutputCategory enum used to identify node output category"""

    VIEW = "view"
    FEATURE = "feature"


class ViewDataColumnType(StrEnum):
    """ViewColumnType enum"""

    SOURCE = "source"
    DERIVED = "derived"


class FeatureDataColumnType(StrEnum):
    """FeatureColumnType"""

    AGGREGATION = "aggregation"
    POST_AGGREGATION = "post_aggregation"


class BaseFrozenModel(BaseModel):
    """BaseFrozenModel class"""

    class Config:
        """Config class"""

        frozen = True


class NodeTransform(BaseFrozenModel):
    """Node Transform"""

    node_type: NodeType
    parameters: Dict[str, Any]


DataColumnT = TypeVar("DataColumnT")
DerivedColumnT = TypeVar("DerivedColumnT")


class DerivedColumnCreationMixin(Generic[DataColumnT, DerivedColumnT]):
    """
    DerivedColumnCreationMixin class
    """

    columns: List[DataColumnT]
    transforms: List[NodeTransform]

    def __init__(self, **kwargs: Any):
        super().__init__(**kwargs)

    @classmethod
    def _flatten_columns(
        cls, columns: Sequence[Union[DataColumnT, DerivedColumnT]]
    ) -> Tuple[Sequence[DataColumnT], List[NodeTransform]]:
        col_map: Dict[DataColumnT, None] = {}
        transforms = []
        for column in columns:
            if isinstance(column, cls):
                # traverse to nested derived columns first
                flat_columns, flat_transforms = cls._flatten_columns(column.columns)
                transforms.extend(flat_transforms)

                for col in flat_columns:
                    if col not in col_map:
                        col_map[col] = None

                # added current transform to the list of transforms
                transforms.extend(column.transforms)
            else:
                col_map[cast(DataColumnT, column)] = None
        return list(col_map), transforms

    @classmethod
    def create(
        cls,
        name: Optional[str],
        columns: Sequence[Union[DataColumnT, DerivedColumnT]],
        transform: NodeTransform,
    ) -> DerivedColumnT:
        """
        Create derived column by flattening the derived columns in the given list of columns

        Parameters
        ----------
        name: Optional[str]
            Output column name
        columns: Sequence[Union[DataColumnT, DerivedColumnT]]
            Input column name
        transform: NodeTransform
            Node transformation

        Returns
        -------
        DerivedColumnT
            Derive column object
        """
        columns, transforms = cls._flatten_columns(columns)
        transforms.append(transform)
        return cast(DerivedColumnT, cls(name=name, columns=columns, transforms=transforms))


class SourceDataColumn(BaseFrozenModel):
    """Source column"""

    name: str
    tabular_data_id: Optional[PydanticObjectId]
    tabular_data_type: TableDataType
    type: Literal[ViewDataColumnType.SOURCE] = Field(ViewDataColumnType.SOURCE, const=True)


class DerivedDataColumn(
    DerivedColumnCreationMixin[SourceDataColumn, "DerivedDataColumn"], BaseFrozenModel
):
    """Derived column"""

    name: Optional[str]
    columns: List[SourceDataColumn]
    transforms: List[NodeTransform]
    type: Literal[ViewDataColumnType.DERIVED] = Field(ViewDataColumnType.DERIVED, const=True)

    def __hash__(self) -> int:
        col_dict = self.dict()
        col_dict["columns"] = sorted(
            [json_util.dumps(col, sort_keys=True) for col in col_dict["columns"]]
        )
        return hash(json_util.dumps(col_dict, sort_keys=True))


ViewDataColumn = Annotated[Union[SourceDataColumn, DerivedDataColumn], Field(discriminator="type")]


class AggregationColumn(BaseFrozenModel):
    """Aggregation column"""

    name: str
    method: AggFunc
    groupby: List[str]
    window: Optional[str]
    category: Optional[str]
    type: Literal[FeatureDataColumnType.AGGREGATION] = Field(
        FeatureDataColumnType.AGGREGATION, const=True
    )
    columns: List[ViewDataColumn]
    groupby_type: Literal[NodeType.GROUPBY, NodeType.ITEM_GROUPBY]

    def __hash__(self) -> int:
        col_dict = self.dict()
        col_dict["columns"] = sorted(
            [json_util.dumps(col, sort_keys=True) for col in col_dict["columns"]]
        )
        return hash(json_util.dumps(col_dict, sort_keys=True))


class PostAggregationColumn(
    DerivedColumnCreationMixin[AggregationColumn, "PostAggregationColumn"], BaseFrozenModel
):
    """Post aggregation column"""

    name: Optional[str]
    columns: List[AggregationColumn]
    transforms: List[NodeTransform]
    type: Literal[FeatureDataColumnType.POST_AGGREGATION] = Field(
        FeatureDataColumnType.POST_AGGREGATION, const=True
    )

    def __hash__(self) -> int:
        col_dict = self.dict()
        col_dict["columns"] = sorted(
            [json_util.dumps(col, sort_keys=True) for col in col_dict["columns"]]
        )
        return hash(json_util.dumps(col_dict, sort_keys=True))


FeatureDataColumn = Annotated[
    Union[AggregationColumn, PostAggregationColumn], Field(discriminator="type")
]


class OperationStructure(BaseFrozenModel):
    """NodeOperationStructure class"""

    columns: List[ViewDataColumn] = Field(default_factory=list)
    aggregations: List[FeatureDataColumn] = Field(default_factory=list)
    output_type: NodeOutputType
    output_category: NodeOutputCategory

    @validator("columns", "aggregations")
    @classmethod
    def _validator(cls, value: List[Any]) -> List[Any]:
        output: Dict[Any, None] = {}
        for obj in value:
            if obj not in output:
                output[obj] = None
        return list(output)
