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
    Set,
    Tuple,
    TypeVar,
    Union,
    cast,
    overload,
)
from typing_extensions import Annotated

from bson import json_util
from pydantic import BaseModel, Field, root_validator, validator

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


class BaseDataColumn(BaseFrozenModel):
    """BaseDataColumn class"""

    name: str
    node_names: Set[str]
    filter: bool


class BaseDerivedColumn(BaseFrozenModel):
    """BaseDerivedColumn class"""

    name: Optional[str]
    transforms: List[str]
    node_names: Set[str]
    filter: bool

    @root_validator(pre=True)
    @classmethod
    def _set_filter_flag(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        if "filter" not in values:
            values["filter"] = any(col.filter for col in values["columns"])
        return values


DataColumnT = TypeVar("DataColumnT", bound=BaseDataColumn)
DerivedColumnT = TypeVar("DerivedColumnT", bound=BaseDerivedColumn)


class DerivedColumnCreationMixin(Generic[DataColumnT, DerivedColumnT]):
    """
    DerivedColumnCreationMixin class
    """

    columns: List[DataColumnT]
    transforms: List[str]
    node_names: Set[str]

    def __init__(self, **kwargs: Any):
        super().__init__(**kwargs)

    @staticmethod
    def _insert_column(
        column_map: Dict[str, DataColumnT], column: DataColumnT
    ) -> Dict[str, DataColumnT]:
        if column.name not in column_map:
            column_map[column.name] = column
        elif len(column.node_names) > len(column_map[column.name].node_names):
            column_map[column.name] = column
        return column_map

    @classmethod
    def _flatten_columns(
        cls, columns: Sequence[Union[DataColumnT, DerivedColumnT]]
    ) -> Tuple[Sequence[DataColumnT], List[str], Set[str]]:
        col_map: Dict[str, DataColumnT] = {}
        transforms = []
        node_names = set()
        for column in columns:
            node_names.update(column.node_names)  # type: ignore
            if isinstance(column, cls):
                # traverse to nested derived columns first
                flat_columns, flat_transforms, _ = cls._flatten_columns(column.columns)
                transforms.extend(flat_transforms)

                # when the column's name are the same, keep the one with the most node_names
                for col in flat_columns:
                    col_map = cls._insert_column(col_map, col)

                # added current transform to the list of transforms
                transforms.extend(column.transforms)
            else:
                col_map = cls._insert_column(col_map, column)
        return list(col_map.values()), transforms, node_names

    @classmethod
    def create(
        cls,
        name: Optional[str],
        columns: Sequence[Union[DataColumnT, DerivedColumnT]],
        transform: Optional[str],
        node_name: str,
    ) -> DerivedColumnT:
        """
        Create derived column by flattening the derived columns in the given list of columns

        Parameters
        ----------
        name: Optional[str]
            Output column name
        columns: Sequence[Union[DataColumnT, DerivedColumnT]]
            Input column name
        transform: Optional[str]
            Node transformation
        node_name: str
            Node name

        Returns
        -------
        DerivedColumnT
            Derive column object
        """
        columns, transforms, node_names = cls._flatten_columns(columns)
        node_names.add(node_name)
        if transform:
            transforms.append(transform)
        return cast(
            DerivedColumnT,
            cls(name=name, columns=columns, transforms=transforms, node_names=node_names),
        )


class SourceDataColumn(BaseDataColumn):
    """Source column"""

    tabular_data_id: Optional[PydanticObjectId]
    tabular_data_type: TableDataType
    type: Literal[ViewDataColumnType.SOURCE] = Field(ViewDataColumnType.SOURCE, const=True)
    filter: bool = Field(default=False)

    def __hash__(self) -> int:
        col_dict = self.dict()
        col_dict["node_names"] = sorted(col_dict["node_names"])
        return hash(json_util.dumps(col_dict, sort_keys=True))


class DerivedDataColumn(
    DerivedColumnCreationMixin[SourceDataColumn, "DerivedDataColumn"], BaseDerivedColumn
):
    """Derived column"""

    columns: List[SourceDataColumn]
    type: Literal[ViewDataColumnType.DERIVED] = Field(ViewDataColumnType.DERIVED, const=True)

    def __hash__(self) -> int:
        col_dict = self.dict()
        col_dict["columns"] = sorted(
            [json_util.dumps(col, sort_keys=True) for col in col_dict["columns"]]
        )
        col_dict["node_names"] = sorted(col_dict["node_names"])
        return hash(json_util.dumps(col_dict, sort_keys=True))


ViewDataColumn = Annotated[Union[SourceDataColumn, DerivedDataColumn], Field(discriminator="type")]


class AggregationColumn(BaseDataColumn):
    """Aggregation column"""

    method: AggFunc
    groupby: List[str]
    window: Optional[str]
    category: Optional[str]
    type: Literal[FeatureDataColumnType.AGGREGATION] = Field(
        FeatureDataColumnType.AGGREGATION, const=True
    )
    column: Optional[ViewDataColumn]
    groupby_type: Literal[NodeType.GROUPBY, NodeType.ITEM_GROUPBY]

    def __hash__(self) -> int:
        col_dict = self.dict()
        return hash(json_util.dumps(col_dict, sort_keys=True))


class PostAggregationColumn(
    DerivedColumnCreationMixin[AggregationColumn, "PostAggregationColumn"], BaseDerivedColumn
):
    """Post aggregation column"""

    columns: List[AggregationColumn]
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


class GroupOperationStructure(BaseFrozenModel):
    """GroupOperationStructure class"""

    source_columns: List[SourceDataColumn] = Field(default_factory=list)
    derived_columns: List[DerivedDataColumn] = Field(default_factory=list)
    aggregations: List[AggregationColumn] = Field(default_factory=list)
    post_aggregation: Optional[PostAggregationColumn]

    @property
    def tabular_data_ids(self) -> List[PydanticObjectId]:
        """
        List of tabular data IDs used in the operation

        Returns
        -------
        List[PydanticObjectId]
        """
        data_ids = [col.tabular_data_id for col in self.source_columns if col.tabular_data_id]
        return list(set(data_ids))


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

    @overload
    def _split_column_by_type(
        self, columns: List[Union[SourceDataColumn, DerivedDataColumn]]
    ) -> Tuple[List[SourceDataColumn], List[DerivedDataColumn]]:
        ...

    @overload
    def _split_column_by_type(
        self, columns: List[Union[AggregationColumn, PostAggregationColumn]]
    ) -> Tuple[List[AggregationColumn], List[PostAggregationColumn]]:
        ...

    def _split_column_by_type(
        self,
        columns: Union[
            List[Union[SourceDataColumn, DerivedDataColumn]],
            List[Union[AggregationColumn, PostAggregationColumn]],
        ],
    ) -> Union[
        Tuple[List[SourceDataColumn], List[DerivedDataColumn]],
        Tuple[List[AggregationColumn], List[PostAggregationColumn]],
    ]:
        _ = self
        input_column_map: Dict[Any, None] = {}
        derived_column_map: Dict[Any, None] = {}
        for column in columns:
            if isinstance(column, (DerivedDataColumn, PostAggregationColumn)):
                derived_column_map[column] = None
                for inner_column in column.columns:
                    input_column_map[inner_column] = None
            else:
                input_column_map[column] = None
        return list(input_column_map), list(derived_column_map)

    def to_group_operation_structure(self) -> GroupOperationStructure:
        """
        Convert the OperationStructure format to group structure format

        Returns
        -------
        GroupOperationStructure
        """
        # pylint: disable=unpacking-non-sequence
        source_columns, derived_columns = self._split_column_by_type(columns=self.columns)
        aggregations, post_aggregations = self._split_column_by_type(columns=self.aggregations)
        assert len(post_aggregations) <= 1
        return GroupOperationStructure(
            source_columns=source_columns,
            derived_columns=derived_columns,
            aggregations=aggregations,
            post_aggregation=next(iter(post_aggregations), None),
        )
