"""
This module contains models used to store node output operation info
"""
# pylint: disable=too-few-public-methods
from typing import (
    Any,
    Dict,
    List,
    Literal,
    Optional,
    Sequence,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
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


BaseColumnT = TypeVar("BaseColumnT", bound="BaseColumn")
BaseDerivedColumnT = TypeVar("BaseDerivedColumnT", bound="BaseDerivedColumn")


class BaseColumn(BaseFrozenModel):
    """BaseColumn class"""

    filter: bool
    node_names: Set[str]

    def clone(self: BaseColumnT, **kwargs: Any) -> BaseColumnT:
        """
        Clone an existing object with certain update

        Parameters
        ----------
        kwargs: Any
            Keyword parameters to overwrite existing object

        Returns
        -------
        Self
        """
        return type(self)(**{**self.dict(), **kwargs})


class BaseDataColumn(BaseColumn):
    """BaseDataColumn class"""

    name: str


class BaseDerivedColumn(BaseColumn):
    """BaseDerivedColumn class"""

    name: Optional[str]
    transforms: List[str]
    columns: Sequence[BaseDataColumn]

    @root_validator(pre=True)
    @classmethod
    def _set_filter_flag(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        if "filter" not in values:
            values["filter"] = any(col.filter for col in values["columns"])
        return values

    @staticmethod
    def insert_column(
        column_map: Dict[str, BaseDataColumn], column: BaseDataColumn
    ) -> Dict[str, BaseDataColumn]:
        """
        Insert column into dictionary. If more than more columns with the same name, aggregate the node names
        (make sure we don't miss any node which is used for pruning) and combine filter flag (take the max
        value to indicate the filtered column has been used at least in one operation).

        Parameters
        ----------
        column_map: Dict[str, BaseDataColumn]
            Dictionary map
        column: BaseDataColumn
            Data column object

        Returns
        -------
        Dict[str, BaseDataColumn]
        """
        if column.name not in column_map:
            column_map[column.name] = column
        else:
            cur_col = column_map[column.name]
            column_map[column.name] = column.clone(
                node_names=cur_col.node_names.union(column.node_names),
                filter=cur_col.filter or column.filter,
            )
        return column_map

    @classmethod
    def _flatten_columns(
        cls, columns: Sequence[Union[BaseDataColumn, "BaseDerivedColumn"]]
    ) -> Tuple[Sequence[BaseDataColumn], List[str], Set[str]]:
        col_map: Dict[str, BaseDataColumn] = {}
        transforms: List[str] = []
        node_names: Set[str] = set()
        for column in columns:
            node_names.update(column.node_names)
            if isinstance(column, BaseDerivedColumn):
                # traverse to nested derived columns first
                flat_columns, flat_transforms, _ = cls._flatten_columns(column.columns)
                transforms.extend(flat_transforms)

                # when the column's name are the same, keep the one with the most node_names
                for col in flat_columns:
                    col_map = cls.insert_column(col_map, col)

                # added current transform to the list of transforms
                transforms.extend(column.transforms)
            else:
                col_map = cls.insert_column(col_map, column)
        return list(col_map.values()), transforms, node_names

    @classmethod
    def create(
        cls: Type[BaseDerivedColumnT],
        name: Optional[str],
        columns: Sequence[Union[BaseDataColumn, "BaseDerivedColumn"]],
        transform: Optional[str],
        node_name: str,
        other_node_names: Optional[Set[str]] = None,
    ) -> BaseDerivedColumnT:
        """
        Create derived column by flattening the derived columns in the given list of columns

        Parameters
        ----------
        name: Optional[str]
            Output column name
        columns: Sequence[Union[BaseDataColumn, BaseDerivedColumn]]
            Input column name
        transform: Optional[str]
            Node transformation
        node_name: str
            Node name
        other_node_names: Optional[Set[str]]
            Set of node name

        Returns
        -------
        BaseDerivedColumnT
            Derive column object
        """
        columns, transforms, node_names = cls._flatten_columns(columns)
        node_names.add(node_name)
        if other_node_names:
            node_names.update(other_node_names)
        if transform:
            transforms.append(transform)
        return cls(name=name, columns=columns, transforms=transforms, node_names=node_names)


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


class DerivedDataColumn(BaseDerivedColumn):
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
        col_dict["node_names"] = sorted(col_dict["node_names"])
        return hash(json_util.dumps(col_dict, sort_keys=True))


class PostAggregationColumn(BaseDerivedColumn):
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
        col_dict["node_names"] = sorted(col_dict["node_names"])
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

    @property
    def all_node_names(self) -> Set[str]:
        """
        Retrieve all node names in the operation structure

        Returns
        -------
        Set[str]
        """
        node_names = set()
        for column in self.columns:
            node_names.update(column.node_names)
        for aggregation in self.aggregations:
            node_names.update(aggregation.node_names)
        return node_names

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
        input_column_map: Dict[str, Any] = {}
        derived_column_map: Dict[Any, None] = {}
        for column in columns:
            if isinstance(column, (DerivedDataColumn, PostAggregationColumn)):
                derived_column_map[column] = None
                for inner_column in column.columns:
                    input_column_map = BaseDerivedColumn.insert_column(
                        input_column_map, inner_column
                    )
            else:
                input_column_map = BaseDerivedColumn.insert_column(input_column_map, column)

        return list(input_column_map.values()), list(derived_column_map)

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
