"""
This module contains models used to store node output operation info
"""
from typing import (
    Any,
    DefaultDict,
    Dict,
    Iterator,
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
from typing_extensions import Annotated  # pylint: disable=wrong-import-order

from collections import defaultdict

from bson import json_util
from pydantic import Field, root_validator, validator

from featurebyte.enum import AggFunc, DBVarType, StrEnum, TableDataType
from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId
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


BaseColumnT = TypeVar("BaseColumnT", bound="BaseColumn")
BaseDerivedColumnT = TypeVar("BaseDerivedColumnT", bound="BaseDerivedColumn")


class BaseColumn(FeatureByteBaseModel):
    """
    BaseColumn class

    name: Optional[str]
        Column name
    dtype: DBVarType
        Column table type
    filter: bool
        Whether the column has been filtered
    node_names: Set[str]
        Set of node names that contributes to the results of the column
    node_name: str
        Node name that represents the output of this column
    """

    name: Optional[str]
    dtype: DBVarType
    filter: bool
    node_names: Set[str]
    node_name: str

    def clone(self: BaseColumnT, **kwargs: Any) -> BaseColumnT:
        """
        Clone an existing object by overriding certain attribute(s)

        Parameters
        ----------
        kwargs: Any
            Keyword parameters to overwrite existing object

        Returns
        -------
        Self
        """
        return type(self)(**{**self.dict(), **kwargs})

    def clone_without_internal_nodes(
        self: BaseColumnT,
        proxy_node_name_map: Dict[str, "OperationStructure"],
        graph_node_name: str,
        graph_node_transform: str,
        **kwargs: Any,
    ) -> BaseColumnT:
        """
        Clone an existing object by removing internal node names. This method is mainly used in
        the nested graph pruning to remove those node names appear only in the nested graph. For example,
        we have the following graph (each square bracket pair represents a node):

            [input] -> [[proxy_input] -> [project] -> [add]] -> [multiply]

        Here, `[[proxy_input] -> [project] -> [add]]` is a graph node and the `[proxy_input]` node is a
        reference node that points to `input` node. `[proxy_input]`, `[project]` and `[add]` nodes are
        all internal nodes to the nested graph. After calling this method, the output of this column
        should not contain any internal node names.

        Parameters
        ----------
        proxy_node_name_map: Dict[str, OperationStructure]
            Dictionary of node name to its corresponding operation structure
        graph_node_name: str
            Node name to replace if any internal node is used
        graph_node_transform: str
            Node transform string of node_name
        kwargs: Any
            Other keywords parameters

        Returns
        -------
        BaseColumnT
        """
        # find match proxy input node names & replace them with the external node names
        proxy_input_names = self.node_names.intersection(proxy_node_name_map)
        node_names = set()
        for name in proxy_input_names:
            node_names.update(proxy_node_name_map[name].all_node_names)

        node_kwargs: Dict[str, Any] = {"node_names": node_names}
        if self.node_name in proxy_node_name_map:
            # update node_name if it points to proxy input node
            # as proxy input node name will be removed from node_names
            op_struct = proxy_node_name_map[self.node_name]
            column = next(col for col in op_struct.columns if col.name == self.name)
            node_kwargs["node_name"] = column.node_name

        # if any of the node name are not from the proxy input names, that means the nested graph's node
        # must change the column in some way, we must include the node name
        if self.node_names.difference(proxy_node_name_map):
            node_kwargs["node_names"].add(graph_node_name)
            node_kwargs["node_name"] = graph_node_name
            if hasattr(self, "transforms"):
                node_kwargs["transforms"] = [graph_node_transform] if graph_node_transform else []
        return self.clone(**node_kwargs, **kwargs)


class BaseDataColumn(BaseColumn):
    """BaseDataColumn class"""

    name: str


class BaseDerivedColumn(BaseColumn):
    """BaseDerivedColumn class"""

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
                node_name=cur_col.node_name
                if len(cur_col.node_names) > len(column.node_names)
                else column.node_name,
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

        # remove empty transforms
        transforms = [transform for transform in transforms if transform]
        return list(col_map.values()), transforms, node_names

    @classmethod
    def create(
        cls: Type[BaseDerivedColumnT],
        name: Optional[str],
        columns: Sequence[Union[BaseDataColumn, "BaseDerivedColumn"]],
        transform: Optional[str],
        node_name: str,
        dtype: DBVarType,
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
        dtype: DBVarType
            Column type
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
        return cls(
            name=name,
            dtype=dtype,
            columns=columns,
            transforms=transforms,
            node_names=node_names,
            node_name=node_name,
        )

    def clone_without_internal_nodes(
        self,
        proxy_node_name_map: Dict[str, "OperationStructure"],
        graph_node_name: str,
        graph_node_transform: str,
        **kwargs: Any,
    ) -> "BaseDerivedColumn":
        columns = [
            col.clone_without_internal_nodes(
                proxy_node_name_map, graph_node_name, graph_node_transform, **kwargs
            )
            for col in self.columns
        ]
        return super().clone_without_internal_nodes(
            proxy_node_name_map=proxy_node_name_map,
            graph_node_name=graph_node_name,
            graph_node_transform=graph_node_transform,
            columns=columns,
            **kwargs,
        )


class SourceDataColumn(BaseDataColumn):
    """Source column"""

    table_id: Optional[PydanticObjectId]
    table_type: TableDataType
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

    method: Optional[AggFunc]
    keys: List[str]
    window: Optional[str]
    category: Optional[str]
    type: Literal[FeatureDataColumnType.AGGREGATION] = Field(
        FeatureDataColumnType.AGGREGATION, const=True
    )
    column: Optional[ViewDataColumn]
    aggregation_type: Literal[
        NodeType.GROUPBY,
        NodeType.ITEM_GROUPBY,
        NodeType.LOOKUP,
        NodeType.AGGREGATE_AS_AT,
        NodeType.REQUEST_COLUMN,
    ]

    def __hash__(self) -> int:
        col_dict = self.dict()
        col_dict["node_names"] = sorted(col_dict["node_names"])
        return hash(json_util.dumps(col_dict, sort_keys=True))

    def clone_without_internal_nodes(
        self,
        proxy_node_name_map: Dict[str, "OperationStructure"],
        graph_node_name: str,
        graph_node_transform: str,
        **kwargs: Any,
    ) -> "AggregationColumn":
        column: Optional[ViewDataColumn] = None
        if self.column is not None:
            column = self.column.clone_without_internal_nodes(  # type: ignore[assignment]
                proxy_node_name_map=proxy_node_name_map,
                graph_node_name=graph_node_name,
                graph_node_transform=graph_node_transform,
                **kwargs,
            )
        return super().clone_without_internal_nodes(
            proxy_node_name_map=proxy_node_name_map,
            graph_node_name=graph_node_name,
            graph_node_transform=graph_node_transform,
            column=column,
            **kwargs,
        )


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


class GroupOperationStructure(FeatureByteBaseModel):
    """GroupOperationStructure class"""

    source_columns: List[SourceDataColumn] = Field(default_factory=list)
    derived_columns: List[DerivedDataColumn] = Field(default_factory=list)
    aggregations: List[AggregationColumn] = Field(default_factory=list)
    post_aggregation: Optional[PostAggregationColumn]
    row_index_lineage: Tuple[str, ...]
    is_time_based: bool = Field(default=False)

    @property
    def table_ids(self) -> List[PydanticObjectId]:
        """
        List of table IDs used in the operation

        Returns
        -------
        List[PydanticObjectId]
        """
        table_ids = [col.table_id for col in self.source_columns if col.table_id]
        return list(set(table_ids))


class OperationStructure(FeatureByteBaseModel):
    """NodeOperationStructure class"""

    # When NodeOutputType is:
    # - NodeOutputType.VIEW -> columns represents the output columns
    # - NodeOutputType.FEATURE -> columns represents the input columns
    columns: List[ViewDataColumn] = Field(default_factory=list)
    aggregations: List[FeatureDataColumn] = Field(default_factory=list)
    output_type: NodeOutputType
    output_category: NodeOutputCategory
    row_index_lineage: Tuple[str, ...]
    is_time_based: bool = Field(default=False)

    def __init__(self, **kwargs: Any):
        super().__init__(**kwargs)
        if self.output_category == NodeOutputCategory.VIEW:
            # make sure there are no duplicated column names
            assert len(self.columns) == len(set(col.name for col in self.columns))
        elif self.output_category == NodeOutputCategory.FEATURE:
            assert len(self.aggregations) == len(set(agg.name for agg in self.aggregations))

    @property
    def series_output_dtype(self) -> DBVarType:
        """
        Retrieve the series output variable type

        Returns
        -------
        DBVarType

        Raises
        ------
        TypeError
            If the output of the node is not series
        ValueError
            If the output column number of the node is not one
        """
        if self.output_type != NodeOutputType.SERIES:
            raise TypeError("Output of the node is not series.")

        if self.output_category == NodeOutputCategory.VIEW:
            num_column = len(self.columns)
        else:
            num_column = len(self.aggregations)

        if num_column != 1:
            raise ValueError("Series output should contain one and only one column.")

        if self.output_category == NodeOutputCategory.VIEW:
            return self.columns[0].dtype
        return self.aggregations[0].dtype

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

    @property
    def source_columns(self) -> List[SourceDataColumn]:
        """
        List of source columns used in the operation structure

        Returns
        -------
        List[SourceDataColumn]
        """
        return [col for col in self.columns if isinstance(col, SourceDataColumn)]

    @property
    def output_column_names(self) -> List[str]:
        """
        List of output column names

        Returns
        -------
        List[str]
        """
        if self.output_category == NodeOutputCategory.VIEW:
            return [col.name for col in self.columns if col.name]
        return [agg.name for agg in self.aggregations if agg.name]

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

    def iterate_source_columns_or_aggregations(
        self,
    ) -> Iterator[Union[SourceDataColumn, AggregationColumn]]:
        """
        Iterate source columns or aggregations. For view category, it returns SourceDataColumn. For feature category,
        it returns AggregationColumn.

        Yields
        ------
        Union[SourceDataColumn, AggregationColumn]
            Column/Aggregation that directly contributes to the output
        """
        if self.output_category == NodeOutputCategory.VIEW:
            for column in self.columns:
                if isinstance(column, SourceDataColumn):
                    yield column
                else:
                    assert isinstance(column, DerivedDataColumn)
                    for source_col in column.columns:
                        yield source_col
        else:
            for aggregation in self.aggregations:
                if isinstance(aggregation, AggregationColumn):
                    yield aggregation
                else:
                    assert isinstance(aggregation, PostAggregationColumn)
                    for source_agg in aggregation.columns:
                        yield source_agg

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
            row_index_lineage=self.row_index_lineage,
            is_time_based=self.is_time_based,
        )


class OperationStructureBranchState:
    """OperationStructureBranchState class"""

    def __init__(self, visited_node_types: Optional[Set[NodeType]] = None):
        if visited_node_types is None:
            visited_node_types = set()
        self.visited_node_types = visited_node_types


class OperationStructureInfo:
    """OperationStructureInfo class"""

    def __init__(
        self,
        operation_structure_map: Optional[Dict[str, OperationStructure]] = None,
        edges_map: Optional[DefaultDict[str, Set[str]]] = None,
        proxy_input_operation_structures: Optional[List[OperationStructure]] = None,
        keep_all_source_columns: bool = False,
        **kwargs: Any,
    ):
        _ = kwargs
        if edges_map is None:
            edges_map = defaultdict(set)
        else:
            edges_map = defaultdict(set, edges_map)
        self.operation_structure_map = operation_structure_map or {}
        self.edges_map = edges_map
        self.proxy_input_operation_structures = proxy_input_operation_structures or []
        self.keep_all_source_columns = keep_all_source_columns
