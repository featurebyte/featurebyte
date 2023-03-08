"""
DataColumn class
"""
from __future__ import annotations

from typing import Any, ClassVar, List, Optional, Type, TypeVar, cast

from http import HTTPStatus

from bson.objectid import ObjectId
from pandas import DataFrame
from pydantic import Field
from typeguard import typechecked

from featurebyte.api.api_object import ApiObject, ForeignKeyMapping, SavableApiObject
from featurebyte.api.database_table import AbstractTableData, DatabaseTable
from featurebyte.api.entity import Entity
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.config import Configurations
from featurebyte.core.mixin import GetAttrMixin, ParentMixin, SampleMixin
from featurebyte.exception import DuplicatedRecordException, RecordRetrievalException
from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.models.feature_store import DataStatus, FeatureStoreModel
from featurebyte.models.tabular_data import TabularDataModel
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import GlobalQueryGraph
from featurebyte.query_graph.model.column_info import ColumnInfo
from featurebyte.query_graph.model.common_table import BaseTableData
from featurebyte.query_graph.model.critical_data_info import CriticalDataInfo
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.cleaning_operation import CleaningOperation
from featurebyte.query_graph.sql.interpreter import GraphInterpreter

DataApiObjectT = TypeVar("DataApiObjectT", bound="DataApiObject")


class DataColumn(FeatureByteBaseModel, ParentMixin, SampleMixin):
    """
    DataColumn class that is used to set metadata such as Entity column. It holds a reference to its
    parent, which is a data object (e.g. EventData)
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc(section=["Column"])

    # pydantic instance variable (public)
    name: str

    @property
    def info(self) -> ColumnInfo:
        """
        Column information which contains column name, column type, associated entity ID & associated
        semantic ID.

        Returns
        -------
        ColumnInfo
        """
        column_info = next(col for col in self.parent.columns_info if col.name == self.name)
        return cast(ColumnInfo, column_info)

    @property
    def feature_store(self) -> FeatureStoreModel:
        """
        Feature store used by parent frame

        Returns
        -------
        FeatureStoreModel
        """
        return cast(FeatureStoreModel, self.parent.feature_store)

    def _prepare_columns_info(self, column_info: ColumnInfo) -> List[ColumnInfo]:
        """
        Prepare columns info attribute of the update payload. The output of this function is used as part of the
        data update route payload.

        Parameters
        ----------
        column_info: ColumnInfo
            Column info is used to replace the item of `columns_info` of this object.

        Returns
        -------
        List[ColumnInfo]
        """
        columns_info = []
        for col in self.parent.columns_info:
            if col.name == column_info.name:
                columns_info.append(column_info)
            else:
                columns_info.append(col)
        return columns_info

    @typechecked
    def as_entity(self, entity_name: Optional[str]) -> None:
        """
        Set the column as the specified entity

        Parameters
        ----------
        entity_name: Optional[str]
            Associate column name to the entity, remove association if entity name is None
        """
        if entity_name is None:
            entity_id = None
        else:
            entity = Entity.get(entity_name)
            entity_id = entity.id

        column_info = ColumnInfo(**{**self.info.dict(), "entity_id": entity_id})
        self.parent.update(
            update_payload={"columns_info": self._prepare_columns_info(column_info)},
            allow_update_local=True,
            add_internal_prefix=True,
        )

    @typechecked
    def update_critical_data_info(self, cleaning_operations: List[CleaningOperation]) -> None:
        """
        Update critical data info of the data column

        Parameters
        ----------
        cleaning_operations: List[CleaningOperation]
            List of cleaning operations to be applied on the column

        Examples
        --------

        Add missing value imputation & negative value imputation operations to a data column

        >>> import featurebyte as fb
        >>> event_data = fb.EventData.get("Credit Card Transactions")  # doctest: +SKIP
        >>> event_data["AMOUNT"].update_critical_data_info(  # doctest: +SKIP
        ...    cleaning_operations=[
        ...        fb.MissingValueImputation(imputed_value=0),
        ...        fb.ValueBeyondEndpointImputation(
        ...            type="less_than", end_point=0, imputed_value=0
        ...        ),
        ...    ]
        ... )

        Check the column info to confirm that critical data info is updated

        >>> event_data["AMOUNT"].info.dict()  # doctest: +SKIP
        {'critical_data_info': {'cleaning_operations': [{'imputed_value': 0,
                                                 'type': 'missing'},
                                                {'end_point': 0,
                                                 'imputed_value': 0,
                                                 'type': 'less_than'}]},
         'dtype': 'FLOAT',
         'entity_id': None,
         'name': 'Discount',
         'semantic_id': None}

        """
        critical_data_info = CriticalDataInfo(cleaning_operations=cleaning_operations)
        column_info = ColumnInfo(**{**self.info.dict(), "critical_data_info": critical_data_info})
        self.parent.update(
            update_payload={"columns_info": self._prepare_columns_info(column_info)},
            allow_update_local=True,
            add_internal_prefix=True,
        )

    def extract_pruned_graph_and_node(self) -> tuple[QueryGraphModel, Node]:
        """
        Extract pruned graph & node from the global query graph

        Returns
        -------
        tuple[QueryGraphModel, Node]
            QueryGraph & mapped Node object (within the pruned graph)
        """
        target_node = self.parent.frame.node
        pruned_graph, node_name_map = self.parent.frame.graph.prune(
            target_node=target_node, aggressive=True
        )
        mapped_node = pruned_graph.get_node_by_name(node_name_map[target_node.name])
        project_node = pruned_graph.add_operation(
            node_type=NodeType.PROJECT,
            node_params={"columns": [self.info.name]},
            node_output_type=NodeOutputType.SERIES,
            input_nodes=[mapped_node],
        )
        return pruned_graph, project_node

    @typechecked
    def preview_sql(self, limit: int = 10) -> str:
        """
        Generate SQL query to preview the transformation output

        Parameters
        ----------
        limit: int
            maximum number of return rows

        Returns
        -------
        str
        """
        pruned_graph, mapped_node = self.extract_pruned_graph_and_node()
        return GraphInterpreter(
            pruned_graph, source_type=self.feature_store.type
        ).construct_preview_sql(node_name=mapped_node.name, num_rows=limit)[0]


class DataListMixin(ApiObject):
    """
    Mixin to implement data source list function
    """

    _route = "/tabular_data"
    _list_schema = TabularDataModel
    _list_fields = ["name", "type", "status", "entities", "created_at"]
    _list_foreign_keys = [
        ForeignKeyMapping("columns_info.entity_id", Entity, "entities"),
    ]

    @classmethod
    def list(cls, include_id: Optional[bool] = False, entity: Optional[str] = None) -> DataFrame:
        """
        List saved data sources

        Parameters
        ----------
        include_id: Optional[bool]
            Whether to include id in the list
        entity: Optional[str]
            Name of entity used to filter results

        Returns
        -------
        DataFrame
            Table of data sources
        """
        data_list = super().list(include_id=include_id)
        if entity:
            data_list = data_list[data_list.entities.apply(lambda entities: entity in entities)]
        return data_list


class DataApiObject(AbstractTableData, SavableApiObject, DataListMixin, GetAttrMixin):
    """
    Base class for all Data objects
    """

    _create_schema_class: ClassVar[Optional[Type[FeatureByteBaseModel]]] = None

    # pydantic instance variable (internal use)
    internal_record_creation_date_column: Optional[str] = Field(alias="record_creation_date_column")

    @property
    def table_data(self) -> BaseTableData:
        try:
            return self._table_data_class(**self.cached_model.json_dict())
        except RecordRetrievalException:
            return self._table_data_class(**self.json_dict())

    @property
    def columns_info(self) -> List[ColumnInfo]:
        try:
            return self.cached_model.columns_info  # pylint: disable=no-member
        except RecordRetrievalException:
            return self.internal_columns_info

    @property
    def status(self) -> DataStatus:
        """
        Data status

        Returns
        -------
        DataStatus
        """
        try:
            return self.cached_model.status  # pylint: disable=no-member
        except RecordRetrievalException:
            return DataStatus.DRAFT

    @property
    def record_creation_date_column(self) -> Optional[str]:
        """
        Record creation date column name

        Returns
        -------
        Optional[str]
        """
        try:
            return self.cached_model.record_creation_date_column  # pylint: disable=no-member
        except RecordRetrievalException:
            return self.internal_record_creation_date_column

    def _get_create_payload(self) -> dict[str, Any]:
        assert self._create_schema_class is not None
        data = self._create_schema_class(**self.json_dict())  # pylint: disable=not-callable
        return data.json_dict()

    @classmethod
    @typechecked
    def create(
        cls: Type[DataApiObjectT],
        tabular_source: DatabaseTable,
        name: str,
        record_creation_date_column: Optional[str] = None,
        _id: Optional[ObjectId] = None,
        **kwargs: Any,
    ) -> DataApiObjectT:
        """
        Create derived instances of DataApiObject from tabular source

        Parameters
        ----------
        tabular_source: DatabaseTable
            DatabaseTable object constructed from FeatureStore
        name: str
            Object name
        record_creation_date_column: str
            Record creation datetime column from the given tabular source
        _id: Optional[ObjectId]
            Identity value for constructed object
        **kwargs: Any
            Additional parameters specific to variants of DataApiObject

        Returns
        -------
        DataApiObjectT

        Raises
        ------
        DuplicatedRecordException
            When record with the same key exists at the persistent layer
        RecordRetrievalException
            When unexpected retrieval failure
        """
        assert cls._create_schema_class is not None

        # construct an input node & insert into the global graph
        data_id_value = _id or ObjectId()
        graph, inserted_node = cls.construct_graph_and_node(
            feature_store_details=tabular_source.feature_store.get_feature_store_details(),
            table_data_dict={
                "tabular_source": tabular_source.tabular_source,
                "columns_info": tabular_source.columns_info,
                "_id": data_id_value,
                **kwargs,
            },
            graph=GlobalQueryGraph(),
        )

        data = cls._create_schema_class(  # pylint: disable=not-callable
            _id=data_id_value,
            name=name,
            tabular_source=tabular_source.tabular_source,
            columns_info=tabular_source.columns_info,
            record_creation_date_column=record_creation_date_column,
            graph=graph,
            node_name=inserted_node.name,
            **kwargs,
        )

        client = Configurations().get_client()
        response = client.get(url=cls._route, params={"name": name})
        if response.status_code == HTTPStatus.OK:
            response_dict = response.json()
            if not response_dict["data"]:
                return cls(
                    **data.json_dict(),
                    feature_store=tabular_source.feature_store,
                    _validate_schema=True,
                )
            existing_record = response_dict["data"][0]
            raise DuplicatedRecordException(
                response,
                f'{cls.__name__} ({existing_record["type"]}.name: "{name}") exists in saved record.',
            )
        raise RecordRetrievalException(response)

    def _get_init_params_from_object(self) -> dict[str, Any]:
        return {"feature_store": self.feature_store}

    @typechecked
    def __getitem__(self, item: str) -> DataColumn:
        """
        Retrieve column from the table

        Parameters
        ----------
        item: str
            Column name

        Returns
        -------
        DataColumn

        Raises
        ------
        KeyError
            when accessing non-exist column
        """
        info = None
        for col in self.columns_info:
            if col.name == item:
                info = col
        if info is None:
            raise KeyError(f'Column "{item}" does not exist!')
        output = DataColumn(name=item)
        output.set_parent(self)
        return output

    @typechecked
    def update_record_creation_date_column(self, record_creation_date_column: str) -> None:
        """
        Update record creation date column

        Parameters
        ----------
        record_creation_date_column: str
            Record creation date column used to perform feature job setting analysis
        """
        self.update(
            update_payload={"record_creation_date_column": record_creation_date_column},
            allow_update_local=True,
            add_internal_prefix=True,
        )
