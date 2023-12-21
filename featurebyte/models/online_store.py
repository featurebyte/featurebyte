"""
This module contains Tile related models
"""
from typing import Any, Dict, List, Literal, Optional, Union, cast
from typing_extensions import Annotated

import pymongo
from pydantic import Field, StrictStr, validator

from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.enum import OnlineStoreType, RedisType, TableDataType
from featurebyte.feature_manager.model import ExtendedFeatureModel
from featurebyte.models.base import (
    FeatureByteBaseDocumentModel,
    FeatureByteBaseModel,
    UniqueConstraintResolutionSignature,
    UniqueValuesConstraint,
)
from featurebyte.models.online_store_compute_query import OnlineStoreComputeQueryModel
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.node.input import InputNode
from featurebyte.query_graph.sql.adapter import get_sql_adapter
from featurebyte.query_graph.sql.online_serving import is_online_store_eligible
from featurebyte.query_graph.sql.online_store_compute_query import (
    get_online_store_precompute_queries,
)


class OnlineFeatureSpec(FeatureByteBaseModel):
    """
    Model for Online Feature Store

    feature: ExtendedFeatureModel
        feature model
    feature_sql: str
        feature sql
    feature_store_table_name: int
        online feature store table name
    tile_ids: List[str]
        derived tile_ids from tile_specs
    entity_column_names: List[str]
        derived entity column names from tile_specs
    """

    feature: ExtendedFeatureModel
    precompute_queries: List[OnlineStoreComputeQueryModel] = []

    @validator("precompute_queries", always=True)
    def _generate_precompute_queries(  # pylint: disable=no-self-argument
        cls,
        val: List[OnlineStoreComputeQueryModel],
        values: Dict[str, Any],
    ) -> List[OnlineStoreComputeQueryModel]:
        if val:
            # Allow direct setting; mainly used in integration tests
            return val
        feature = values["feature"]
        precompute_queries = get_online_store_precompute_queries(
            graph=feature.graph,
            node=feature.node,
            source_type=feature.feature_store_type,
        )
        return precompute_queries

    @property
    def tile_ids(self) -> List[str]:
        """
        Derived tile_ids property from tile_specs

        Returns
        -------
        List[str]
            derived tile_ids
        """
        tile_ids_set = set()
        for tile_spec in self.feature.tile_specs:
            tile_ids_set.add(tile_spec.tile_id)
        return list(tile_ids_set)

    @property
    def aggregation_ids(self) -> List[str]:
        """
        Derive aggregation_ids property from tile_specs

        Returns
        -------
        List[str]
            derived aggregation_ids
        """
        out = set()
        for tile_spec in self.feature.tile_specs:
            out.add(tile_spec.aggregation_id)
        return list(out)

    @property
    def event_table_ids(self) -> List[str]:
        """
        derived event_table_ids from graph

        Returns
        -------
            derived event_table_ids
        """
        output = []
        for input_node in self.feature.graph.iterate_nodes(
            target_node=self.feature.node, node_type=NodeType.INPUT
        ):
            input_node2 = cast(InputNode, input_node)
            if input_node2.parameters.type == TableDataType.EVENT_TABLE:
                e_id = input_node2.parameters.id
                if e_id:
                    output.append(str(e_id))

        return output

    @property
    def is_online_store_eligible(self) -> bool:
        """
        Whether pre-computation with online store is eligible for the feature

        Returns
        -------
        bool
        """
        return is_online_store_eligible(graph=self.feature.graph, node=self.feature.node)

    @property
    def value_type(self) -> str:
        """
        Feature value's table type (e.g. VARCHAR)

        Returns
        -------
        str
        """
        adapter = get_sql_adapter(self.feature.feature_store_type)
        return adapter.get_physical_type_from_dtype(self.feature.dtype)


class RedisOnlineStoreDetails(FeatureByteBaseModel):
    """
    Configuration details for Redis online store.

    Examples
    --------
    >>> details = fb.RedisOnlineStoreDetails(
    ...   redis_type="redis",
    ...   connection_string="localhost:6379",
    ...   key_ttl_seconds=3600
    ... )
    """

    __fbautodoc__ = FBAutoDoc(proxy_class="featurebyte.RedisOnlineStoreDetails")

    # Online store type selector
    type: Literal[OnlineStoreType.REDIS] = OnlineStoreType.REDIS

    redis_type: RedisType = Field(
        default=RedisType.REDIS, description="Redis type: redis or redis_cluster."
    )
    #  format: host:port,parameter1,parameter2 eg. redis:6379,db=0
    connection_string: StrictStr = Field(
        default="localhost:6379",
        description="Connection string with format 'host:port,parameter1,parameter2' eg. redis:6379,db=0",
    )

    key_ttl_seconds: Optional[int] = Field(
        default=None, description="Redis key bin ttl (in seconds) for expiring entities."
    )


class MySQLOnlineStoreDetails(FeatureByteBaseModel):
    """
    Configuration details for MySQL online store.

    Examples
    --------
    >>> details = fb.MySQLOnlineStoreDetails(
    ...   host="localhost",
    ...   user="user",
    ...   password="password",
    ...   database="database",
    ...   port=3306
    ... )
    """

    __fbautodoc__ = FBAutoDoc(proxy_class="featurebyte.MySQLOnlineStoreDetails")

    # Online store type selector
    type: Literal[OnlineStoreType.MYSQL] = OnlineStoreType.MYSQL

    host: StrictStr = Field(default="localhost", description="MySQL connection host.")

    user: StrictStr = Field(description="MySQL connection user.")

    password: StrictStr = Field(description="MySQL connection password.")

    database: StrictStr = Field(description="MySQL connection database.")

    port: int = Field(default=3306, description="MySQL connection port.")


OnlineStoreDetails = Annotated[
    Union[RedisOnlineStoreDetails, MySQLOnlineStoreDetails],
    Field(discriminator="type"),
]


class OnlineStoreModel(FeatureByteBaseDocumentModel):
    """
    Model for Online Store
    """

    details: OnlineStoreDetails

    class Settings(FeatureByteBaseDocumentModel.Settings):
        """
        MongoDB settings
        """

        collection_name: str = "online_store"
        unique_constraints: List[UniqueValuesConstraint] = [
            UniqueValuesConstraint(
                fields=("_id",),
                conflict_fields_signature={"id": ["_id"]},
                resolution_signature=UniqueConstraintResolutionSignature.GET_NAME,
            ),
            UniqueValuesConstraint(
                fields=("name",),
                conflict_fields_signature={"name": ["name"]},
                resolution_signature=UniqueConstraintResolutionSignature.GET_NAME,
            ),
            UniqueValuesConstraint(
                fields=("details",),
                conflict_fields_signature={"details": ["details"]},
                resolution_signature=UniqueConstraintResolutionSignature.GET_NAME,
            ),
        ]

        indexes = FeatureByteBaseDocumentModel.Settings.indexes + [
            pymongo.operations.IndexModel("details.type"),
            pymongo.operations.IndexModel("details"),
            [
                ("name", pymongo.TEXT),
                ("details.type", pymongo.TEXT),
            ],
        ]
