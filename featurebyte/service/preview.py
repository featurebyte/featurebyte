"""
PreviewService class
"""
from __future__ import annotations

from typing import Any, AsyncGenerator, Optional, Tuple

import os

import pandas as pd

from featurebyte.common.utils import dataframe_to_json
from featurebyte.exception import LimitExceededError
from featurebyte.logging import get_logger
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.sql.common import sql_to_string
from featurebyte.query_graph.sql.interpreter import GraphInterpreter
from featurebyte.query_graph.sql.materialisation import get_source_count_expr, get_source_expr
from featurebyte.schema.feature_store import (
    FeatureStorePreview,
    FeatureStoreSample,
    FeatureStoreShape,
)
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.session_manager import SessionManagerService
from featurebyte.session.base import BaseSession

MAX_TABLE_CELLS = int(
    os.environ.get("MAX_TABLE_CELLS", 10000000 * 300)
)  # 10 million rows, 300 columns


logger = get_logger(__name__)


class PreviewService:
    """
    PreviewService class
    """

    def __init__(
        self,
        session_manager_service: SessionManagerService,
        feature_store_service: FeatureStoreService,
    ):
        self.feature_store_service = feature_store_service
        self.session_manager_service = session_manager_service

    async def _get_feature_store_session(
        self, graph: QueryGraph, node_name: str, feature_store_name: str, get_credential: Any
    ) -> Tuple[FeatureStoreModel, BaseSession]:
        """
        Get feature store and session from a graph

        Parameters
        ----------
        graph: QueryGraph
            Query graph to use
        node_name: str
            Name of node to use
        feature_store_name: str
            Name of feature store
        get_credential: Any
            Get credential handler function

        Returns
        -------
        Tuple[FeatureStoreModel, BaseSession]
        """
        feature_store_dict = graph.get_input_node(node_name).parameters.feature_store_details.dict()
        feature_store = FeatureStoreModel(**feature_store_dict, name=feature_store_name)
        session = await self.session_manager_service.get_feature_store_session(
            feature_store=feature_store,
            get_credential=get_credential,
        )
        return feature_store, session

    async def shape(self, preview: FeatureStorePreview, get_credential: Any) -> FeatureStoreShape:
        """
        Get the shape of a QueryObject that is not a Feature (e.g. SourceTable, EventTable, EventView, etc)

        Parameters
        ----------
        preview: FeatureStorePreview
            FeatureStorePreview object
        get_credential: Any
            Get credential handler function

        Returns
        -------
        FeatureStoreShape
            Row and column counts
        """
        feature_store, session = await self._get_feature_store_session(
            graph=preview.graph,
            node_name=preview.node_name,
            feature_store_name=preview.feature_store_name,
            get_credential=get_credential,
        )
        shape_sql, num_cols = GraphInterpreter(
            preview.graph, source_type=feature_store.type
        ).construct_shape_sql(node_name=preview.node_name)
        logger.debug("Execute shape SQL", extra={"shape_sql": shape_sql})
        result = await session.execute_query(shape_sql)
        assert result is not None
        return FeatureStoreShape(
            num_rows=result["count"].iloc[0],
            num_cols=num_cols,
        )

    async def preview(
        self, preview: FeatureStorePreview, limit: int, get_credential: Any
    ) -> dict[str, Any]:
        """
        Preview a QueryObject that is not a Feature (e.g. SourceTable, EventTable, EventView, etc)

        Parameters
        ----------
        preview: FeatureStorePreview
            FeatureStorePreview object
        limit: int
            Row limit on preview results
        get_credential: Any
            Get credential handler function

        Returns
        -------
        dict[str, Any]
            Dataframe converted to json string
        """
        feature_store, session = await self._get_feature_store_session(
            graph=preview.graph,
            node_name=preview.node_name,
            feature_store_name=preview.feature_store_name,
            get_credential=get_credential,
        )
        preview_sql, type_conversions = GraphInterpreter(
            preview.graph, source_type=feature_store.type
        ).construct_preview_sql(node_name=preview.node_name, num_rows=limit)
        result = await session.execute_query(preview_sql)
        return dataframe_to_json(result, type_conversions)

    async def sample(
        self, sample: FeatureStoreSample, size: int, seed: int, get_credential: Any
    ) -> dict[str, Any]:
        """
        Sample a QueryObject that is not a Feature (e.g. SourceTable, EventTable, EventView, etc)

        Parameters
        ----------
        sample: FeatureStoreSample
            FeatureStoreSample object
        size: int
            Maximum rows to sample
        seed: int
            Random seed to use for sampling
        get_credential: Any
            Get credential handler function

        Returns
        -------
        dict[str, Any]
            Dataframe converted to json string
        """
        feature_store, session = await self._get_feature_store_session(
            graph=sample.graph,
            node_name=sample.node_name,
            feature_store_name=sample.feature_store_name,
            get_credential=get_credential,
        )
        sample_sql, type_conversions = GraphInterpreter(
            sample.graph, source_type=feature_store.type
        ).construct_sample_sql(
            node_name=sample.node_name,
            num_rows=size,
            seed=seed,
            from_timestamp=sample.from_timestamp,
            to_timestamp=sample.to_timestamp,
            timestamp_column=sample.timestamp_column,
        )
        result = await session.execute_query(sample_sql)
        return dataframe_to_json(result, type_conversions)

    async def describe(
        self, sample: FeatureStoreSample, size: int, seed: int, get_credential: Any
    ) -> dict[str, Any]:
        """
        Sample a QueryObject that is not a Feature (e.g. SourceTable, EventTable, EventView, etc)

        Parameters
        ----------
        sample: FeatureStoreSample
            FeatureStoreSample object
        size: int
            Maximum rows to sample
        seed: int
            Random seed to use for sampling
        get_credential: Any
            Get credential handler function

        Returns
        -------
        dict[str, Any]
            Dataframe converted to json string
        """
        feature_store, session = await self._get_feature_store_session(
            graph=sample.graph,
            node_name=sample.node_name,
            feature_store_name=sample.feature_store_name,
            get_credential=get_credential,
        )

        describe_sql, type_conversions, row_names, columns = GraphInterpreter(
            sample.graph, source_type=feature_store.type
        ).construct_describe_sql(
            node_name=sample.node_name,
            num_rows=size,
            seed=seed,
            from_timestamp=sample.from_timestamp,
            to_timestamp=sample.to_timestamp,
            timestamp_column=sample.timestamp_column,
        )
        logger.debug("Execute describe SQL", extra={"describe_sql": describe_sql})
        result = await session.execute_query(describe_sql)
        assert result is not None
        results = pd.DataFrame(
            result.values.reshape(len(columns), -1).T,
            index=row_names,
            columns=[str(column.name) for column in columns],
        ).dropna(axis=0, how="all")
        return dataframe_to_json(results, type_conversions, skip_prepare=True)

    async def download_table(
        self,
        location: TabularSource,
        get_credential: Any,
    ) -> Optional[AsyncGenerator[bytes, None]]:
        """
        Download table from location.

        Parameters
        ----------
        location: TabularSource
            Location to download from
        get_credential: Any
            Get credential handler function

        Returns
        -------
        AsyncGenerator[bytes, None]
            Asynchronous bytes generator

        Raises
        ------
        LimitExceededError
            Table size exceeds the limit.
        """
        feature_store = await self.feature_store_service.get_document(
            document_id=location.feature_store_id
        )
        db_session = await self.session_manager_service.get_feature_store_session(
            feature_store=feature_store,
            get_credential=get_credential,
        )

        # check size of the table
        sql_expr = get_source_count_expr(source=location.table_details)
        sql = sql_to_string(
            sql_expr,
            source_type=db_session.source_type,
        )
        result = await db_session.execute_query(sql)
        assert result is not None
        columns = await db_session.list_table_schema(**location.table_details.json_dict())
        shape = (result["row_count"].iloc[0], len(columns))

        logger.debug(
            "Downloading table from feature store",
            extra={
                "location": location.json_dict(),
                "shape": shape,
            },
        )

        if shape[0] * shape[0] > MAX_TABLE_CELLS:
            raise LimitExceededError(f"Table size {shape} exceeds download limit.")

        sql_expr = get_source_expr(source=location.table_details)
        sql = sql_to_string(
            sql_expr,
            source_type=db_session.source_type,
        )
        return db_session.get_async_query_stream(sql)
