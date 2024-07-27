"""Module with extra services for data type info detection like embedding columns or flat (non-nested) dicts"""

from __future__ import annotations

import json
from abc import ABC, abstractmethod
from typing import Any

import numpy as np
import pandas as pd

from featurebyte.common.utils import dataframe_from_json
from featurebyte.enum import DBVarType
from featurebyte.logging import get_logger
from featurebyte.models.feature_store import TableModel
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.model.column_info import ColumnInfo
from featurebyte.schema.feature_store import FeatureStoreSample
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.preview import PreviewService

logger = get_logger(__name__)

SAMPLE_SIZE = 1000
RANDOM_SEED = 42


class SpecializedDtypeDetectionService:
    """Service that detects specialized data types based on the sample of data.
    For example:
        - Checking if ARRAY column has same number of dimensions in all observations (square),
          and might represent embedding of the text.
        - Checking if OBJECT is flat dictionary (no nested dictionaries).
    """

    def __init__(
        self,
        preview_service: PreviewService,
        feature_store_service: FeatureStoreService,
    ):
        self.preview_service = preview_service
        self.feature_store_service = feature_store_service

        self.detectors: list[BaseSpecializedDtypeDetector] = [
            ArrayEmbeddingDtypeDetector(),
            FlatDictDtypeDetector(),
        ]

    async def detect_and_update_column_dtypes(self, table: TableModel) -> None:
        """Detect and update column dtypes for the given table with the specialized dtypes.

        Parameters
        ----------
        table : TableModel
            Table with columns to run detection for.
        """
        feature_store = await self.feature_store_service.get_document(
            document_id=table.tabular_source.feature_store_id,
        )
        graph, node = table.construct_graph_and_node(
            feature_store_details=feature_store.get_feature_store_details(),
            table_data_dict=table.model_dump(by_alias=True),
        )
        columns = [
            col.name
            for col in table.columns_info
            if col.dtype in DBVarType.supported_detection_types()
        ]
        if not columns:
            return

        node = graph.add_operation(
            node_type=NodeType.PROJECT,
            node_params={"columns": columns},
            node_output_type=NodeOutputType.FRAME,
            input_nodes=[node],
        )
        sample = dataframe_from_json(
            await self.preview_service.sample(
                FeatureStoreSample(
                    graph=graph,
                    node_name=node.name,
                    feature_store_id=feature_store.id,
                ),
                size=SAMPLE_SIZE,
                seed=RANDOM_SEED,
            )
        )
        if sample.shape[0] > 0:
            for detector in self.detectors:
                await detector.detect(table.columns_info, sample)


class BaseSpecializedDtypeDetector(ABC):
    """Base specialized dtype detector interface."""

    @abstractmethod
    async def detect(self, columns_info: list[ColumnInfo], sample: pd.DataFrame) -> None:
        """Given the sample of data, check if column of a concrete type has fulfilled certain properties.
        Updates ColumnInfo.dtype with the detected type.

        Examples of checks: array if the same dimension, flat dictionary, etc.

        Parameters
        ----------
        columns_info : list[ColumnInfo]
            List of columns to check
        sample : pd.DataFrame
            Data sample
        """


class ArrayEmbeddingDtypeDetector(BaseSpecializedDtypeDetector):
    """Detect and add attributes to columns which can be considered embeddings:
    - 1 dimensional
    - Same number of dimensions
    - All values numeric and finite
    """

    async def detect(self, columns_info: list[ColumnInfo], sample: pd.DataFrame) -> None:
        for column in columns_info:
            if str(column.dtype) == DBVarType.ARRAY.value:
                series = sample[column.name]
                series = series[pd.notnull(series)]
                shapes = series.apply(len)

                # skip if arrays have different shape (number of dimensions)
                if shapes.unique().ravel().shape != (1,):
                    continue

                # skip if arrays are not 1 dimensional
                is_1d = series.apply(lambda x: len(np.array(x).shape) == 1)
                if not is_1d.all():
                    continue

                # skip if arrays
                all_num = series.apply(
                    lambda x: np.all(np.isfinite(pd.to_numeric(x, errors="coerce")))
                )
                if all_num.all():
                    column.dtype = DBVarType.EMBEDDING


class FlatDictDtypeDetector(BaseSpecializedDtypeDetector):
    """Detect and add attributes to columns which are flat (not nested) dicts."""

    def is_flat_dict(self, data: Any) -> bool:
        """Check dict value is flat (non-nested).

        Parameters
        ----------
        data : Any
            Input value to check.

        Returns
        -------
        bool
            True if flat, False otherwise.
        """
        if isinstance(data, str):
            value = json.loads(data)
        else:
            value = data
        return isinstance(value, dict) and all(not isinstance(val, dict) for val in value.values())

    async def detect(self, columns_info: list[ColumnInfo], sample: pd.DataFrame) -> None:
        for column in columns_info:
            if column.dtype in DBVarType.dictionary_types():
                series = sample[column.name]
                is_flat = series.apply(self.is_flat_dict)
                if is_flat.all():
                    column.dtype = DBVarType.FLAT_DICT
