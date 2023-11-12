"""Module with extra services for data type info detection like embedding columns or flat (non-nested) dicts"""
from __future__ import annotations

import json
from abc import ABC, abstractmethod

import numpy as np
import pandas as pd

from featurebyte.api.source_table import SourceTable
from featurebyte.common.utils import dataframe_from_json
from featurebyte.enum import ColumnAttribute, DBVarType
from featurebyte.logging import get_logger
from featurebyte.models.feature_store import TableModel
from featurebyte.query_graph.model.column_info import ColumnInfo
from featurebyte.schema.feature_store import FeatureStoreSample
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.preview import PreviewService

logger = get_logger(__name__)

SAMPLE_SIZE = 1000
RANDOM_SEED = 42


class ColumnAttributesDetectionService:
    """Service that detects attributes of the column with specific datatypes.
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

        self.detectors: list[BaseColumnAttributesDetector] = [
            ArrayEmbeddingColumnAttributesDetector(),
            FlatDictColumnAttributesDetector(),
        ]

    async def add_columns_attributes(self, table: TableModel) -> None:
        """Detect and adds columns attributes like embedding, flat dict, etc.

        Parameters
        ----------
        table : TableModel
            Table with columns to run detection for.
        """
        feature_store = await self.feature_store_service.get_document(
            document_id=table.tabular_source.feature_store_id,
        )
        source_table = SourceTable(
            feature_store=feature_store,
            tabular_source=table.tabular_source,
            columns_info=table.columns_info,
        )
        graph, node = source_table.frame.extract_pruned_graph_and_node()
        sample = dataframe_from_json(
            await self.preview_service.sample(
                FeatureStoreSample(
                    graph=graph,
                    node_name=node.name,
                ),
                size=SAMPLE_SIZE,
                seed=RANDOM_SEED,
            )
        )
        for detector in self.detectors:
            await detector.detect(table.columns_info, sample)


class BaseColumnAttributesDetector(ABC):
    """Base column attributes detector interface."""

    @abstractmethod
    async def detect(self, columns_info: list[ColumnInfo], sample: pd.DataFrame) -> None:
        """Given the sample of data, check if column of a concrete type has some specific attributes.
        Updates ColumnInfo.attributes list with new attribute.

        Examples of checks: array if the same dimension, flat dictionary, etc.

        Parameters
        ----------
        columns_info : list[ColumnInfo]
            List of columns to check
        sample : pd.DataFrame
            Data sample
        """


class ArrayEmbeddingColumnAttributesDetector(BaseColumnAttributesDetector):
    """Detect and add attributes to columns which can be considered embeddings:
    - 1 dimensional
    - Same number of dimensions
    - All values numeric and finite
    """

    async def detect(self, columns_info: list[ColumnInfo], sample: pd.DataFrame) -> None:
        for column in columns_info:
            if str(column.dtype) == DBVarType.ARRAY.value:
                series = sample[column.name]
                shapes = series.apply(len)

                if shapes.unique().ravel().shape != (1,):
                    continue

                is_1d = series.apply(lambda x: len(np.array(x).shape) == 1)

                if not is_1d.all():
                    continue

                all_num = series.apply(
                    lambda x: np.all(np.isfinite(pd.to_numeric(x, errors="coerce")))
                )
                if all_num.all():
                    column.attributes.append(ColumnAttribute.EMBEDDING)


class FlatDictColumnAttributesDetector(BaseColumnAttributesDetector):
    """Detect and add attributes to columns which are flat (not nested) dicts."""

    def is_flat_dict(self, data: str) -> bool:
        """Check dict value is flat (non-nested).

        Parameters
        ----------
        data : str
            Input value to check.

        Returns
        -------
        bool
            True if flat, False otherwise.
        """
        value = json.loads(data)
        return isinstance(value, dict) and all(not isinstance(val, dict) for val in value.values())

    async def detect(self, columns_info: list[ColumnInfo], sample: pd.DataFrame) -> None:
        for column in columns_info:
            if str(column.dtype) in [DBVarType.OBJECT.value, DBVarType.STRUCT.value]:
                series = sample[column.name]
                is_flat = series.apply(self.is_flat_dict)
                if is_flat.all():
                    column.attributes.append(ColumnAttribute.FLAT_DICT)
