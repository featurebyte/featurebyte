"""
ObservationTable class
"""
# pylint: disable=duplicate-code
from __future__ import annotations

from typing import List, Literal, Optional, Sequence, Union

import os
from pathlib import Path

import pandas as pd
from bson import ObjectId
from typeguard import typechecked

from featurebyte.api.api_object_util import ForeignKeyMapping
from featurebyte.api.entity import Entity
from featurebyte.api.feature_store import FeatureStore
from featurebyte.api.materialized_table import MaterializedTableMixin
from featurebyte.api.primary_entity_mixin import PrimaryEntityMixin
from featurebyte.api.templates.doc_util import substitute_docstring
from featurebyte.api.templates.entity_doc import (
    ENTITY_DOC,
    PRIMARY_ENTITY_DOC,
    PRIMARY_ENTITY_IDS_DOC,
)
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.observation_table import ObservationInput, ObservationTableModel, Purpose
from featurebyte.schema.observation_table import (
    ObservationTableListRecord,
    ObservationTableUpdate,
    ObservationTableUpload,
)

DOCSTRING_FORMAT_PARAMS = {"class_name": "ObservationTable"}


class ObservationTable(
    PrimaryEntityMixin, MaterializedTableMixin
):  # pylint: disable=too-many-public-methods
    """
    ObservationTable class
    """

    __fbautodoc__ = FBAutoDoc(proxy_class="featurebyte.ObservationTable")

    _route = "/observation_table"
    _list_schema = ObservationTableListRecord
    _get_schema = ObservationTableModel
    _update_schema_class = ObservationTableUpdate
    _list_fields = [
        "name",
        "type",
        "shape",
        "feature_store_name",
        "created_at",
    ]
    _list_foreign_keys = [
        ForeignKeyMapping("feature_store_id", FeatureStore, "feature_store_name"),
    ]

    @property
    def entity_ids(self) -> List[PydanticObjectId]:
        """
        Returns the entity ids of the observation table.

        Returns
        -------
        List[PydanticObjectId]
            List of entity ids.
        """
        entity_ids = []
        for col in self.columns_info:
            if col.entity_id is not None:
                entity_ids.append(col.entity_id)
        return entity_ids

    @property
    @substitute_docstring(
        doc_template=PRIMARY_ENTITY_IDS_DOC, format_kwargs=DOCSTRING_FORMAT_PARAMS
    )
    def primary_entity_ids(
        self,
    ) -> Sequence[ObjectId]:  # pylint: disable=missing-function-docstring
        return self.cached_model.primary_entity_ids

    @property
    @substitute_docstring(doc_template=ENTITY_DOC, format_kwargs=DOCSTRING_FORMAT_PARAMS)
    def entities(self) -> List[Entity]:  # pylint: disable=missing-function-docstring
        return self._get_entities()

    @property
    @substitute_docstring(doc_template=PRIMARY_ENTITY_DOC, format_kwargs=DOCSTRING_FORMAT_PARAMS)
    def primary_entity(self) -> List[Entity]:  # pylint: disable=missing-function-docstring
        return [Entity.get_by_id(entity_id) for entity_id in self.primary_entity_ids]

    @property
    def context_id(self) -> ObjectId:
        """
        Returns the context id of the observation table.

        Returns
        -------
        ObjectId
            Context id of the observation table.
        """
        return self.cached_model.context_id

    @property
    def use_case_ids(self) -> Sequence[ObjectId]:
        """
        Returns the use case ids of the observation table.

        Returns
        -------
        Sequence[ObjectId]
            List of use case ids.
        """
        return self.cached_model.use_case_ids

    @property
    def request_input(self) -> ObservationInput:
        """
        Returns the request input of the observation table.

        Returns
        -------
        ObservationInput
            Request input of the observation table.
        """
        return self.cached_model.request_input

    @property
    def purpose(self) -> Purpose:
        """
        Returns the purpose of the observation table.

        Returns
        -------
        Purpose
            Purpose of the observation table.
        """
        return self.cached_model.purpose

    @property
    def most_recent_point_in_time(self) -> str:
        """
        Returns the most recent point in time of the observation table.

        Returns
        -------
        str
            Most recent point in time of the observation table.
        """
        return self.cached_model.most_recent_point_in_time

    @property
    def least_recent_point_in_time(self) -> Optional[str]:
        """
        Returns the least recent point in time of the observation table.

        Returns
        -------
        Optional[str]
            Least recent point in time of the observation table.
        """
        return self.cached_model.least_recent_point_in_time

    @property
    def min_interval_secs_between_entities(self) -> Optional[float]:
        """
        Returns the minimum interval in seconds between events within entities of the observation table.

        Returns
        -------
        Optional[float]
            Minimum interval in seconds between events within entities of the observation table.
        """
        return self.cached_model.min_interval_secs_between_entities

    @property
    def entity_column_name_to_count(self) -> Optional[dict[str, int]]:
        """
        Returns the entity column name to unique entity count mapping of the observation table.

        Returns
        -------
        Optional[dict]
            Entity column name to unique entity count mapping of the observation table.
        """
        return self.cached_model.entity_column_name_to_count

    def to_pandas(self) -> pd.DataFrame:
        """
        Converts the observation table to a pandas dataframe.

        Returns
        -------
        pd.DataFrame

        Examples
        --------
        >>> observation_table = catalog.get_observation_table("observation_table_name")  # doctest: +SKIP
        >>> observation_table.to_pandas()  # doctest: +SKIP
        """
        return super().to_pandas()

    def preview(self, limit: int = 10) -> pd.DataFrame:
        """
        Returns a DataFrame that contains a selection of rows of the observation table.

        Parameters
        ----------
        limit: int
            Maximum number of return rows.

        Returns
        -------
        pd.DataFrame
            Preview rows of the table.

        Examples
        --------
        >>> observation_table = catalog.get_observation_table("observation_table_name")  # doctest: +SKIP
        >>> observation_table.preview()  # doctest: +SKIP
        """
        return super().preview(limit=limit)

    def sample(self, size: int = 10, seed: int = 1234) -> pd.DataFrame:
        """
        Returns a DataFrame that contains a random selection of rows of the observation table based on a
        specified size and seed for sampling control.

        Parameters
        ----------
        size: int
            Maximum number of rows to sample, with an upper bound of 10,000 rows.
        seed: int
            Seed to use for random sampling.

        Returns
        -------
        pd.DataFrame
            Sampled rows from the table.

        Examples
        --------
        >>> observation_table = catalog.get_observation_table("observation_table_name")  # doctest: +SKIP
        >>> observation_table.sample()  # doctest: +SKIP
        """
        return super().sample(size=size, seed=seed)

    def describe(self, size: int = 0, seed: int = 1234) -> pd.DataFrame:
        """
        Returns descriptive statistics of the observation table.

        Parameters
        ----------
        size: int
            Maximum number of rows to sample. If 0, all rows will be used.
        seed: int
            Seed to use for random sampling.

        Returns
        -------
        pd.DataFrame
            Summary of the table.

        Examples
        --------
        >>> observation_table = catalog.get_observation_table("observation_table_name")  # doctest: +SKIP
        >>> observation_table.describe()  # doctest: +SKIP
        """
        return super().describe(size=size, seed=seed)

    def download(self, output_path: Optional[Union[str, Path]] = None) -> Path:
        """
        Downloads the observation table from the database.

        Parameters
        ----------
        output_path: Optional[Union[str, Path]]
            Location to save downloaded parquet file.

        Returns
        -------
        Path

        Raises
        ------
        FileExistsError
            File already exists at output path.
        RecordRetrievalException
            Error retrieving record from API.

        Examples
        --------
        >>> observation_table = catalog.get_observation_table("observation_table_name")  # doctest: +SKIP
        >>> downloaded_path = observation_table.download(output_path="path/to/download")  # doctest: +SKIP

        # noqa: DAR402
        """
        return super().download(output_path=output_path)

    def delete(self) -> None:
        """
        Deletes the observation table.

        Raises
        ------
        RecordDeletionException
            When the record cannot be deleted properly

        Examples
        --------
        >>> observation_table = catalog.get_observation_table("observation_table_name")  # doctest: +SKIP
        >>> observation_table.delete()  # doctest: +SKIP

        # noqa: DAR402
        """
        super().delete()

    @typechecked
    def update_description(self, description: Optional[str]) -> None:
        """
        Update description for the observation table.

        Parameters
        ----------
        description: Optional[str]
            Description of the object

        Examples
        --------
        >>> observation_table = catalog.get_observation_table("observation_table")  # doctest: +SKIP
        >>> observation_table.update_description(description)  # doctest: +SKIP
        """
        super().update_description(description)

    @typechecked
    def update_purpose(self, purpose: Literal[tuple(Purpose)]) -> None:  # type: ignore[misc]
        """
        Update purpose for the observation table.

        Parameters
        ----------
        purpose: Literal[tuple(Purpose)]
            Purpose for the observation table. Expect value to be a string or a Purpose enum.

        Examples
        --------
        >>> observation_table = catalog.get_observation_table("observation_table")  # doctest: +SKIP
        >>> observation_table.update_purpose(fb.Purpose.EDA)  # doctest: +SKIP
        """
        self.update(
            update_payload={"purpose": purpose},
            allow_update_local=False,
            url=f"{self._route}/{self.id}",
            skip_update_schema_check=True,
        )

    @classmethod
    def upload(
        cls,
        file_path: Union[str, Path],
        name: str,
        purpose: Optional[Purpose] = None,
        primary_entities: Optional[List[str]] = None,
    ) -> ObservationTable:
        """
        Upload a file to create an observation table. This file can either be a CSV or Parquet file.

        Parameters
        ----------
        file_path: Union[str, Path]
            Path to file to upload. The file path should end in the appropriate .csv or .parquet file extension.
        name: str
            Name of the observation table to create.
        purpose: Optional[Purpose]
            Purpose of the observation table.
        primary_entities: Optional[List[str]]
            List of primary entities for the observation table.

        Returns
        -------
        ObservationTable

        Examples
        --------
        >>> observation_table = ObservationTable.upload(  # doctest: +SKIP
        ...     file_path="path/to/csv/file.csv",
        ...     name="observation_table_name",
        ...     purpose=fb.Purpose.PREVIEW,
        ...     primary_entities=["entity_name_1"],
        ... )
        """
        primary_entity_ids = []
        if primary_entities is not None:
            for entity_name in primary_entities:
                primary_entity_ids.append(Entity.get(entity_name).id)
        payload = ObservationTableUpload(
            name=name,
            purpose=purpose,
            primary_entity_ids=primary_entity_ids,
            uploaded_file_name=os.path.basename(file_path),
        )
        with open(file_path, "rb") as file_object:
            observation_table_doc = cls.post_async_task(
                route=f"{cls._route}/upload",
                payload={"payload": payload.json()},
                is_payload_json=False,
                files={"observation_set": file_object},
            )
        return ObservationTable.get_by_id(observation_table_doc["_id"])
