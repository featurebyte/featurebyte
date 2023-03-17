"""
FeatureStore class
"""
from __future__ import annotations

from typing import List, Optional, cast

from http import HTTPStatus

from typeguard import typechecked

from featurebyte.api.source_table import SourceTable
from featurebyte.config import Configurations
from featurebyte.enum import SourceType
from featurebyte.exception import RecordRetrievalException
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.node.schema import TableDetails


class DataSource:
    """
    DataSource class to represent a table source in FeatureByte.
    """

    def __init__(self, feature_store_model: FeatureStoreModel):
        self._feature_store = feature_store_model

    @property
    def type(self) -> SourceType:
        """
        Get table source type

        Returns
        -------
        SourceType
            Data source type
        """
        return self._feature_store.type

    @typechecked
    def list_databases(self) -> List[str]:
        """
        List databases in a table source

        Returns
        -------
        List[str]
            List of databases

        Raises
        ------
        RecordRetrievalException
            Failed to retrieve database list
        """
        client = Configurations().get_client()
        response = client.post(url="/feature_store/database", json=self._feature_store.json_dict())
        if response.status_code == HTTPStatus.OK:
            return cast(List[str], response.json())
        raise RecordRetrievalException(response)

    @typechecked
    def list_schemas(self, database_name: Optional[str] = None) -> List[str]:
        """
        List schemas in a database

        Parameters
        ----------
        database_name: Optional[str]
            Database name

        Returns
        -------
        list schemas

        Raises
        ------
        RecordRetrievalException
            Failed to retrieve database schema list
        """
        client = Configurations().get_client()
        response = client.post(
            url=f"/feature_store/schema?database_name={database_name}",
            json=self._feature_store.json_dict(),
        )
        if response.status_code == HTTPStatus.OK:
            return cast(List[str], response.json())
        raise RecordRetrievalException(response)

    @typechecked
    def list_tables(
        self,
        database_name: Optional[str] = None,
        schema_name: Optional[str] = None,
    ) -> List[str]:
        """
        List tables in a schema

        Parameters
        ----------
        database_name: Optional[str]
            Database name
        schema_name: Optional[str]
            Schema name

        Returns
        -------
        list tables

        Raises
        ------
        RecordRetrievalException
            Failed to retrieve database table list
        """
        client = Configurations().get_client()
        response = client.post(
            url=f"/feature_store/table?database_name={database_name}&schema_name={schema_name}",
            json=self._feature_store.json_dict(),
        )
        if response.status_code == HTTPStatus.OK:
            return cast(List[str], response.json())
        raise RecordRetrievalException(response)

    @typechecked
    def get_table(
        self,
        table_name: str,
        database_name: Optional[str] = None,
        schema_name: Optional[str] = None,
    ) -> SourceTable:
        """
        Get table from the feature store

        Parameters
        ----------
        table_name: str
            Table name
        database_name: Optional[str]
            Database name
        schema_name: Optional[str]
            Schema name

        Returns
        -------
        DatabaseTable
        """
        return SourceTable(
            feature_store=self._feature_store,
            tabular_source=TabularSource(
                feature_store_id=self._feature_store.id,
                table_details=TableDetails(
                    database_name=database_name,
                    schema_name=schema_name,
                    table_name=table_name,
                ),
            ),
        )
