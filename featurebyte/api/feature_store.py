"""
FeatureStore class
"""
from __future__ import annotations

from http import HTTPStatus

from featurebyte.api.database_table import DatabaseTable
from featurebyte.config import Configurations, Credentials
from featurebyte.core.generic import ExtendedFeatureStoreModel
from featurebyte.exception import (
    DuplicatedRecordException,
    RecordCreationException,
    RecordRetrievalException,
)
from featurebyte.models.feature_store import TableDetails


class FeatureStore(ExtendedFeatureStoreModel):
    """
    FeatureStore class
    """

    def list_databases(self, credentials: Credentials | None = None) -> list[str]:
        """
        List databases accessible by the feature store

        Parameters
        ----------
        credentials: Credentials
            configuration contains data source settings & credentials

        Returns
        -------
        list databases
        """
        return self.get_session(credentials=credentials).list_databases()

    def list_schemas(
        self, database_name: str | None = None, credentials: Credentials | None = None
    ) -> list[str]:
        """
        List schemas in the database

        Parameters
        ----------
        database_name: str | None
            Database name
        credentials: Credentials
            configuration contains data source settings & credentials


        Returns
        -------
        list schemas
        """
        return self.get_session(credentials=credentials).list_schemas(database_name=database_name)

    def list_tables(
        self,
        database_name: str | None = None,
        schema_name: str | None = None,
        credentials: Credentials | None = None,
    ) -> list[str]:
        """
        List tables in the schema

        Parameters
        ----------
        database_name: str | None
            Database name
        schema_name: str | None
            Schema name
        credentials: Credentials
            configuration contains data source settings & credentials

        Returns
        -------
        list tables
        """
        return self.get_session(credentials=credentials).list_tables(
            database_name=database_name, schema_name=schema_name
        )

    def get_table(
        self,
        table_name: str,
        database_name: str | None = None,
        schema_name: str | None = None,
        credentials: Credentials | None = None,
    ) -> DatabaseTable:
        """
        Get table from the feature store

        Parameters
        ----------
        table_name: str
            Table name
        database_name: str | None
            Database name
        schema_name: str | None
            Schema name
        credentials: Credentials
            configuration contains data source settings & credentials

        Returns
        -------
        DatabaseTable
        """
        return DatabaseTable(
            feature_store=self,
            tabular_source=(
                self.id,
                TableDetails(
                    database_name=database_name,
                    schema_name=schema_name,
                    table_name=table_name,
                ),
            ),
            credentials=credentials,
        )

    @classmethod
    def get(cls, name: str) -> FeatureStore:
        """
        Retrieve feature store from the persistent given feature store name

        Parameters
        ----------
        name: str
            Feature store name

        Returns
        -------
        FeatureStore
            FeatureStore object of the given feature store name

        Raises
        ------
        RecordRetrievalException
            When the feature store not found
        """
        client = Configurations().get_client()
        response = client.get(url="/feature_store/", params={"name": name})
        if response.status_code == HTTPStatus.OK:
            response_dict = response.json()
            if response_dict["data"]:
                feature_store_dict = response_dict["data"][0]
                return FeatureStore(**feature_store_dict)
        raise RecordRetrievalException(
            response, f'FeatureStore (feature_store.name: "{name}") not found!'
        )

    def save(self) -> None:
        """
        Save feature store to persistent
        """
        client = Configurations().get_client()
        response = client.post(url="/feature_store", json=self.json_dict())
        if response.status_code != HTTPStatus.CREATED:
            if response.status_code == HTTPStatus.CONFLICT:
                raise DuplicatedRecordException(response)
            raise RecordCreationException(response)
        type(self).__init__(self, **response.json())
