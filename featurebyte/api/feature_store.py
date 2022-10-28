"""
FeatureStore class
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, List, Optional, TypeVar, cast

from http import HTTPStatus

from typeguard import typechecked

from featurebyte.api.api_object import SavableApiObject
from featurebyte.config import Configurations
from featurebyte.exception import RecordRetrievalException
from featurebyte.models.feature_store import FeatureStoreModel, TableDetails, TabularSource
from featurebyte.schema.feature_store import FeatureStoreCreate

if TYPE_CHECKING:
    from featurebyte.api.database_table import DatabaseTable
else:
    DatabaseTable = TypeVar("DatabaseTable")


class FeatureStore(FeatureStoreModel, SavableApiObject):
    """
    FeatureStore class
    """

    # class variables
    _route = "/feature_store"

    def _get_create_payload(self) -> dict[str, Any]:
        data = FeatureStoreCreate(**self.json_dict())
        return data.json_dict()

    @typechecked
    def list_databases(self) -> List[str]:
        """
        List databases accessible by the feature store

        Returns
        -------
        list databases

        Raises
        ------
        RecordRetrievalException
            Failed to retrieve database list
        """
        client = Configurations().get_client()
        response = client.post(url="/feature_store/database", json=self.json_dict())
        if response.status_code == HTTPStatus.OK:
            return cast(List[str], response.json())
        raise RecordRetrievalException(response)

    @typechecked
    def list_schemas(self, database_name: Optional[str] = None) -> List[str]:
        """
        List schemas in the database

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
            url=f"/feature_store/schema?database_name={database_name}", json=self.json_dict()
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
        List tables in the schema

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
            json=self.json_dict(),
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
    ) -> DatabaseTable:
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
        # pylint: disable=import-outside-toplevel
        from featurebyte.api.database_table import DatabaseTable

        return DatabaseTable(
            feature_store=self,
            tabular_source=TabularSource(
                feature_store_id=self.id,
                table_details=TableDetails(
                    database_name=database_name,
                    schema_name=schema_name,
                    table_name=table_name,
                ),
            ),
        )
