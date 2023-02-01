"""
FeatureStore class
"""
from __future__ import annotations

from typing import Any, List, Optional, cast

from http import HTTPStatus

from typeguard import typechecked

from featurebyte.api.api_object import SavableApiObject
from featurebyte.api.database_table import DatabaseTable
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.config import Configurations
from featurebyte.enum import SourceType
from featurebyte.exception import RecordRetrievalException
from featurebyte.models.credential import Credential
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.node.schema import DatabaseDetails, TableDetails
from featurebyte.schema.feature_store import FeatureStoreCreate


class FeatureStore(FeatureStoreModel, SavableApiObject):
    """
    FeatureStore class
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc(
        section=["FeatureStore"],
        proxy_class="featurebyte.FeatureStore",
    )

    # class variables
    _route = "/feature_store"
    _list_schema = FeatureStoreModel
    _get_schema = FeatureStoreModel
    _list_fields = ["name", "type", "created_at"]

    # optional credential parameters
    credentials: Optional[Credential] = None

    def _get_create_payload(self) -> dict[str, Any]:
        data = FeatureStoreCreate(**self.json_dict())
        return data.json_dict()

    @classmethod
    def create(
        cls,
        name: str,
        source_type: SourceType,
        details: DatabaseDetails,
        credentials: Optional[Credential] = None,
    ) -> FeatureStore:
        """
        Create will return a new instance of a feature store.
        We prefer to use this over the default constructor of `FeatureStore` because
        we want to perform some additional validation checks.
        Note that this function should only be called once for a specific set of details.

        Parameters
        ----------
        name: str
            feature store name
        source_type: SourceType
            type of feature store
        details: DatabaseDetails
            details of the database we want to connect to
        credentials: Optional[Credential]
            Credentials for the data warehouse. If there are already credentials in your configuration file,
            these will be ignored.

        Returns
        -------
        FeatureStore

        Examples
        --------
        Create a feature store housed in a Snowflake database

        >>> import featurebyte as fb
        >>> fb.FeatureStore.create(  # doctest: +SKIP
        ...     name="Snowflake Feature Store",
        ...     source_type=fb.SourceType.SNOWFLAKE,
        ...     details=fb.SnowflakeDetails(
        ...         account="xxx.us-central1.gcp",
        ...         warehouse="COMPUTE_WH",
        ...         database="FEATUREBYTE",
        ...         sf_schema="FEATURESTORE",
        ...     ),
        ...     credentials=fb.Credential(
        ...         name="Snowflake Feature Store",
        ...         credential_type="USERNAME_PASSWORD",
        ...         credential=fb.UsernamePasswordCredential(
        ...             username="username",
        ...             password="password"
        ...         )
        ...     )
        ... )

        List created feature stores
        >>> fb.FeatureStore.list()  # doctest: +SKIP
                              name       type              created_at
        0  Snowflake Feature Store  snowflake 2023-01-04 12:16:51.811

        """
        # Construct object, and save to persistent layer.
        feature_store = FeatureStore(
            name=name, type=source_type, details=details, credentials=credentials
        )
        feature_store.save()
        return feature_store

    @typechecked
    def list_databases(self) -> List[str]:
        """
        List databases accessible by the feature store

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
