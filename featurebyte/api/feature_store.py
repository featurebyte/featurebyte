"""
FeatureStore class
"""
from __future__ import annotations

from typing import Any, Optional

from pydantic import Field

from featurebyte.api.api_object import SavableApiObject
from featurebyte.api.data_source import DataSource
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.enum import SourceType
from featurebyte.exception import RecordRetrievalException
from featurebyte.models.credential import DatabaseCredential, StorageCredential
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.query_graph.node.schema import DatabaseDetails
from featurebyte.schema.feature_store import FeatureStoreCreate


class FeatureStore(FeatureStoreModel, SavableApiObject):
    """
    FeatureStore class to represent a feature store in FeatureByte.
    This class is used to manage a feature store in FeatureByte.

    The purpose of a Feature Store is to centralize pre-calculated values,
    which can significantly reduce the latency of feature serving during training and inference.
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc(proxy_class="featurebyte.FeatureStore")

    # class variables
    _route = "/feature_store"
    _list_schema = FeatureStoreModel
    _get_schema = FeatureStoreModel
    _list_fields = ["name", "type", "created_at"]

    # pydantic instance variable (public)
    saved: bool = Field(
        default=False,
        allow_mutation=False,
        exclude=True,
        description="Flag to indicate whether the FeatureStore object is saved in the FeatureByte catalog.",
    )

    # optional credential parameters
    database_credential: Optional[DatabaseCredential] = None
    storage_credential: Optional[StorageCredential] = None

    def _get_create_payload(self) -> dict[str, Any]:
        data = FeatureStoreCreate(**self.json_dict())
        return data.json_dict()

    @classmethod
    def create(
        cls,
        name: str,
        source_type: SourceType,
        details: DatabaseDetails,
        database_credential: Optional[DatabaseCredential] = None,
        storage_credential: Optional[StorageCredential] = None,
    ) -> FeatureStore:
        """
        Create and return an instance of a feature store.

        Database details and credentials provided are validated.
        Note that only one feature store can be created for a specific set of database details.

        Parameters
        ----------
        name: str
            Name of the feature store.
        source_type: SourceType
            Type of the feature store.
        details: DatabaseDetails
            Details of the database to use for the feature store.
        database_credential: Optional[DatabaseCredential]
            Credential details to use when connecting to the database.
        storage_credential: Optional[StorageCredential]
            Credential details to use when connecting to the storage.

        Returns
        -------
        FeatureStore

        See Also
        --------
        - [FeatureStore.get_or_create](/reference/featurebyte.api.feature_store.FeatureStore.get_or_create/): Get or create FeatureStore
        """
        # Construct object, and save to persistent layer.
        feature_store = FeatureStore(
            name=name,
            type=source_type,
            details=details,
            database_credential=database_credential,
            storage_credential=storage_credential,
        )
        feature_store.save()
        return feature_store

    @classmethod
    def get_or_create(
        cls,
        name: str,
        source_type: SourceType,
        details: DatabaseDetails,
        database_credential: Optional[DatabaseCredential] = None,
        storage_credential: Optional[StorageCredential] = None,
    ) -> FeatureStore:
        """
        Create and return an instance of a feature store. If a feature store with the same name already exists,
        return that instead.

        Database details and credentials provided are validated.
        Note that only one feature store can be created for a specific set of database details.

        Parameters
        ----------
        name: str
            Name of the feature store.
        source_type: SourceType
            Type of the feature store.
        details: DatabaseDetails
            Details of the database to use for the feature store.
        database_credential: Optional[DatabaseCredential]
            Credential details to use when connecting to the database.
        storage_credential: Optional[StorageCredential]
            Credential details to use when connecting to the storage.

        Returns
        -------
        FeatureStore

        Examples
        --------
        Create a feature store housed in a Snowflake database

        >>> feature_store = fb.FeatureStore.get_or_create(
        ...     name="playground",
        ...     source_type=SourceType.SPARK,
        ...     details=fb.SparkDetails(
        ...         host="spark-thrift",
        ...         http_path="cliservice",
        ...         port=10000,
        ...         storage_type="file",
        ...         storage_url="/data/staging/featurebyte",
        ...         storage_spark_url="file:///opt/spark/data/staging/featurebyte",
        ...         featurebyte_catalog="spark_catalog",
        ...         featurebyte_schema="playground",
        ...     )
        ... )
        >>> FeatureStore.list()[["name", "type"]]
                  name   type
        0   playground  spark

        See Also
        --------
        - [FeatureStore.create](/reference/featurebyte.api.feature_store.FeatureStore.create/): Create FeatureStore
        """
        try:
            return FeatureStore.get(name=name)
        except RecordRetrievalException:
            return FeatureStore.create(
                name=name,
                source_type=source_type,
                details=details,
                database_credential=database_credential,
                storage_credential=storage_credential,
            )

    def get_data_source(self) -> DataSource:
        """
        Get the data source associated with the feature store.

        Returns
        -------
        DataSource
            DataSource object

        See Also
        --------
        - [DataSource](/reference/featurebyte.api.data_source.DataSource/): DataSource class
        """
        return DataSource(feature_store_model=self)
