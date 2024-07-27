"""
FeatureStore class
"""

from __future__ import annotations

from typing import Any, ClassVar, Dict, List, Optional

from bson import ObjectId
from pandas import DataFrame

from featurebyte.api.data_source import DataSource
from featurebyte.api.savable_api_object import DeletableApiObject, SavableApiObject
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.enum import SourceType
from featurebyte.exception import RecordRetrievalException
from featurebyte.models.credential import DatabaseCredential, StorageCredential
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.query_graph.node.schema import DatabaseDetails
from featurebyte.schema.feature_store import DatabaseDetailsUpdate, FeatureStoreCreate


class FeatureStore(FeatureStoreModel, SavableApiObject, DeletableApiObject):
    """
    FeatureStore class to represent a feature store in FeatureByte.
    This class is used to manage a feature store in FeatureByte.

    The purpose of a Feature Store is to centralize pre-calculated values,
    which can significantly reduce the latency of feature serving during training and inference.
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(proxy_class="featurebyte.FeatureStore")
    _route: ClassVar[str] = "/feature_store"
    _list_schema: ClassVar[Any] = FeatureStoreModel
    _get_schema: ClassVar[Any] = FeatureStoreModel
    _list_fields: ClassVar[List[str]] = ["name", "type", "created_at"]

    # optional credential parameters
    database_credential: Optional[DatabaseCredential] = None
    storage_credential: Optional[StorageCredential] = None

    def _get_create_payload(self) -> dict[str, Any]:
        data = FeatureStoreCreate(**self.model_dump(by_alias=True))
        return data.json_dict()

    def info(self, verbose: bool = False) -> Dict[str, Any]:
        """
        Returns a dictionary that summarizes the essential information of the feature store represented by the
        FeatureStore object. The dictionary contains the following keys:

        - `name`: The name of the feature store.
        - `created_at`: The timestamp indicating when the feature store owas created.
        - `updated_at`: The timestamp indicating when the FeatureStore object was last updated.
        - `source`: The type of the feature store (Spark, Snowflake, DataBricks,...).
        - `database_details`: details of the database used by the feature store.

        Parameters
        ----------
        verbose: bool
            The parameter "verbose" in the current state of the code does not have any impact on the output.

        Returns
        -------
        Dict[str, Any]
            Key-value mapping of properties of the object.

        Examples
        --------
        >>> feature_store = fb.FeatureStore.get(<feature_store_name>)  # doctest: +SKIP
        >>> feature_store.info()  # doctest: +SKIP
        """
        return super().info(verbose)

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
        Creates and saves a Feature Store object to enable FeatureByte to work with a data warehouse. FeatureByte
        leverages a data warehouse as both a data source and a feature store.

        To create a feature store, you need to specify the connection details and credentials to use.

        Note that featurestore is one-off task. Only one feature store can be created for a specific set of
        database details.

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
        >>> feature_store = fb.FeatureStore.create(  # doctest: +SKIP
        ...     name="playground",
        ...     source_type=SourceType.SPARK,
        ...     details=fb.SparkDetails(
        ...         host="spark-thrift",
        ...         http_path="cliservice",
        ...         port=10000,
        ...         storage_type="file",
        ...         storage_url="/data/staging/featurebyte",
        ...         storage_path="file:///opt/spark/data/staging/featurebyte",
        ...         catalog_name="spark_catalog",
        ...         schema_name="playground",
        ...     ),
        ... )
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
        ...         storage_path="file:///opt/spark/data/staging/featurebyte",
        ...         catalog_name="spark_catalog",
        ...         schema_name="playground",
        ...     ),
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

    @classmethod
    def get(cls, name: str) -> FeatureStore:
        """
        Gets a FeatureStore object by its name.

        Parameters
        ----------
        name: str
            Name of the feature store to retrieve.

        Returns
        -------
        FeatureStore
            FeatureStore object.

        Examples
        --------
        Get a FeatureStore object that is already saved.

        >>> feature_store = fb.FeatureStore.get("feature_store_name")  # doctest: +SKIP
        """
        return super().get(name)

    @classmethod
    def get_by_id(
        cls,
        id: ObjectId,
    ) -> FeatureStore:
        """
        Returns a FeatureStore object by its unique identifier (ID).

        Parameters
        ----------
        id: ObjectId
            FeatureStore unique identifier (ID) to retrieve.

        Returns
        -------
        FeatureStore
            FeatureStore object.

        Examples
        --------
        Get a FeatureStore object by its Object ID.

        >>> fb.FeatureStore.get_by_id(<catalog_id>)  # doctest: +SKIP
        """
        return cls._get_by_id(id=id)

    @classmethod
    def list(cls, include_id: Optional[bool] = True) -> DataFrame:
        """
        Returns a DataFrame that lists the feature stores by their names, types and creation dates.

        Parameters
        ----------
        include_id: Optional[bool]
            Whether to include id in the list.

        Returns
        -------
        DataFrame
            Table of objects.

        Examples
        --------
        List all feature stores.

        >>> feature_stores = fb.FeatureStore.list()
        """
        return super().list(include_id=include_id)

    def get_data_source(self) -> DataSource:
        """
        Gets the data source associated with the feature store.

        Returns
        -------
        DataSource
            DataSource object

        Examples
        --------
        Get a data source from a feature store.

        >>> data_source = fb.FeatureStore.get("playground").get_data_source()

        See Also
        --------
        - [DataSource](/reference/featurebyte.api.data_source.DataSource/): DataSource class
        """
        return DataSource(feature_store_model=self)

    def delete(self) -> None:
        """
        Deletes the feature store from the persistent data store. The feature store can only be deleted if it is not
        being referenced by any catalog.

        Examples
        --------
        >>> feature_store = fb.FeatureStore.get_by_id(<catalog_id>)  # doctest: +SKIP
        >>> feature_store.delete()  # doctest: +SKIP
        """
        self._delete()

    def update_details(
        self, http_path: Optional[str] = None, warehouse: Optional[str] = None
    ) -> None:
        """
        Updates the details of the feature store.

        Parameters
        ----------
        http_path: Optional[str]
            HTTP path to use.
        warehouse: Optional[str]
            Warehouse to use.

        Examples
        --------
        Update the details of a feature store.

        >>> feature_store = fb.FeatureStore.get_by_id(<catalog_id>)  # doctest: +SKIP
        >>> feature_store.update_details(warehouse="feature_engineering")  # doctest: +SKIP
        """
        self.update(
            update_payload=DatabaseDetailsUpdate(
                http_path=http_path, warehouse=warehouse
            ).json_dict(),
            allow_update_local=False,
            url=f"{self._route}/{self.id}/details",
            skip_update_schema_check=True,
        )
