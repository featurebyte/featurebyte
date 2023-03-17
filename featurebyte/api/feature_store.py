"""
FeatureStore class
"""
from __future__ import annotations

from typing import Any, Optional

from featurebyte.api.api_object import SavableApiObject
from featurebyte.api.data_source import DataSource
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.enum import SourceType
from featurebyte.exception import RecordRetrievalException
from featurebyte.models.credential import Credential
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.query_graph.node.schema import DatabaseDetails
from featurebyte.schema.feature_store import FeatureStoreCreate


class FeatureStore(FeatureStoreModel, SavableApiObject):
    """
    FeatureStore class to represent a feature store in FeatureByte.
    This class is used to manage a feature store in FeatureByte.
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
        Save and return a new instance of a feature store.
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

        See Also
        --------
        FeatureStore.get_or_create
        """
        # Construct object, and save to persistent layer.
        feature_store = FeatureStore(
            name=name, type=source_type, details=details, credentials=credentials
        )
        feature_store.save()
        return feature_store

    @classmethod
    def get_or_create(
        cls,
        name: str,
        source_type: SourceType,
        details: DatabaseDetails,
        credentials: Optional[Credential] = None,
    ) -> FeatureStore:
        """
        Save and return a new instance of a feature store. If a feature store with the same name already exists,
        return that feature store instead.

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
        >>> feature_store.name
        'playground'

        List created feature stores
        >>> FeatureStore.list()  # doctest: +SKIP
                  name   type              created_at
        0   playground  spark 2023-03-16 09:22:23.755

        See Also
        --------
        FeatureStore.create
        """
        try:
            return FeatureStore.get(name=name)
        except RecordRetrievalException:
            return FeatureStore.create(
                name=name,
                source_type=source_type,
                details=details,
                credentials=credentials,
            )

    def get_data_source(self) -> DataSource:
        """
        Get the data source associated with the feature store

        Returns
        -------
        DataSource
            DataSource object
        """
        return DataSource(feature_store_model=self)
