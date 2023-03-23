"""
FeatureStore class
"""
from __future__ import annotations

from typing import List, Optional, cast

from http import HTTPStatus

from typeguard import typechecked

from featurebyte.api.source_table import SourceTable
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.config import Configurations
from featurebyte.enum import SourceType
from featurebyte.exception import RecordRetrievalException
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.node.schema import TableDetails


class DataSource:
    """
    DataSource class to represent a data source in FeatureByte.
    This class is used to manage a data source in FeatureByte.
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc(
        section=["DataSource"],
        proxy_class="featurebyte.DataSource",
    )

    def __init__(self, feature_store_model: FeatureStoreModel):
        self._feature_store = feature_store_model

    @property
    def type(self) -> SourceType:
        """
        Get the data source type, which indicates how the data is stored and computed.
        e.g. `SourceType.SPARK`

        Returns
        -------
        SourceType
            Data source type.

        Examples
        --------
        >>> fb.FeatureStore.get("playground").get_data_source().type
        'spark'

        See Also
        --------
        - [SourceType](/reference/featurebyte.enum.SourceType/): SourceType
        """
        return self._feature_store.type

    @typechecked
    def list_databases(self) -> List[str]:
        """
        List databases in the data source.

        Returns
        -------
        List[str]
            List of databases.

        Raises
        ------
        RecordRetrievalException
            Failed to retrieve database list.

        Examples
        --------
        >>> data_source = fb.FeatureStore.get("playground").get_data_source()
        >>> data_source.list_databases()
        ['spark_catalog']
        """
        client = Configurations().get_client()
        response = client.post(url="/feature_store/database", json=self._feature_store.json_dict())
        if response.status_code == HTTPStatus.OK:
            return cast(List[str], response.json())
        raise RecordRetrievalException(response)

    @typechecked
    def list_schemas(self, database_name: Optional[str] = None) -> List[str]:
        """
        List schemas in a database.

        Parameters
        ----------
        database_name: Optional[str]
            Name of database.

        Returns
        -------
        List[str]
            List of schemas.

        Raises
        ------
        RecordRetrievalException
            Failed to retrieve database schema list.

        Examples
        --------
        >>> data_source = fb.FeatureStore.get("playground").get_data_source()
        >>> data_source.list_schemas(database_name="spark_catalog")
        ['default', 'doctest_grocery', 'playground']
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
        List tables in a database schema.

        Parameters
        ----------
        database_name: Optional[str]
            Name of database.
        schema_name: Optional[str]
            Name of schema.

        Returns
        -------
        List[str]
            List of tables.

        Raises
        ------
        RecordRetrievalException
            Failed to retrieve database table list

        Examples
        --------
        >>> data_source = fb.FeatureStore.get("playground").get_data_source()
        >>> data_source.list_tables(
        ...     database_name="spark_catalog",
        ...     schema_name="doctest_grocery",
        ... )[:3]
        ['grocerycustomer', 'groceryinvoice', 'groceryproduct']
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
        Get a table in a database schema.

        Parameters
        ----------
        table_name: str
            Name of table.
        database_name: Optional[str]
            Name of database.
        schema_name: Optional[str]
            Name of schema.

        Returns
        -------
        SourceTable
            SourceTable object.

        Examples
        --------
        >>> data_source = fb.FeatureStore.get("playground").get_data_source()
        >>> source_table = data_source.get_table(
        ...     table_name="groceryinvoice",
        ...     database_name="spark_catalog",
        ...     schema_name="doctest_grocery",
        ... )
        >>> source_table.columns
        ['GroceryInvoiceGuid', 'GroceryCustomerGuid', 'Timestamp', 'record_available_at', 'Amount']

        See Also
        --------
        - [SourceTable](/reference/featurebyte.api.source_table.SourceTable/): SourceTable
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
