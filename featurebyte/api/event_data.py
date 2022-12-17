"""
EventData class
"""
from __future__ import annotations

from typing import Any, Optional

from bson.objectid import ObjectId
from typeguard import typechecked

from featurebyte.api.data import DataApiObject
from featurebyte.api.database_table import DatabaseTable
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.common.env_util import display_html_in_notebook
from featurebyte.models.event_data import EventDataModel, FeatureJobSetting
from featurebyte.query_graph.model.table import EventTableData
from featurebyte.schema.event_data import EventDataCreate, EventDataUpdate


class EventData(EventDataModel, DataApiObject):
    """
    EventData class
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc(
        section=["Data"],
        proxy_class="featurebyte.EventData",
    )

    # class variables
    _route = "/event_data"
    _update_schema_class = EventDataUpdate
    _create_schema_class = EventDataCreate
    _table_data_class = EventTableData

    @property
    def timestamp_column(self) -> Optional[str]:
        """
        Event timestamp column

        Returns
        -------
        Optional[str]
        """
        return self.event_timestamp_column

    @classmethod
    @typechecked
    def from_tabular_source(
        cls,
        tabular_source: DatabaseTable,
        name: str,
        event_timestamp_column: str,
        event_id_column: str,
        record_creation_date_column: Optional[str] = None,
        _id: Optional[ObjectId] = None,
    ) -> EventData:
        """
        Create EventData object from tabular source

        Parameters
        ----------
        tabular_source: DatabaseTable
            DatabaseTable object constructed from FeatureStore
        name: str
            Event data name
        event_id_column: str
            Event ID column from the given tabular source
        event_timestamp_column: str
            Event timestamp column from the given tabular source
        record_creation_date_column: str
            Record creation datetime column from the given tabular source
        _id: Optional[ObjectId]
            Identity value for constructed object

        Returns
        -------
        EventData

        Examples
        --------

        Create EventData from a table in the feature store

        >>> credit_card_transactions = EventData.from_tabular_source(  # doctest: +SKIP
        ...    name="Credit Card Transactions",
        ...    tabular_source=feature_store.get_table(
        ...      database_name="DEMO",
        ...      schema_name="CREDIT_CARD",
        ...      table_name="TRANSACTIONS"
        ...    ),
        ...    event_id_column="TRANSACTIONID",
        ...    event_timestamp_column="TIMESTAMP",
        ...    record_creation_date_column="RECORD_AVAILABLE_AT",
        ... )

        Get information about the EventData

        >>> credit_card_transactions.info(verbose=True)  # doctest: +SKIP
        {'name': 'CCDEMOTRANSACTIONS',
        'created_at': '2022-10-17T08:09:15.730000',
        'updated_at': '2022-11-14T12:15:59.707000',
        'status': 'DRAFT',
        'event_timestamp_column': 'TIMESTAMP',
        'record_creation_date_column': 'RECORD_AVAILABLE_AT',
        'table_details': {'database_name': 'DEMO',
        'schema_name': 'CREDIT_CARD',
        'table_name': 'TRANSACTIONS'},
        'default_feature_job_setting': {'blind_spot': '30s',
        'frequency': '60m',
        'time_modulo_frequency': '15s'},
        'entities': [{'name': 'ACCOUNTID', 'serving_names': ['ACCOUNTID']}],
        'column_count': 6,
        'columns_info': [{'name': 'TRANSACTIONID',
        'dtype': 'VARCHAR',
        'entity': None},
        {'name': 'ACCOUNTID', 'dtype': 'VARCHAR', 'entity': 'ACCOUNTID'},
        {'name': 'TIMESTAMP', 'dtype': 'TIMESTAMP', 'entity': None},
        {'name': 'RECORD_AVAILABLE_AT', 'dtype': 'TIMESTAMP', 'entity': None},
        {'name': 'DESCRIPTION', 'dtype': 'VARCHAR', 'entity': None},
        {'name': 'AMOUNT', 'dtype': 'FLOAT', 'entity': None}]}

        """
        return super().create(
            tabular_source=tabular_source,
            name=name,
            record_creation_date_column=record_creation_date_column,
            _id=_id,
            event_timestamp_column=event_timestamp_column,
            event_id_column=event_id_column,
        )

    @typechecked
    def update_default_feature_job_setting(
        self, feature_job_setting: Optional[FeatureJobSetting] = None
    ) -> None:
        """
        Update default feature job setting

        Parameters
        ----------
        feature_job_setting: Optional[FeatureJobSetting]
            Feature job setting object (auto-detected if not provided)
        """
        if feature_job_setting is None:
            job_setting_analysis = self.post_async_task(
                route="/feature_job_setting_analysis", payload={"event_data_id": str(self.id)}
            )
            recommended_setting = job_setting_analysis["analysis_result"][
                "recommended_feature_job_setting"
            ]
            feature_job_setting = FeatureJobSetting(
                blind_spot=f'{recommended_setting["blind_spot"]}s',
                time_modulo_frequency=f'{recommended_setting["job_time_modulo_frequency"]}s',
                frequency=f'{recommended_setting["frequency"]}s',
            )

            display_html_in_notebook(job_setting_analysis["analysis_report"])

        self.update(
            update_payload={"default_feature_job_setting": feature_job_setting.dict()},
            allow_update_local=True,
        )

    @property
    def default_feature_job_setting_history(self) -> list[dict[str, Any]]:
        """
        List of default_job_setting history entries

        Returns
        -------
        list[dict[str, Any]]
        """
        return self._get_audit_history(field_name="default_feature_job_setting")
