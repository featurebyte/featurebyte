"""
EventData class
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, Literal, Optional, Type, Union

from datetime import datetime

import pandas as pd
from bson.objectid import ObjectId
from pydantic import Field, StrictStr, root_validator
from typeguard import typechecked

from featurebyte.api.base_data import DataApiObject
from featurebyte.api.database_table import DatabaseTable
from featurebyte.api.feature_job_setting_analysis import FeatureJobSettingAnalysis
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.common.validator import construct_data_model_root_validator
from featurebyte.enum import DBVarType, TableDataType
from featurebyte.exception import InvalidSettingsError, RecordRetrievalException
from featurebyte.models.event_data import EventDataModel
from featurebyte.query_graph.model.feature_job_setting import FeatureJobSetting
from featurebyte.query_graph.model.table import AllTableDataT, EventTableData
from featurebyte.schema.event_data import EventDataCreate, EventDataUpdate
from featurebyte.schema.feature_job_setting_analysis import FeatureJobSettingAnalysisCreate

if TYPE_CHECKING:
    from featurebyte.api.event_view import EventView


class EventData(DataApiObject):
    """
    EventData is an object connected with an event table in the data warehouse. These tables must have the following
    properties:\n
    - and an event_id column as a primary key\n
    - an event timestamp

    Users are strongly encouraged to annotate the data by tagging entities and defining:

    - the semantic of the data field
    - critical data information on the data quality that requires cleaning before feature engineering.

    Before registering a new EventData, users are asked to set the default for the FeatureJob scheduling for features
    that will be extracted from the EventData.

    To build features, users create Event Views from EventData.
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc(section=["Data"], proxy_class="featurebyte.EventData")

    # class variables
    _route = "/event_data"
    _update_schema_class = EventDataUpdate
    _create_schema_class = EventDataCreate
    _get_schema = EventDataModel
    _table_data_class: ClassVar[Type[AllTableDataT]] = EventTableData

    # pydantic instance variable (public)
    type: Literal[TableDataType.EVENT_DATA] = Field(TableDataType.EVENT_DATA, const=True)

    # pydantic instance variable (internal use)
    internal_default_feature_job_setting: Optional[FeatureJobSetting] = Field(
        alias="default_feature_job_setting"
    )
    internal_event_timestamp_column: StrictStr = Field(alias="event_timestamp_column")
    internal_event_id_column: Optional[StrictStr] = Field(alias="event_id_column")  # DEV-556

    # pydantic validators
    _root_validator = root_validator(allow_reuse=True)(
        construct_data_model_root_validator(
            columns_info_key="internal_columns_info",
            expected_column_field_name_type_pairs=[
                ("internal_record_creation_date_column", DBVarType.supported_timestamp_types()),
                ("internal_event_timestamp_column", DBVarType.supported_timestamp_types()),
                ("internal_event_id_column", DBVarType.supported_id_types()),
            ],
        )
    )

    def get_view(self) -> EventView:
        """
        Create an EventView object corresponding to the EventData

        Returns
        -------
        EventView
            The created EventView object

        Example
        -------
        Create an EventView from an EventData

        >>> import featurebyte
        >>> event_data = featurebyte.EventData.get("GroceryInvoice")
        >>> event_view = event_data.get_view()
        """
        from featurebyte.api.event_view import EventView  # pylint: disable=import-outside-toplevel

        return EventView.from_event_data(event_data=self)

    @property
    def default_feature_job_setting(self) -> Optional[FeatureJobSetting]:
        """
        Default feature job setting of the EventData

        Returns
        -------
        Optional[FeatureJobSetting]
        """
        try:
            return self.cached_model.default_feature_job_setting
        except RecordRetrievalException:
            return self.internal_default_feature_job_setting

    @property
    def event_timestamp_column(self) -> str:
        """
        Event timestamp column name of the EventData

        Returns
        -------
        str
        """
        try:
            return self.cached_model.event_timestamp_column
        except RecordRetrievalException:
            return self.internal_event_timestamp_column

    @property
    def event_id_column(self) -> Optional[str]:
        """
        Event ID column name of the EventData associated with the ItemData

        Returns
        -------
        str
        """
        try:
            return self.cached_model.event_id_column
        except RecordRetrievalException:
            return self.internal_event_id_column

    @property
    def timestamp_column(self) -> Optional[str]:
        """
        Timestamp column name of the EventData

        Returns
        -------
        Optional[str]
        """
        return self.event_timestamp_column

    @property
    def default_feature_job_setting_history(self) -> list[dict[str, Any]]:
        """
        List of default_job_setting history entries

        Returns
        -------
        list[dict[str, Any]]
        """
        return self._get_audit_history(field_name="default_feature_job_setting")

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
    def update_default_feature_job_setting(self, feature_job_setting: FeatureJobSetting) -> None:
        """
        Update default feature job setting

        Parameters
        ----------
        feature_job_setting: FeatureJobSetting
            Feature job setting object
        """
        self.update(
            update_payload={"default_feature_job_setting": feature_job_setting.dict()},
            allow_update_local=True,
            add_internal_prefix=True,
        )

    @typechecked
    def create_new_feature_job_setting_analysis(
        self,
        analysis_date: Optional[datetime] = None,
        analysis_length: int = 2419200,
        min_featurejob_period: int = 60,
        exclude_late_job: bool = False,
        blind_spot_buffer_setting: int = 5,
        job_time_buffer_setting: Union[int, Literal["auto"]] = "auto",
        late_data_allowance: float = 5e-5,
    ) -> FeatureJobSettingAnalysis:
        """
        Create new feature job setting analysis on the event data

        Parameters
        ----------
        analysis_date: Optional[datetime]
            Analysis date
        analysis_length: int
            Length of analysis (seconds)
        min_featurejob_period: int
            Minimum period for a feature job
        exclude_late_job: bool
            Exclude late jobs in analysis
        job_time_buffer_setting: Union[int, Literal["auto"]]
            Buffer time for job execution (seconds)
        blind_spot_buffer_setting: int
            Buffer time for data population blind spot
        late_data_allowance: float
            Threshold for late records (percentile)

        Returns
        -------
        FeatureJobSettingAnalysis
        """
        payload = FeatureJobSettingAnalysisCreate(
            event_data_id=self.id,
            analysis_date=analysis_date,
            analysis_length=analysis_length,
            min_featurejob_period=min_featurejob_period,
            exclude_late_job=exclude_late_job,
            blind_spot_buffer_setting=blind_spot_buffer_setting,
            job_time_buffer_setting=job_time_buffer_setting,
            late_data_allowance=late_data_allowance,
        )
        job_setting_analysis = self.post_async_task(
            route="/feature_job_setting_analysis", payload=payload.json_dict()
        )
        analysis = FeatureJobSettingAnalysis.get_by_id(job_setting_analysis["_id"])
        analysis.display_report()
        return analysis

    @typechecked
    def initialize_default_feature_job_setting(self) -> None:
        """
        Initialize default feature job setting by performing an analysis on the data

        Raises
        ------
        InvalidSettingsError
            Default feature job setting is already initialized
        """
        if self.default_feature_job_setting:
            raise InvalidSettingsError("Default feature job setting is already initialized")

        analysis = self.create_new_feature_job_setting_analysis()
        self.update_default_feature_job_setting(analysis.get_recommendation())

    @typechecked
    def list_feature_job_setting_analysis(self) -> Optional[pd.DataFrame]:
        """
        List feature job setting analysis that has been performed

        Returns
        -------
        Optional[DataFrame]
            Table of feature job analysis
        """
        return FeatureJobSettingAnalysis.list(event_data_id=self.id)
