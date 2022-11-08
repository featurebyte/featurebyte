"""
EventData class
"""
from __future__ import annotations

from typing import Any, Optional

from http import HTTPStatus

from bson.objectid import ObjectId
from typeguard import typechecked

from featurebyte.api.data import DataApiObject
from featurebyte.api.database_table import DatabaseTable
from featurebyte.common.env_util import display_html_in_notebook
from featurebyte.config import Configurations
from featurebyte.enum import TableDataType
from featurebyte.exception import DuplicatedRecordException, RecordRetrievalException
from featurebyte.models.event_data import EventDataModel, FeatureJobSetting
from featurebyte.schema.event_data import EventDataCreate, EventDataUpdate


class EventData(EventDataModel, DataApiObject):
    """
    EventData class
    """

    # class variables
    _route = "/event_data"
    _update_schema_class = EventDataUpdate

    def _get_create_payload(self) -> dict[str, Any]:
        data = EventDataCreate(**self.json_dict())
        return data.json_dict()

    @classmethod
    def _get_other_input_node_parameters(cls, values: dict[str, Any]) -> dict[str, Any]:
        # the key `_id` is used during deserialization, the key `id` is used during setattr
        return {
            "type": TableDataType.EVENT_DATA,
            "timestamp": values["event_timestamp_column"],
            "id": values.get("_id", values.get("id")),
        }

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

        Raises
        ------
        DuplicatedRecordException
            When record with the same key exists at the persistent layer
        RecordRetrievalException
            When unexpected retrieval failure
        """
        data = EventDataCreate(
            _id=_id or ObjectId(),
            name=name,
            tabular_source=tabular_source.tabular_source,
            columns_info=tabular_source.columns_info,
            event_timestamp_column=event_timestamp_column,
            event_id_column=event_id_column,
            record_creation_date_column=record_creation_date_column,
        )
        client = Configurations().get_client()
        response = client.get(url=cls._route, params={"name": name})
        if response.status_code == HTTPStatus.OK:
            response_dict = response.json()
            if not response_dict["data"]:
                return EventData(
                    **data.json_dict(),
                    feature_store=tabular_source.feature_store,
                )
            raise DuplicatedRecordException(
                response, f'EventData (event_data.name: "{name}") exists in saved record.'
            )
        raise RecordRetrievalException(response)

    @typechecked
    def update_record_creation_date_column(self, record_creation_date_column: str) -> None:
        """
        Update record creation date column

        Parameters
        ----------
        record_creation_date_column: str
            Record creation date column used to perform feature job setting analysis
        """
        self.update(
            update_payload={"record_creation_date_column": record_creation_date_column},
            allow_update_local=True,
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
            Feature job setting object (if not provided,
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
