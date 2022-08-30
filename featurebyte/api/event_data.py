"""
EventData class
"""
from __future__ import annotations

from typing import Any, Dict, Optional

from http import HTTPStatus

from bson.objectid import ObjectId
from pydantic import validator
from typeguard import typechecked

from featurebyte.api.api_object import ApiObject
from featurebyte.api.database_table import DatabaseTable
from featurebyte.api.util import get_entity
from featurebyte.common.env_util import is_notebook
from featurebyte.common.model_util import validate_job_setting_parameters
from featurebyte.config import Configurations, Credentials
from featurebyte.core.mixin import GetAttrMixin, ParentMixin
from featurebyte.exception import DuplicatedRecordException, RecordRetrievalException
from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.models.event_data import EventDataModel
from featurebyte.models.feature_store import ColumnInfo
from featurebyte.schema.event_data import EventDataCreate, EventDataUpdate


class EventDataColumn(FeatureByteBaseModel, ParentMixin):
    """
    EventDataColumn class to set metadata like entity
    """

    info: ColumnInfo

    @typechecked
    def as_entity(self, entity_name: Optional[str]) -> None:
        """
        Set the column as the specified entity

        Parameters
        ----------
        entity_name: Optional[str]
            Associate column name to the entity, remove association if entity name is None
        """
        if entity_name is None:
            entity_id = None
        else:
            entity_dict = get_entity(entity_name)
            entity_id = entity_dict["_id"]

        columns_info = []
        for col in self.parent.columns_info:
            if col.name == self.info.name:
                self.info = ColumnInfo(**{**col.dict(), "entity_id": entity_id})
                columns_info.append(self.info)
            else:
                columns_info.append(col)

        self.parent.update({"columns_info": columns_info})


class EventData(EventDataModel, DatabaseTable, ApiObject, GetAttrMixin):
    """
    EventData class
    """

    # class variables
    _route = "/event_data"
    _update_schema_class = EventDataUpdate

    def _get_init_params_from_object(self) -> dict[str, Any]:
        return {"feature_store": self.feature_store, "credentials": self.credentials}

    def _get_create_payload(self) -> dict[str, Any]:
        data = EventDataCreate(**self.json_dict())
        return data.json_dict()

    @classmethod
    def _get_other_input_node_parameters(cls, values: dict[str, Any]) -> dict[str, Any]:
        return {"timestamp": values["event_timestamp_column"]}

    @classmethod
    @typechecked
    def from_tabular_source(
        cls,
        tabular_source: DatabaseTable,
        name: str,
        event_timestamp_column: str,
        record_creation_date_column: Optional[str] = None,
        credentials: Optional[Credentials] = None,
    ) -> EventData:
        """
        Create EventData object from tabular source

        Parameters
        ----------
        tabular_source: DatabaseTable
            DatabaseTable object constructed from FeatureStore
        name: str
            Event data name
        event_timestamp_column: str
            Event timestamp column from the given tabular source
        record_creation_date_column: str
            Record creation datetime column from the given tabular source
        credentials: Optional[Credentials]
            Credentials dictionary mapping from the config file

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
            _id=ObjectId(),
            name=name,
            tabular_source=tabular_source.tabular_source,
            columns_info=tabular_source.columns_info,
            event_timestamp_column=event_timestamp_column,
            record_creation_date_column=record_creation_date_column,
        )
        client = Configurations().get_client()
        response = client.get(url="/event_data/", params={"name": name})
        if response.status_code == HTTPStatus.OK:
            response_dict = response.json()
            if not response_dict["data"]:
                return EventData(
                    **data.dict(),
                    feature_store=tabular_source.feature_store,
                    credentials=credentials,
                )
            raise DuplicatedRecordException(
                response, f'EventData (event_data.name: "{name}") exists in saved record.'
            )
        raise RecordRetrievalException(response)

    @validator("event_timestamp_column")
    @classmethod
    def _check_event_timestamp_column_exists(cls, value: str, values: dict[str, Any]) -> str:
        columns = {dict(col)["name"] for col in values["columns_info"]}
        if value not in columns:
            raise ValueError(f'Column "{value}" not found in the table!')
        return value

    @validator("record_creation_date_column")
    @classmethod
    def _check_record_creation_date_column_exists(cls, value: str, values: dict[str, Any]) -> str:
        columns = {dict(col)["name"] for col in values["columns_info"]}
        if value and value not in columns:
            raise ValueError(f'Column "{value}" not found in the table!')
        return value

    @typechecked
    def __getitem__(self, item: str) -> EventDataColumn:
        """
        Retrieve column from the table

        Parameters
        ----------
        item: str
            Column name

        Returns
        -------
        EventDataColumn

        Raises
        ------
        KeyError
            when accessing non-exist column
        """
        info = None
        for col in self.columns_info:
            if col.name == item:
                info = col
        if info is None:
            raise KeyError(f'Column "{item}" does not exist!')
        output = EventDataColumn(info=info)
        output.set_parent(self)
        return output

    @typechecked
    def update_record_creation_date_column(self, record_creation_date_column: str) -> None:
        """
        Update record creation date column

        Parameters
        ----------
        record_creation_date_column: str
            Record creation date column used to perform feature job setting analysis
        """
        # perform record creation datetime column assignment first to
        # trigger record creation date column validation check
        self.record_creation_date_column = record_creation_date_column
        self.update({"record_creation_date_column": record_creation_date_column})

    @typechecked
    def update_default_feature_job_setting(
        self, feature_job_setting: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Update default feature job setting

        Parameters
        ----------
        feature_job_setting: Optional[Dict[str, Any]]
            Feature job setting dictionary contains blind_spot, frequency & time_modulo_frequency keys
        """
        if feature_job_setting:
            _ = validate_job_setting_parameters(**feature_job_setting)
        else:
            job_setting_analysis = self.post_async_task(
                route="/feature_job_setting_analysis", payload={"event_data_id": str(self.id)}
            )
            recommended_setting = job_setting_analysis["analysis_result"][
                "recommended_feature_job_setting"
            ]
            feature_job_setting = {
                "blind_spot": f'{recommended_setting["blind_spot"]}s',
                "time_modulo_frequency": f'{recommended_setting["job_time_modulo_frequency"]}s',
                "frequency": f'{recommended_setting["frequency"]}s',
            }

            if is_notebook():
                # pylint: disable=import-outside-toplevel
                from IPython.display import HTML, display  # pylint: disable=import-error

                display(HTML(job_setting_analysis["analysis_report"]))

        self.update({"default_feature_job_setting": feature_job_setting})

    @property
    def default_feature_job_setting_history(self) -> list[dict[str, Any]]:
        """
        List of default_job_setting history entries

        Returns
        -------
        list[dict[str, Any]]
        """
        return self._get_audit_history(field_name="default_feature_job_setting")
