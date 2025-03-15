"""
EventTableService class
"""

from __future__ import annotations

from typing import Optional

from bson import ObjectId

from featurebyte.exception import (
    CronFeatureJobSettingConversionError,
    InvalidDefaultFeatureJobSettingError,
)
from featurebyte.models.event_table import EventTableModel
from featurebyte.query_graph.model.feature_job_setting import CronFeatureJobSetting
from featurebyte.schema.event_table import EventTableCreate, EventTableServiceUpdate
from featurebyte.service.base_table_document import BaseTableDocumentService


class EventTableService(
    BaseTableDocumentService[EventTableModel, EventTableCreate, EventTableServiceUpdate]
):
    """
    EventTableService class
    """

    document_class = EventTableModel
    document_update_class = EventTableServiceUpdate

    @property
    def class_name(self) -> str:
        return "EventTable"

    async def update_document(
        self,
        document_id: ObjectId,
        data: EventTableServiceUpdate,
        exclude_none: bool = True,
        document: Optional[EventTableModel] = None,
        return_document: bool = True,
        skip_block_modification_check: bool = False,
        populate_remote_attributes: bool = True,
    ) -> Optional[EventTableModel]:
        if isinstance(data, EventTableServiceUpdate) and isinstance(
            data.default_feature_job_setting, CronFeatureJobSetting
        ):
            try:
                _ = data.default_feature_job_setting.to_feature_job_setting()
            except CronFeatureJobSettingConversionError as e:
                raise InvalidDefaultFeatureJobSettingError(
                    "The provided CronFeatureJobSetting cannot be used as a default feature job "
                    f"setting ({str(e)})"
                ) from e
        return await super().update_document(
            document_id=document_id,
            data=data,
            exclude_none=exclude_none,
            document=document,
            return_document=return_document,
            skip_block_modification_check=skip_block_modification_check,
            populate_remote_attributes=populate_remote_attributes,
        )
