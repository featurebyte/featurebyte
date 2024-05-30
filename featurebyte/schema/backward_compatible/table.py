"""
Table API routes response
"""

from typing import Any, Dict, Optional, Sequence, Union
from typing_extensions import Annotated

from pydantic import Field, parse_obj_as, root_validator

from featurebyte.models.dimension_table import DimensionTableModel
from featurebyte.models.event_table import EventTableModel, FeatureJobSettingHistoryEntry
from featurebyte.models.item_table import ItemTableModel
from featurebyte.models.proxy_table import ProxyTableModel
from featurebyte.models.scd_table import SCDTableModel
from featurebyte.query_graph.model.feature_job_setting import FeatureJobSetting
from featurebyte.schema.event_table import EventTableList
from featurebyte.schema.scd_table import SCDTableList
from featurebyte.schema.table import TableList


class FeatureJobSettingResponse(FeatureJobSetting):
    """
    Feature Job Setting Response
    """

    frequency: str
    time_modulo_frequency: str

    @root_validator(pre=True)
    @classmethod
    def _handle_backward_compatibility_by_populating_older_fields(
        cls, values: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Handle backward compatibility by populating older fields

        Parameters
        ----------
        values : Dict[str, Any]
            Values to validate

        Returns
        -------
        Dict[str, Any]
            Validated values
        """
        if "period" in values:
            values["frequency"] = values["period"]
        if "offset" in values:
            values["time_modulo_frequency"] = values["offset"]
        return values


class FeatureJobSettingHistoryEntryResponse(FeatureJobSettingHistoryEntry):
    """
    Feature Job Setting History Entry Response
    """

    setting: Optional[FeatureJobSettingResponse]


class EventTableModelResponse(EventTableModel):
    """
    Event Table Model Response
    """

    default_feature_job_setting: Optional[FeatureJobSettingResponse]


class SCDTableModelResponse(SCDTableModel):
    """
    SCD Table Model Response
    """

    default_feature_job_setting: Optional[FeatureJobSettingResponse]


class EventTableListResponse(EventTableList):
    """
    Event Table List Response
    """

    data: Sequence[EventTableModelResponse]


class SCDTableListResponse(SCDTableList):
    """
    SCD Table List Response
    """

    data: Sequence[SCDTableModelResponse]


TableModel = Annotated[
    Union[EventTableModelResponse, ItemTableModel, DimensionTableModel, SCDTableModelResponse],
    Field(discriminator="type"),
]


class ProxyTableModelResponse(ProxyTableModel):  # pylint: disable=abstract-method
    """
    Pseudo Data class to support multiple table types.
    This class basically parses the persistent table model record & deserialized it into proper type.
    """

    def __new__(cls, **kwargs: Any) -> Any:
        return parse_obj_as(TableModel, kwargs)  # type: ignore[arg-type]


class TableListResponse(TableList):
    """
    Table List Response
    """

    data: Sequence[ProxyTableModelResponse]
