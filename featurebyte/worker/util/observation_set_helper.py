"""
Observation table helper
"""

from pathlib import Path
from typing import Optional, Union

import pandas as pd

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.observation_table import ObservationTableModel
from featurebyte.service.observation_table import ObservationTableService
from featurebyte.storage import Storage


class ObservationSetHelper:
    """
    Observation set helper class
    """

    def __init__(self, observation_table_service: ObservationTableService, temp_storage: Storage):
        self.observation_table_service = observation_table_service
        self.temp_storage = temp_storage

    async def get_observation_set(
        self,
        observation_table_id: Optional[PydanticObjectId],
        observation_set_storage_path: Optional[str],
    ) -> Union[pd.DataFrame, ObservationTableModel]:
        """
        Get an ObservationTableModel or in-memory Dataframe.

        Parameters
        ----------
        observation_table_id: Optional[PydanticObjectId]
            ObservationTable ID
        observation_set_storage_path: Optional[str]
            Observation set storage path

        Returns
        -------
        Union[pd.DataFrame, ObservationTableModel]
        """
        if observation_table_id is not None:
            # ObservationTable as observation set
            assert observation_set_storage_path is None
            observation_table_service: ObservationTableService = self.observation_table_service
            return await observation_table_service.get_document(observation_table_id)

        # In-memory DataFrame as observation set
        assert observation_set_storage_path is not None
        return await self.temp_storage.get_dataframe(Path(observation_set_storage_path))
