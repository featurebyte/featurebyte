"""
Snowflake Feature Manager class
"""
from __future__ import annotations

from typing import Any, List, Optional

import json

import pandas as pd
from jinja2 import Template
from pydantic import BaseModel, PrivateAttr

from featurebyte.config import Credentials
from featurebyte.core.generic import ExtendedDatabaseSourceModel
from featurebyte.logger import logger
from featurebyte.models.event_data import TileSpec
from featurebyte.session.base import BaseSession
from featurebyte.tile.snowflake_tile import TileSnowflake

tm_ins_feature_registry = Template(
    """
    INSERT INTO FEATURE_REGISTRY
    SELECT
        '{{feature.name}}' as NAME, '{{feature.version}}' as VERSION, True as IS_DEFAULT, '{{feature.status.value}}' as STATUS,
        {{feature.frequency_minute}} as FREQUENCY_MINUTES, {{feature.time_modulo_frequency_second}} as TIME_MODULO_FREQUENCY_SECOND,
        {{feature.blind_spot_second}} as BLIND_SPOT_SECOND, '{{feature.tile_sql}}' as TILE_SQL,
        '{{feature.column_names}}' as COLUMN_NAMES, parse_json('{{tile_ids_str}}') as TILE_IDS,
        False as ONLINE_ENABLED, SYSDATE() as CREATED_AT
"""
)

tm_update_tile_ids = Template(
    """
    UPDATE FEATURE_REGISTRY SET TILE_IDS = parse_json('{{tile_ids_str}}') WHERE NAME = '{{feature.name}}' AND VERSION = '{{feature.version}}'
"""
)

tm_last_tile_index = Template(
    """
    SELECT
        t_reg.TILE_ID, t_reg.LAST_TILE_INDEX_ONLINE, t_reg.LAST_TILE_INDEX_OFFLINE
    FROM
        (SELECT value as TILE_ID FROM FEATURE_REGISTRY, LATERAL FLATTEN(input => TILE_IDS)
        WHERE NAME = '{{feature.name}}' AND VERSION = '{{feature.version}}') t_id, TILE_REGISTRY t_reg
    WHERE t_reg.TILE_ID = t_id.TILE_ID
"""
)


class FeatureSnowflake(BaseModel):
    """
    Snowflake Feature Manager class

    Parameters
    ----------
    feature: TileSpec
        feature instance
    credentials: Credentials
        credentials to the datasource
    """

    feature: TileSpec
    credentials: Credentials
    _session: BaseSession = PrivateAttr()

    def __init__(self, **kw: Any) -> None:
        """
        Custom constructor for TileSnowflake to instantiate a datasource session with credentials

        Parameters
        ----------
        kw: Any
            constructor arguments
        """
        super().__init__(**kw)
        data_source = ExtendedDatabaseSourceModel(**self.feature.datasource.dict())
        self._session = data_source.get_session(credentials=self.credentials)

    def insert_feature_registry(self) -> None:
        """
        Insert feature registry record. Update the is_default of the existing feature registry records to be False,
        then insert the new registry record with is_default to True
        """
        feature_versions = self.retrieve_features(version=self.feature.version)
        logger.debug(f"feature_versions: {feature_versions}")
        if len(feature_versions) == 0:
            self._session.execute_query(
                f"UPDATE FEATURE_REGISTRY SET IS_DEFAULT = False WHERE NAME = '{self.feature.name}'"  # nosec
            )
            logger.debug("Done updating is_default of other versions to false")
            tm_sql = tm_ins_feature_registry
        else:
            existing_tile_ids = set(feature_versions[0].tile_ids)
            existing_tile_ids.update(self.feature.tile_ids)
            self.feature.tile_ids = list(existing_tile_ids)
            logger.debug(f"Done updating tile_ids: {existing_tile_ids}")
            tm_sql = tm_update_tile_ids

        tile_ids_str = str(self.feature.tile_ids).replace("'", '"').upper()
        sql = tm_sql.render(feature=self.feature, tile_ids_str=tile_ids_str)
        logger.debug(f"generated sql: {sql}")
        self._session.execute_query(sql)

    def retrieve_features(self, version: Optional[str] = None) -> List[TileSpec]:
        """
        Retrieve Feature instances. If version parameter is not presented, return all the feature versions

        Parameters
        ----------
        version: str
            version of Feature
        Returns
        -------
            list of Feature instances
        """
        sql = f"SELECT * FROM FEATURE_REGISTRY WHERE NAME = '{self.feature.name}'"  # nosec
        if version:
            sql += f" AND VERSION = '{version}'"

        dataframe = self._session.execute_query(sql)
        result = []
        if dataframe is not None:
            for _, row in dataframe.iterrows():
                tile_ids = json.loads(row["TILE_IDS"]) if row["TILE_IDS"] else []
                feature_version = TileSpec(
                    name=self.feature.name,
                    version=row["VERSION"],
                    status=row["STATUS"],
                    is_default=row["IS_DEFAULT"],
                    time_modulo_frequency_second=row["TIME_MODULO_FREQUENCY_SECOND"],
                    blind_spot_second=row["BLIND_SPOT_SECOND"],
                    frequency_minute=row["FREQUENCY_MINUTES"],
                    tile_sql=row["TILE_SQL"],
                    column_names=row["COLUMN_NAMES"],
                    tile_ids=tile_ids,
                    online_enabled=row["ONLINE_ENABLED"],
                    datasource=self.feature.datasource,
                )
                result.append(feature_version)

        return result

    def online_enable(self) -> None:
        """
        Schedule both online and offline tile jobs

        Parameters
        ----------
        """
        for tile_id in self.feature.tile_ids:
            logger.info(f"tile_id: {tile_id}")
            tile_mgr = TileSnowflake(
                feature_name=self.feature.name,
                time_modulo_frequency_seconds=self.feature.time_modulo_frequency_second,
                blind_spot_seconds=self.feature.blind_spot_second,
                frequency_minute=self.feature.frequency_minute,
                tile_sql=self.feature.tile_sql,
                column_names=self.feature.column_names,
                tile_id=tile_id,
                tabular_source=self.feature.datasource,
                credentials=self.credentials,
            )
            # insert tile_registry record
            tile_mgr.insert_tile_registry()
            logger.debug(f"Done insert_tile_registry for {tile_id}")

            # enable online tiles scheduled job
            tile_mgr.schedule_online_tiles()
            logger.debug(f"Done schedule_online_tiles for {tile_id}")

            # enable offline tiles scheduled job
            tile_mgr.schedule_offline_tiles()
            logger.debug(f"Done schedule_offline_tiles for {tile_id}")

    def get_last_tile_index(self) -> pd.DataFrame:
        """
        Get last_tile_index of all the tile_ids as dataframe

        Returns
        -------
            last_tile_index of all the tile_ids as dataframe
        """
        sql = tm_last_tile_index.render(feature=self.feature)
        logger.debug(f"generated sql: {sql}")
        result = self._session.execute_query(sql)
        return result
