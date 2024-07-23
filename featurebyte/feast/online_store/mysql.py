"""
Custom MySQL online store implementation for Feast.
"""

from typing import Literal, Sequence

from feast import Entity, FeatureView, RepoConfig
from feast.infra.online_stores.contrib.mysql_online_store.mysql import (
    MySQLOnlineStore as BaseMySQLOnlineStore,
)
from feast.infra.online_stores.contrib.mysql_online_store.mysql import (
    MySQLOnlineStoreConfig as BaseMySQLOnlineStoreConfig,
)
from feast.infra.online_stores.contrib.mysql_online_store.mysql import (
    _drop_table_and_index,
    _table_id,
)


class FBMySQLOnlineStoreConfig(BaseMySQLOnlineStoreConfig):
    """Configuration for the MySQL online store"""

    type: Literal["featurebyte.feast.online_store.mysql.FBMySQLOnlineStore"] = (
        "featurebyte.feast.online_store.mysql.FBMySQLOnlineStore"  # type: ignore
    )


class FBMySQLOnlineStore(BaseMySQLOnlineStore):
    """An online store implementation that uses MySQL."""

    def update(
        self,
        config: RepoConfig,
        tables_to_delete: Sequence[FeatureView],
        tables_to_keep: Sequence[FeatureView],
        entities_to_delete: Sequence[Entity],
        entities_to_keep: Sequence[Entity],
        partial: bool,
    ) -> None:
        conn = self._get_conn(config)
        cur = conn.cursor()
        project = config.project

        # We don't create any special state for the entities in this implementation.
        for table in tables_to_keep:
            table_name = _table_id(project, table)
            index_name = f"{table_name}_ek"
            cur.execute(
                f"""CREATE TABLE IF NOT EXISTS {table_name} (entity_key VARCHAR(512),
                feature_name VARCHAR(256),
                value BLOB,
                event_ts timestamp NULL DEFAULT NULL,
                created_ts timestamp NULL DEFAULT NULL,
                PRIMARY KEY(entity_key, feature_name))"""
            )
            index_exists = cur.execute(
                f"""
                SELECT 1 FROM information_schema.statistics
                WHERE table_schema = DATABASE() AND table_name = '{table_name}' AND index_name = '{index_name}'
                """
            )
            if not index_exists:
                cur.execute(f"ALTER TABLE {table_name} ADD INDEX {index_name} (entity_key);")

        for table in tables_to_delete:
            _drop_table_and_index(cur, project, table)
