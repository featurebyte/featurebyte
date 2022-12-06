"""
Feature SQL template
"""
from jinja2 import Template

tm_last_tile_index = Template(
    """
    SELECT
        t_reg.TILE_ID,
        t_reg.LAST_TILE_INDEX_ONLINE,
        t_reg.LAST_TILE_INDEX_OFFLINE
    FROM TILE_REGISTRY t_reg, TILE_FEATURE_MAPPING t_mapping
    WHERE t_reg.TILE_ID = t_mapping.TILE_ID
    AND t_mapping.FEATURE_NAME = '{{feature.name}}'
    AND t_mapping.FEATURE_VERSION = '{{feature.version.to_str()}}'
"""
)

tm_feature_tile_monitor = Template(
    """
    SELECT
        f.FEATURE_NAME AS NAME,
        f.FEATURE_VERSION AS VERSION,
        f.FEATURE_READINESS AS READINESS,
        f.FEATURE_EVENT_DATA_IDS AS EVENT_DATA_IDS,
        t.TILE_ID,
        t.TIME_MODULO_FREQUENCY_SECOND AS TILE_MOD_FREQUENCY,
        t.BLIND_SPOT_SECOND AS TILE_BLIND_SPOT,
        t.FREQUENCY_MINUTE AS TILE_FREQUENCY,
        t_m.TILE_START_DATE,
        t_m.TILE_TYPE,
        t_m.CREATED_AT as TILE_MONITOR_DATE
    FROM TILE_FEATURE_MAPPING f, TILE_REGISTRY t, TILE_MONITOR_SUMMARY t_m
    WHERE f.TILE_ID = t.TILE_ID
    AND f.TILE_ID = t_m.TILE_ID
    AND t_m.CREATED_AT >= '{{query_start_ts}}'
    AND t_m.CREATED_AT <= '{{query_end_ts}}'
"""
)


tm_upsert_tile_feature_mapping = Template(
    """
    MERGE INTO TILE_FEATURE_MAPPING a
    USING (
        SELECT
            '{{tile_id}}' AS TILE_ID,
            '{{feature_name}}' as FEATURE_NAME,
            '{{feature_type}}' as FEATURE_TYPE,
            '{{feature_version}}' as FEATURE_VERSION,
            '{{feature_readiness}}' as FEATURE_READINESS,
            '{{feature_event_data_ids}}' as FEATURE_EVENT_DATA_IDS,
            '{{feature_sql}}' as FEATURE_SQL,
            '{{feature_store_table_name}}' as FEATURE_STORE_TABLE_NAME,
            '{{entity_column_names_str}}' as FEATURE_ENTITY_COLUMN_NAMES,
            {{is_deleted}} as IS_DELETED
    ) b
    ON
        a.TILE_ID = b.TILE_ID
        AND a.FEATURE_NAME = b.FEATURE_NAME
        AND a.FEATURE_VERSION = b.FEATURE_VERSION
    WHEN MATCHED THEN
        UPDATE SET a.IS_DELETED = b.IS_DELETED
    WHEN NOT MATCHED THEN
        INSERT (
            TILE_ID,
            FEATURE_NAME,
            FEATURE_TYPE,
            FEATURE_VERSION,
            FEATURE_READINESS,
            FEATURE_EVENT_DATA_IDS,
            FEATURE_SQL,
            FEATURE_STORE_TABLE_NAME,
            FEATURE_ENTITY_COLUMN_NAMES,
            IS_DELETED
        ) VALUES (
            b.TILE_ID,
            b.FEATURE_NAME,
            b.FEATURE_TYPE,
            b.FEATURE_VERSION,
            b.FEATURE_READINESS,
            b.FEATURE_EVENT_DATA_IDS,
            b.FEATURE_SQL,
            b.FEATURE_STORE_TABLE_NAME,
            b.FEATURE_ENTITY_COLUMN_NAMES,
            b.IS_DELETED
        )
"""
)

tm_delete_tile_feature_mapping = Template(
    """
    UPDATE TILE_FEATURE_MAPPING SET IS_DELETED = TRUE
    WHERE TILE_ID = '{{tile_id}}'
    AND FEATURE_NAME = '{{feature_name}}'
    AND FEATURE_VERSION = '{{feature_version}}'
"""
)
