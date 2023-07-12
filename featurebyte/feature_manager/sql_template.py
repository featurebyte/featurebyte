"""
Feature SQL template
"""
from jinja2 import Template

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

tm_upsert_online_store_mapping = Template(
    """
    MERGE INTO ONLINE_STORE_MAPPING a
    USING (
        SELECT
            '{{tile_id}}' AS TILE_ID,
            '{{aggregation_id}}' as AGGREGATION_ID,
            '{{result_id}}' as RESULT_ID,
            '{{result_type}}' as RESULT_TYPE,
            '{{sql_query}}' as SQL_QUERY,
            '{{online_store_table_name}}' as ONLINE_STORE_TABLE_NAME,
            '{{entity_column_names}}' as ENTITY_COLUMN_NAMES
    ) b
    ON
        a.RESULT_ID = b.RESULT_ID
    WHEN NOT MATCHED THEN
        INSERT (
            TILE_ID,
            AGGREGATION_ID,
            RESULT_ID,
            RESULT_TYPE,
            SQL_QUERY,
            ONLINE_STORE_TABLE_NAME,
            ENTITY_COLUMN_NAMES
        ) VALUES (
            b.TILE_ID,
            b.AGGREGATION_ID,
            b.RESULT_ID,
            b.RESULT_TYPE,
            b.SQL_QUERY,
            b.ONLINE_STORE_TABLE_NAME,
            b.ENTITY_COLUMN_NAMES
        )
"""
)

tm_delete_online_store_mapping = Template(
    """
    DELETE FROM ONLINE_STORE_MAPPING
    WHERE RESULT_ID = '{{result_id}}'
"""
)
