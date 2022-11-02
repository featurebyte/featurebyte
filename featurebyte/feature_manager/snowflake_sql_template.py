"""
Feature SQL template
"""
from jinja2 import Template

tm_insert_feature_registry = Template(
    """
    INSERT INTO FEATURE_REGISTRY (
        NAME,
        VERSION,
        READINESS,
        TILE_SPECS,
        EVENT_DATA_IDS
    )
    SELECT
        '{{feature.name}}' as NAME,
        '{{feature.version.to_str()}}' as VERSION,
        '{{feature.readiness}}' as READINESS,
        parse_json('{{tile_specs_str}}') as TILE_SPECS,
        '{{event_ids_str}}' as EVENT_DATA_IDS
"""
)

tm_remove_feature_registry = Template(
    """
    DELETE FROM FEATURE_REGISTRY WHERE NAME = '{{feature.name}}' AND VERSION = '{{feature.version.to_str()}}'
"""
)

tm_update_feature_registry_default_false = Template(
    """
    UPDATE FEATURE_REGISTRY SET IS_DEFAULT = False WHERE NAME = '{{feature.name}}'
"""
)

tm_update_feature_registry = Template(
    """
    UPDATE FEATURE_REGISTRY
    SET
        READINESS = '{{feature.readiness}}',
        IS_DEFAULT = {{feature.is_default}},
        ONLINE_ENABLED = {{online_enabled}}
    WHERE NAME = '{{feature.name}}'
    AND VERSION = '{{feature.version.to_str()}}'
"""
)


tm_select_feature_registry = Template(
    """
    SELECT * FROM FEATURE_REGISTRY WHERE NAME = '{{feature_name}}'
    {% if version is not none %}
        AND VERSION = '{{version.to_str()}}'
    {% endif %}
"""
)

tm_last_tile_index = Template(
    """
    SELECT
        t_reg.TILE_ID,
        t_reg.LAST_TILE_INDEX_ONLINE,
        t_reg.LAST_TILE_INDEX_OFFLINE
    FROM
        (
            SELECT value:tile_id as TILE_ID FROM FEATURE_REGISTRY, LATERAL FLATTEN(input => TILE_SPECS)
            WHERE NAME = '{{feature.name}}' AND VERSION = '{{feature.version.to_str()}}'
        ) t_spec,
        TILE_REGISTRY t_reg
    WHERE t_reg.TILE_ID = t_spec.TILE_ID
"""
)

tm_insert_feature_list_registry = Template(
    """
    INSERT INTO FEATURE_LIST_REGISTRY (
        NAME,
        VERSION,
        STATUS,
        FEATURE_VERSIONS
    )
    SELECT
        '{{feature_list.name}}' as NAME,
        '{{feature_list.version.to_str()}}' as VERSION,
        '{{feature_list.status}}' as STATUS,
        parse_json('{{feature_lst_str}}') as FEATURE_VERSIONS
"""
)

tm_select_feature_list_registry = Template(
    """
    SELECT * FROM FEATURE_LIST_REGISTRY WHERE NAME = '{{feature_list_name}}'
"""
)

tm_update_feature_list_registry = Template(
    """
    UPDATE FEATURE_LIST_REGISTRY
    SET
        STATUS = '{{feature_list.status}}'
    WHERE NAME = '{{feature_list.name}}'
    AND VERSION = '{{feature_list.version.to_str()}}'
"""
)

tm_feature_tile_monitor = Template(
    """
    SELECT f_t.*, t_m.TILE_START_DATE, t_m.TILE_TYPE, t_m.CREATED_AT as TILE_MONITOR_DATE
    FROM
    (
        SELECT
            f.NAME,
            f.VERSION,
            f.READINESS,
            f.EVENT_DATA_IDS,
            CAST(t.value:tile_id AS VARCHAR) as TILE_ID,
            CAST(t.value:time_modulo_frequency_second AS INT) as TILE_MOD_FREQUENCY,
            CAST(t.value:blind_spot_second AS INT) as TILE_BLIND_SPOT,
            CAST(t.value:frequency_minute AS INT) as TILE_FREQUENCY
        FROM FEATURE_REGISTRY f, LATERAL FLATTEN(INPUT => TILE_SPECS) t
    ) f_t, TILE_MONITOR_SUMMARY t_m
    WHERE f_t.TILE_ID = t_m.TILE_ID
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
            '{{feature_spec.feature_name}}' as FEATURE_NAME,
            '{{feature_spec.feature_version}}' as FEATURE_VERSION,
            '{{feature_spec.feature_sql}}' as FEATURE_SQL,
            '{{feature_spec.feature_store_table_name}}' as FEATURE_STORE_TABLE_NAME,
            '{{entity_column_names_str}}' as FEATURE_ENTITY_COLUMN_NAMES
    ) b
    ON
        a.TILE_ID = b.TILE_ID
        AND a.FEATURE_NAME = b.FEATURE_NAME
        AND a.FEATURE_VERSION = b.FEATURE_VERSION
    WHEN NOT MATCHED THEN
        INSERT (
            TILE_ID,
            FEATURE_NAME,
            FEATURE_VERSION,
            FEATURE_SQL,
            FEATURE_STORE_TABLE_NAME,
            FEATURE_ENTITY_COLUMN_NAMES
        ) VALUES (
            b.TILE_ID,
            b.FEATURE_NAME,
            b.FEATURE_VERSION,
            b.FEATURE_SQL,
            b.FEATURE_STORE_TABLE_NAME,
            b.FEATURE_ENTITY_COLUMN_NAMES
        )
"""
)
