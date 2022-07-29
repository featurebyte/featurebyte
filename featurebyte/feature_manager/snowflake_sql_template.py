"""
Feature SQL template
"""
from jinja2 import Template

tm_insert_feature_registry = Template(
    """
    INSERT INTO FEATURE_REGISTRY (
        NAME,
        VERSION,
        DESCRIPTION,
        READINESS,
        TILE_SPECS,
        EVENT_DATA_IDS
    )
    SELECT
        '{{feature.name}}' as NAME,
        '{{feature.version}}' as VERSION,
        '{{feature.description}}' as DESCRIPTION,
        '{{feature.readiness}}' as READINESS,
        parse_json('{{tile_specs_str}}') as TILE_SPECS,
        '{{event_ids_str}}' as EVENT_DATA_IDS
"""
)

tm_remove_feature_registry = Template(
    """
    DELETE FROM FEATURE_REGISTRY WHERE NAME = '{{feature.name}}' AND VERSION = '{{feature.version}}'
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
        DESCRIPTION = '{{feature.description}}',
        IS_DEFAULT = {{feature.is_default}},
        ONLINE_ENABLED = {{online_enabled}}
    WHERE NAME = '{{feature.name}}'
    AND VERSION = '{{feature.version}}'
"""
)


tm_select_feature_registry = Template(
    """
    SELECT * FROM FEATURE_REGISTRY WHERE NAME = '{{feature_name}}'
    {% if version is not none %}
        AND VERSION = '{{version}}'
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
            WHERE NAME = '{{feature.name}}' AND VERSION = '{{feature.version}}'
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
        DESCRIPTION,
        READINESS,
        STATUS,
        FEATURE_VERSIONS
    )
    SELECT
        '{{feature_list.name}}' as NAME,
        '{{feature_list.version}}' as VERSION,
        '{{feature_list.description}}' as DESCRIPTION,
        '{{feature_list.readiness}}' as READINESS,
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
        READINESS = '{{feature_list.readiness}}',
        STATUS = '{{feature_list.status}}',
        DESCRIPTION = '{{feature_list.description}}'
    WHERE NAME = '{{feature_list.name}}'
    AND VERSION = '{{feature_list.version}}'
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
            f.DESCRIPTION,
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
