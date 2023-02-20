"""
Tile SQL Jinjia template
"""
from jinja2 import Template

tm_select_tile_registry = Template(
    """
    SELECT * FROM TILE_REGISTRY WHERE TILE_ID = '{{tile_id}}'
"""
)

tm_update_tile_registry = Template(
    """
    UPDATE TILE_REGISTRY SET IS_ENABLED = {{is_enabled}} WHERE TILE_ID = '{{tile_id}}'
"""
)

tm_insert_tile_registry = Template(
    """
    INSERT INTO TILE_REGISTRY (
        TILE_ID,
        TILE_SQL,
        ENTITY_COLUMN_NAMES,
        VALUE_COLUMN_NAMES,
        VALUE_COLUMN_TYPES,
        FREQUENCY_MINUTE,
        TIME_MODULO_FREQUENCY_SECOND,
        BLIND_SPOT_SECOND
    ) VALUES (
        '{{tile_id}}',
        '{{tile_sql}}',
        '{{entity_column_names}}',
        '{{value_column_names}}',
        '{{value_column_types}}',
        {{frequency_minute}},
        {{time_modulo_frequency_second}},
        {{blind_spot_second}}
    )
"""
)

tm_generate_tile = Template(
    """
    call SP_TILE_GENERATE(
        '{{tile_sql}}',
        '{{tile_start_date_column}}',
        '{{tile_last_start_date_column}}',
        {{time_modulo_frequency_second}},
        {{blind_spot_second}},
        {{frequency_minute}},
        '{{entity_column_names}}',
        '{{value_column_names}}',
        '{{value_column_types}}',
        '{{tile_id}}',
        '{{tile_type.value}}',
        {{last_tile_start_ts_str}}
    )
"""
)

tm_tile_entity_tracking = Template(
    """
    call SP_TILE_GENERATE_ENTITY_TRACKING(
        '{{tile_id}}',
        '{{entity_column_names}}',
        '{{entity_table}}',
        '{{tile_last_start_date_column}}'
    )
"""
)

tm_shell_schedule_tile = Template(
    """
    CREATE OR REPLACE TASK {{temp_task_name}}
      WAREHOUSE = {{warehouse}}
      SCHEDULE = 'USING CRON {{cron}} UTC'
    AS
        call SP_TILE_TRIGGER_GENERATE_SCHEDULE(
            '{{temp_task_name}}',
            '{{warehouse}}',
            '{{tile_id}}',
            '{{aggregation_id}}',
            {{time_modulo_frequency_second}},
            {{blind_spot_second}},
            {{frequency_minute}},
            {{offline_minutes}},
            '{{tile_sql}}',
            '{{tile_start_date_column}}',
            '{{tile_last_start_date_column}}',
            '{{tile_start_placeholder}}',
            '{{tile_end_placeholder}}',
            '{{entity_column_names}}',
            '{{value_column_names}}',
            '{{value_column_types}}',
            '{{tile_type.value}}',
            {{monitor_periods}}
        )
"""
)

tm_schedule_tile = Template(
    """
    call SP_TILE_GENERATE_SCHEDULE(
        '{{tile_id}}',
        '{{aggregation_id}}',
        {{time_modulo_frequency_second}},
        {{blind_spot_second}},
        {{frequency_minute}},
        {{offline_minutes}},
        '{{tile_sql}}',
        '{{tile_start_date_column}}',
        '{{tile_last_start_date_column}}',
        '{{tile_start_placeholder}}',
        '{{tile_end_placeholder}}',
        '{{entity_column_names}}',
        '{{value_column_names}}',
        '{{value_column_types}}',
        '{{tile_type.value}}',
        {{monitor_periods}},
        to_varchar(SYSDATE(), 'YYYY-MM-DD"T"HH24:MI:SS.ff3"Z"')
    )
"""
)

tm_retrieve_tile_job_audit_logs = Template(
    """
    SELECT * FROM TILE_JOB_MONITOR
    WHERE CREATED_AT >= '{{start_date_str}}'
    {% if end_date_str is defined and end_date_str %}
        AND CREATED_AT <= '{{end_date_str}}'
    {% endif %}
    {% if tile_id is defined and tile_id %}
        AND TILE_ID = '{{tile_id}}'
    {% endif %}
"""
)
