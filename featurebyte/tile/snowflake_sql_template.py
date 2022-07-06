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
        COLUMN_NAMES,
        FREQUENCY_MINUTE,
        TIME_MODULO_FREQUENCY_SECOND,
        BLIND_SPOT_SECOND
    ) VALUES (
        '{{tile_id}}',
        '{{tile_sql}}',
        '{{column_names}}',
        {{frequency_minute}},
        {{time_modulo_frequency_seconds}},
        {{blind_spot_seconds}}
    )
"""
)

tm_generate_tile = Template(
    """
    call SP_TILE_GENERATE(
        '{{tile_sql}}',
        '{{tile_start_date_column}}',
        {{time_modulo_frequency_seconds}},
        {{blind_spot_seconds}},
        {{frequency_minute}},
        '{{column_names}}',
        '{{tile_id}}',
        '{{tile_type.value}}',
        '{{last_tile_start_ts_str}}'
    )
"""
)

tm_schedule_tile = Template(
    """
    CREATE OR REPLACE TASK {{temp_task_name}}
      WAREHOUSE = {{warehouse}}
      SCHEDULE = 'USING CRON {{cron}} UTC'
    AS
        call SP_TILE_TRIGGER_GENERATE_SCHEDULE(
            '{{temp_task_name}}',
            '{{warehouse}}',
            '{{tile_id}}',
            {{time_modulo_frequency_seconds}},
            {{blind_spot_seconds}},
            {{frequency_minute}},
            {{offline_minutes}},
            '{{sql}}',
            '{{tile_start_date_column}}',
            '{{tile_start_placeholder}}',
            '{{tile_end_placeholder}}',
            '{{column_names}}',
            '{{tile_type.value}}',
            {{monitor_periods}}
        )
"""
)
