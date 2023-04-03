"""
Tile SQL Jinjia template
"""
from jinja2 import Template

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
        {{last_tile_start_ts_str}},
        '{{aggregation_id}}'
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
