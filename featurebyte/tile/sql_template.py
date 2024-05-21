"""
Tile SQL Jinjia template
"""

from jinja2 import Template

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
