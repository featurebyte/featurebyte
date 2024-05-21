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
