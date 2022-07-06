"""
Feature SQL template
"""
from jinja2 import Template

tm_insert_feature_registry = Template(
    """
    INSERT INTO FEATURE_REGISTRY (NAME, VERSION, READINESS, TILE_SPECS)
    SELECT
        '{{feature.name}}' as NAME, '{{feature.version}}' as VERSION,
        '{{feature.readiness.value}}' as READINESS, parse_json('{{tile_specs_str}}') as TILE_SPECS
"""
)

tm_update_feature_registry = Template(
    """
    UPDATE FEATURE_REGISTRY SET IS_DEFAULT = {{is_default}} WHERE NAME = '{{feature_name}}'
"""
)

tm_select_feature_registry = Template(
    """
    SELECT * FROM FEATURE_REGISTRY WHERE NAME = '{{feature_name}}'
"""
)

tm_last_tile_index = Template(
    """
    SELECT
        t_reg.TILE_ID, t_reg.LAST_TILE_INDEX_ONLINE, t_reg.LAST_TILE_INDEX_OFFLINE
    FROM
        (SELECT value:tile_id as TILE_ID FROM FEATURE_REGISTRY, LATERAL FLATTEN(input => TILE_SPECS)
        WHERE NAME = '{{feature.name}}' AND VERSION = '{{feature.version}}') t_spec, TILE_REGISTRY t_reg
    WHERE t_reg.TILE_ID = t_spec.TILE_ID
"""
)
