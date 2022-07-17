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
        TILE_SPECS
    )
    SELECT
        '{{feature.name}}' as NAME,
        '{{feature.version}}' as VERSION,
        '{{feature.description}}' as DESCRIPTION,
        '{{feature.readiness}}' as READINESS,
        parse_json('{{tile_specs_str}}') as TILE_SPECS
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
        ONLINE_ENABLED = {{feature.online_enabled}}
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
