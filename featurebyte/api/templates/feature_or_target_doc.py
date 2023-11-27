"""
This module contains the docstring template used by the Feature or Target API.
"""

VERSION_DOC = """
Returns the version identifier of a {class_name} object.

Returns
-------
str

Examples
--------
{examples}
"""

CATALOG_ID_DOC = """
Returns the catalog ID that is associated with the {class_name} object.

Returns
-------
ObjectId
Catalog ID of the table.

See Also
--------
- [Catalog](/reference/featurebyte.api.catalog.Catalog)
"""

TABLE_IDS_DOC = """
Returns the table IDs used by the {class_name} object.

Returns
-------
Sequence[ObjectId]
"""

DEFINITION_DOC = """
Displays the {object_type} definition file of the {object_type}.

The file is the single source of truth for a {object_type} version. The file is generated automatically after a
{object_type} is declared in the SDK and is stored in the FeatureByte Service.

This file uses the same SDK syntax as the {object_type} declaration and provides an explicit outline of the intended
operations of the {object_type} declaration, including those that are inherited but not explicitly declared by the
user. These operations may include feature job settings and cleaning operations inherited from tables metadata.

The {object_type} definition file serves as the basis for generating the final logical execution graph, which is
then transpiled into platform-specific SQL (e.g. SnowSQL, SparkSQL) for {object_type} materialization.

Returns
-------
str

Examples
--------
{examples}
"""

PREVIEW_DOC = """
{description}

The small observation set should combine historical points-in-time and key values of the primary entity from
the {object_type}. Associated serving entities can also be utilized.

Parameters
----------
observation_set : pd.DataFrame
    Observation set DataFrame which combines historical points-in-time and values of the {object_type} primary entity
    or its descendant (serving entities). The column containing the point-in-time values should be named
    `POINT_IN_TIME`, while the columns representing entity values should be named using accepted serving
    names for the entity.

Returns
-------
pd.DataFrame
    Materialized {object_type} values.
    The returned DataFrame will have the same number of rows, and include all columns from the observation set.

    **Note**: `POINT_IN_TIME` values will be converted to UTC time.

{examples}

{see_also}
"""
