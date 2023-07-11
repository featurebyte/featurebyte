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

ENTITY_IDS_DOC = """
Returns the entity IDs associated with the {class_name} object.

Returns
-------
Sequence[ObjectId]
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
