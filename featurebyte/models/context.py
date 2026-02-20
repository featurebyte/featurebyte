"""
This module contains context related models.
"""

from typing import Any, ClassVar, List, Optional

import pymongo
from pydantic import BaseModel, Field, StrictStr, field_validator, model_validator

from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.common.validator import construct_sort_validator
from featurebyte.enum import DBVarType, FeatureType
from featurebyte.models.base import (
    FeatureByteBaseModel,
    FeatureByteCatalogBaseDocumentModel,
    PydanticObjectId,
    UniqueConstraintResolutionSignature,
    UniqueValuesConstraint,
)
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.model.forecast_point_schema import ForecastPointSchema


class UserProvidedColumn(FeatureByteBaseModel):
    """
    Defines a user-provided column that will be supplied in observation tables during feature
    materialization. These columns can be accessed as Features through the Context.

    User-provided columns allow you to incorporate external data (such as customer-provided
    information or real-time inputs) into your feature engineering workflow without requiring
    them to be stored in source tables.

    Parameters
    ----------
    name : str
        The name of the column. This must match the column name that will be provided
        in the observation table during materialization.
    dtype : DBVarType
        The data type of the column (e.g., ``DBVarType.FLOAT``, ``DBVarType.INT``,
        ``DBVarType.VARCHAR``).
    feature_type : FeatureType
        The semantic type of the feature (e.g., ``FeatureType.NUMERIC``,
        ``FeatureType.CATEGORICAL``, ``FeatureType.TIMESTAMP``).
    description : str, optional
        A description of the column for documentation purposes.

    Examples
    --------
    Create a context with user-provided columns:

    >>> context = fb.Context.create(
    ...     name="loan_application_context",
    ...     primary_entity=["customer"],
    ...     user_provided_columns=[
    ...         fb.UserProvidedColumn(
    ...             name="annual_income",
    ...             dtype=fb.DBVarType.FLOAT,
    ...             feature_type=fb.FeatureType.NUMERIC,
    ...             description="Customer's self-reported annual income",
    ...         ),
    ...         fb.UserProvidedColumn(
    ...             name="employment_status",
    ...             dtype=fb.DBVarType.VARCHAR,
    ...             feature_type=fb.FeatureType.CATEGORICAL,
    ...         ),
    ...     ],
    ... )

    Access user-provided columns as features:

    >>> income_feature = context.get_user_provided_feature("annual_income")

    See Also
    --------
    - [Context.create](/reference/featurebyte.api.context.Context.create/):
        Create a Context with user-provided columns.
    - [Context.add_user_provided_column](/reference/featurebyte.api.context.Context.add_user_provided_column/):
        Add a user-provided column to an existing Context.
    - [Context.get_user_provided_feature](/reference/featurebyte.api.context.Context.get_user_provided_feature/):
        Get a Feature from a user-provided column.
    """

    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(proxy_class="featurebyte.UserProvidedColumn")

    name: StrictStr
    dtype: DBVarType
    feature_type: FeatureType
    description: Optional[StrictStr] = Field(default=None)


class ContextModel(FeatureByteCatalogBaseDocumentModel):
    """
    Context is used to define the circumstances in which features are expected to be materialized.

    primary_entity_ids: List[PydanticObjectId]
        List of entity ids associated with this context
    treatment_id: Optional[PydanticObjectId]
        Treatment_id if it is causal modeling context
    graph: Optional[QueryGraph]
        Graph to store the context view
    """

    # TODO: make graph attribute lazy

    primary_entity_ids: List[PydanticObjectId]
    treatment_id: Optional[PydanticObjectId] = Field(default=None)
    forecast_point_schema: Optional[ForecastPointSchema] = Field(default=None)
    graph: Optional[QueryGraph] = Field(default=None)
    node_name: Optional[str] = Field(default=None)

    default_preview_table_id: Optional[PydanticObjectId] = Field(default=None)
    default_eda_table_id: Optional[PydanticObjectId] = Field(default=None)

    # User-provided columns that will be available in observation tables for this context
    user_provided_columns: List[UserProvidedColumn] = Field(default_factory=list)

    # pydantic validators
    _sort_ids_validator = field_validator("primary_entity_ids")(construct_sort_validator())

    @model_validator(mode="before")
    @classmethod
    def _set_primary_entity_ids(cls, values: Any) -> Any:
        if isinstance(values, BaseModel):
            values = values.model_dump(by_alias=True)

        entity_ids = values.get("entity_ids", None)
        primary_entity_ids = values.get("primary_entity_ids", None)
        if entity_ids and not primary_entity_ids:
            values["primary_entity_ids"] = entity_ids
        return values

    class Settings(FeatureByteCatalogBaseDocumentModel.Settings):
        """
        MongoDB Settings
        """

        collection_name: str = "context"
        unique_constraints: List[UniqueValuesConstraint] = [
            UniqueValuesConstraint(
                fields=("_id",),
                conflict_fields_signature={"id": ["_id"]},
                resolution_signature=None,
            ),
            UniqueValuesConstraint(
                fields=("name",),
                conflict_fields_signature={"name": ["name"]},
                resolution_signature=UniqueConstraintResolutionSignature.GET_NAME,
            ),
        ]

        indexes = FeatureByteCatalogBaseDocumentModel.Settings.indexes + [
            pymongo.operations.IndexModel("treatment_id"),
            [
                ("name", pymongo.TEXT),
                ("description", pymongo.TEXT),
            ],
        ]
