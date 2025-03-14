"""
Target API object
"""

from __future__ import annotations

from typing import Any, ClassVar, Dict, List, Optional, Sequence, Union, cast

import pandas as pd
from bson import ObjectId
from pydantic import BaseModel, Field, model_validator
from typeguard import typechecked

from featurebyte.api.api_object_util import ForeignKeyMapping
from featurebyte.api.entity import Entity
from featurebyte.api.feature_or_target_mixin import FeatureOrTargetMixin
from featurebyte.api.feature_store import FeatureStore
from featurebyte.api.observation_table import ObservationTable
from featurebyte.api.savable_api_object import DeletableApiObject, SavableApiObject
from featurebyte.api.target_namespace import TargetNamespace
from featurebyte.api.templates.doc_util import substitute_docstring
from featurebyte.api.templates.entity_doc import (
    ENTITY_DOC,
    ENTITY_IDS_DOC,
    PRIMARY_ENTITY_DOC,
    PRIMARY_ENTITY_IDS_DOC,
)
from featurebyte.api.templates.feature_or_target_doc import (
    CATALOG_ID_DOC,
    DEFINITION_DOC,
    PREVIEW_DOC,
    TABLE_IDS_DOC,
    VERSION_DOC,
)
from featurebyte.api.templates.series_doc import ISNULL_DOC, NOTNULL_DOC
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.common.utils import dataframe_to_arrow_bytes, enforce_observation_set_row_order
from featurebyte.core.accessor.target_datetime import TargetDtAccessorMixin
from featurebyte.core.accessor.target_string import TargetStrAccessorMixin
from featurebyte.core.series import Series
from featurebyte.enum import TargetType
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.models.target import TargetModel
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.schema.target import TargetCreate
from featurebyte.schema.target_table import TargetTableCreate

DOCSTRING_FORMAT_PARAMS = {"class_name": "Target"}


class Target(
    Series,
    DeletableApiObject,
    SavableApiObject,
    FeatureOrTargetMixin,
    TargetDtAccessorMixin,
    TargetStrAccessorMixin,
):
    """
    Target class used to represent a Target in FeatureByte.
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(proxy_class="featurebyte.Target")
    _route = "/target"
    _list_schema = TargetModel
    _get_schema = TargetModel
    _list_fields = ["name", "entities"]
    _list_foreign_keys = [
        ForeignKeyMapping("entity_ids", Entity, "entities"),
    ]

    # pydantic instance variable (public)
    feature_store: FeatureStoreModel = Field(
        exclude=True,
        frozen=True,
        description="Provides information about the feature store that the target is connected to.",
    )
    # this is used to store the target_type before the target is saved
    internal_target_type: Optional[TargetType] = Field(default=None, alias="target_type")

    def _get_create_payload(self) -> dict[str, Any]:
        data = TargetCreate(**self.model_dump(by_alias=True))
        return data.json_dict()

    def _get_init_params_from_object(self) -> dict[str, Any]:
        return {"feature_store": self.feature_store}

    def validate_series_operation(self, other_series: Series) -> bool:
        """
        Validate that the other series is a Target.

        Parameters
        ----------
        other_series: Series
            The other series to validate.

        Returns
        -------
        bool
        """
        return isinstance(other_series, Target)

    @model_validator(mode="before")
    @classmethod
    def _set_feature_store(cls, values: Any) -> Any:
        if isinstance(values, BaseModel):
            values = values.model_dump(by_alias=True)

        if "feature_store" not in values:
            tabular_source = values.get("tabular_source")
            if isinstance(tabular_source, dict):
                feature_store_id = TabularSource(**tabular_source).feature_store_id
                values["feature_store"] = FeatureStore.get_by_id(id=feature_store_id)
        return values

    @property
    @substitute_docstring(
        doc_template=VERSION_DOC,
        examples=(
            """
            >>> target = catalog.get_feature("CustomerProductGroupCounts_7d")  # doctest: +SKIP
            >>> target.version  # doctest: +SKIP
            'V230323'
            """
        ),
        format_kwargs=DOCSTRING_FORMAT_PARAMS,
    )
    def version(self) -> str:
        return self._get_version()

    @property
    @substitute_docstring(doc_template=CATALOG_ID_DOC, format_kwargs=DOCSTRING_FORMAT_PARAMS)
    def catalog_id(self) -> ObjectId:
        return self._get_catalog_id()

    @property
    @substitute_docstring(doc_template=ENTITY_IDS_DOC, format_kwargs=DOCSTRING_FORMAT_PARAMS)
    def entity_ids(self) -> Sequence[ObjectId]:
        return self._get_entity_ids()

    @property
    @substitute_docstring(
        doc_template=PRIMARY_ENTITY_IDS_DOC, format_kwargs=DOCSTRING_FORMAT_PARAMS
    )
    def primary_entity_ids(
        self,
    ) -> Sequence[ObjectId]:
        return self._get_primary_entity_ids()

    @property
    @substitute_docstring(doc_template=ENTITY_DOC, format_kwargs=DOCSTRING_FORMAT_PARAMS)
    def entities(self) -> List[Entity]:
        return self._get_entities()

    @property
    @substitute_docstring(doc_template=PRIMARY_ENTITY_DOC, format_kwargs=DOCSTRING_FORMAT_PARAMS)
    def primary_entity(self) -> List[Entity]:
        return self._get_primary_entity()

    @property
    @substitute_docstring(doc_template=TABLE_IDS_DOC, format_kwargs=DOCSTRING_FORMAT_PARAMS)
    def table_ids(self) -> Sequence[ObjectId]:
        return self._get_table_ids()

    @substitute_docstring(
        doc_template=ISNULL_DOC,
        format_kwargs=DOCSTRING_FORMAT_PARAMS,
        examples=(
            """
            >>> target = catalog.get_target("target_latest_invoice_timestamp")
            >>> new_target = target.isnull()
            """
        ),
    )
    def isnull(self) -> Target:
        return super().isnull()

    @substitute_docstring(
        doc_template=NOTNULL_DOC,
        format_kwargs=DOCSTRING_FORMAT_PARAMS,
        examples=(
            """
            >>> target = catalog.get_target("target_latest_invoice_timestamp")
            >>> new_target = target.notnull()
            """
        ),
    )
    def notnull(self) -> Target:
        return super().notnull()

    @property
    def window(self) -> Optional[str]:
        """
        Returns the window of this target.

        Returns
        -------
        Optional[str]

        Raises
        ------
        ValueError
            If the target does not have a window.
        """
        window = self.target_namespace.window
        if window is None:
            raise ValueError("Target does not have a window")
        return window

    @property
    @substitute_docstring(
        doc_template=DEFINITION_DOC,
        examples=(
            """
            >>> target = catalog.get_target("InvoiceCount_60days")  # doctest: +SKIP
            >>> target_definition = target.definition  # doctest: +SKIP
            """
        ),
        format_kwargs={"object_type": "target"},
    )
    def definition(self) -> str:
        return self._generate_definition()

    @substitute_docstring(
        doc_template=PREVIEW_DOC,
        description="Materializes a Target object using a small observation set of up to 50 rows.",
        format_kwargs={"object_type": "target"},
    )
    @enforce_observation_set_row_order
    @typechecked
    def preview(
        self,
        observation_set: Union[ObservationTable, pd.DataFrame],
    ) -> pd.DataFrame:
        return self._preview(observation_set=observation_set, url="/target/preview")

    @typechecked
    def info(self, verbose: bool = False) -> Dict[str, Any]:
        """
        Returns a dictionary that summarizes the essential information of an Target object. The dictionary
        contains the following keys:

        - `name`: The name of the FeatureList object.
        - `created_at`: The timestamp indicating when the FeatureList object was created.
        - `updated_at`: The timestamp indicating when the FeatureList object was last updated.
        - `entities`: List of entities involved in the computation of the features contained in the Target object.
        - `window`: Window of the Target object.
        - `has_recipe`: Whether the Target object has a recipe attached.
        - `default_version_mode`: Indicates whether the default version mode is 'auto' or 'manual'.
        - `input_data`: Data used to create the target.
        - `metadata`: Summary of key operations.

        Parameters
        ----------
        verbose: bool
            Control verbose level of the summary.

        Returns
        -------
        Dict[str, Any]
            Key-value mapping of properties of the object.

        Examples
        --------
        >>> target = catalog.get_target("target")  # doctest: +SKIP
        >>> info = target.info()  # doctest: +SKIP
        """
        return super().info(verbose)

    @enforce_observation_set_row_order
    @typechecked
    def compute_targets(
        self,
        observation_table: Union[ObservationTable, pd.DataFrame],
        serving_names_mapping: Optional[Dict[str, str]] = None,
        skip_entity_validation_checks: bool = False,
    ) -> pd.DataFrame:
        """
        Returns a DataFrame with target values for analysis, model training, or evaluation. The target
        request data consists of an observation set that combines points-in-time and key values of the
        primary entity from the target.

        Associated serving entities can also be utilized.

        Parameters
        ----------
        observation_table : Union[ObservationTable, pd.DataFrame]
            Observation set DataFrame or ObservationTable object, which combines points-in-time and values
            of the target primary entity or its descendant (serving entities). The column containing the point-in-time
            values should be named `POINT_IN_TIME`, while the columns representing entity values should be named using
            accepted serving names for the entity.
        serving_names_mapping : Optional[Dict[str, str]]
            Optional serving names mapping if the training events table has different serving name columns than those
            defined in Entities, mapping from original serving name to new name.
        skip_entity_validation_checks: bool
            Whether to skip entity validation checks.

        Returns
        -------
        pd.DataFrame
            Materialized target.

            **Note**: `POINT_IN_TIME` values will be converted to UTC time.

        Examples
        --------
        >>> target = catalog.get_target("target")  # doctest: +SKIP
        >>> target.compute_targets(observation_table)  # doctest: +SKIP
        """
        temp_target_table_name = f"__TEMPORARY_TARGET_TABLE_{ObjectId()}"
        temp_target_table = self.compute_target_table(
            observation_table=observation_table,
            observation_table_name=temp_target_table_name,
            serving_names_mapping=serving_names_mapping,
            skip_entity_validation_checks=skip_entity_validation_checks,
        )
        try:
            return temp_target_table.to_pandas()
        finally:
            temp_target_table.delete()

    @typechecked
    def compute_target_table(
        self,
        observation_table: Union[ObservationTable, pd.DataFrame],
        observation_table_name: str,
        serving_names_mapping: Optional[Dict[str, str]] = None,
        skip_entity_validation_checks: bool = False,
    ) -> ObservationTable:
        """
        Materialize feature list using an observation table asynchronously. The targets
        will be materialized into a target table.

        Parameters
        ----------
        observation_table: Union[ObservationTable, pd.DataFrame]
            Observation set with `POINT_IN_TIME` and serving names columns. This can be either an
            ObservationTable of a pandas DataFrame.
        observation_table_name: str
            Name of the observation table to be created with the target values
        serving_names_mapping : Optional[Dict[str, str]]
            Optional serving names mapping if the training events table has different serving name.
        skip_entity_validation_checks: bool
            Whether to skip entity validation checks

        Returns
        -------
        ObservationTable

        Examples
        --------
        >>> target = catalog.get_target("target")  # doctest: +SKIP
        >>> target.compute_target_table(observation_table, "target_table")  # doctest: +SKIP
        """
        is_input_observation_table = isinstance(observation_table, ObservationTable)
        observation_table_id = observation_table.id if is_input_observation_table else None

        if self.saved:
            target_id = self.id
            graph = None
            node_names = None
        else:
            target_id = None
            graph = self.graph
            node_names = [self.node.name]

        target_table_create_params = TargetTableCreate(
            name=observation_table_name,
            observation_table_id=observation_table_id,
            feature_store_id=self.feature_store.id,
            serving_names_mapping=serving_names_mapping,
            graph=graph,
            node_names=node_names,
            context_id=observation_table.context_id if is_input_observation_table else None,
            skip_entity_validation_checks=skip_entity_validation_checks,
            target_id=target_id,
        )
        if is_input_observation_table:
            files = None
        else:
            assert isinstance(observation_table, pd.DataFrame)
            files = {"observation_set": dataframe_to_arrow_bytes(observation_table)}
        observation_table_doc = self.post_async_task(
            route="/target_table",
            payload={"payload": target_table_create_params.model_dump_json()},
            is_payload_json=False,
            files=files,
        )
        return ObservationTable.get_by_id(observation_table_doc["_id"])

    @classmethod
    def list(
        cls,
        include_id: Optional[bool] = False,
    ) -> pd.DataFrame:
        """
        List saved targets.

        Parameters
        ----------
        include_id: Optional[bool]
            Whether to include id in the list

        Returns
        -------
        pd.DataFrame
            Table of targets
        """
        return TargetNamespace.list(include_id=include_id)

    @property
    def target_namespace(self) -> TargetNamespace:
        """
        TargetNamespace object of current target

        Returns
        -------
        TargetNamespace
        """
        target_namespace_id = cast(TargetModel, self.cached_model).target_namespace_id
        return TargetNamespace.get_by_id(id=target_namespace_id)

    @property
    def target_type(self) -> Optional[TargetType]:
        """
        Target type

        Returns
        -------
        Optional[str]
            Target type
        """
        if self.saved:
            return self.target_namespace.target_type
        return self.internal_target_type

    @typechecked
    def update_description(self, description: Optional[str]) -> None:
        """
        Update target description

        Parameters
        ----------
        description: Optional[str]
            Description of target
        """
        self.target_namespace.update_description(description=description)

    @typechecked
    def update_version_description(self, description: Optional[str]) -> None:
        """
        Update target version description

        Parameters
        ----------
        description: Optional[str]
            Description of target version
        """
        super().update_description(description=description)

    @typechecked
    def update_target_type(self, target_type: Union[TargetType, str]) -> None:
        """
        Update target type of target.

        A target type can be one of the following:

        The target type determines the nature of the prediction task and must be one of the following:

        1. **REGRESSION** - The target variable is continuous, predicting numerical values.
        2. **CLASSIFICATION** - The target variable has two possible categorical outcomes (binary classification).
        3. **MULTI_CLASSIFICATION** - The target variable has more than two possible categorical outcomes.

        Parameters
        ----------
        target_type: Union[TargetType, str]
            Type of the Target used to indicate the modeling type of the target

        Examples
        --------
        >>> target = catalog.get_target("InvoiceCount_60days")  # doctest: +SKIP
        >>> target.update_target_type("REGRESSION")  # doctest: +SKIP
        """
        value = TargetType(target_type)
        if self.saved:
            self.target_namespace.update_target_type(target_type=value)
        else:
            self.internal_target_type = value

    def delete(self) -> None:
        """
        Delete a target from the persistent data store. A target can only be deleted from the persistent
        data store if

        - the target is not used in any use case
        - the target is not used in any observation table


        Examples
        --------
        >>> target = catalog.get_target("target_latest_invoice_timestamp")
        >>> target.delete()  # doctest: +SKIP
        """
        super()._delete()
