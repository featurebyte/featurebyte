"""
Exposure API object
"""

from __future__ import annotations

from typing import Any, ClassVar, Dict, List, Optional, Sequence, Union, cast

import pandas as pd
from bson import ObjectId
from pydantic import BaseModel, Field, model_validator
from typeguard import typechecked

from featurebyte.api.api_object_util import ForeignKeyMapping
from featurebyte.api.entity import Entity
from featurebyte.api.exposure_namespace import ExposureNamespace
from featurebyte.api.feature_or_target_mixin import FeatureOrTargetMixin
from featurebyte.api.feature_store import FeatureStore
from featurebyte.api.observation_table import ObservationTable
from featurebyte.api.savable_api_object import DeletableApiObject, SavableApiObject
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
from featurebyte.core.series import Series
from featurebyte.models.exposure import ExposureModel
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.schema.exposure import ExposureCreate
from featurebyte.schema.target_table import TargetTableCreate

DOCSTRING_FORMAT_PARAMS = {"class_name": "Exposure"}


class Exposure(
    Series,
    DeletableApiObject,
    SavableApiObject,
    FeatureOrTargetMixin,
):
    """
    Exposure class used to represent an Exposure in FeatureByte.

    An Exposure is an optional companion to a Target that provides a time offset
    or adjustment value. When associated with a Use Case, Exposures are computed
    alongside the Target during target computation.
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(proxy_class="featurebyte.Exposure")
    _route = "/exposure"
    _list_schema = ExposureModel
    _get_schema = ExposureModel
    _list_fields = ["name", "entities"]
    _list_foreign_keys = [
        ForeignKeyMapping("entity_ids", Entity, "entities"),
    ]

    # pydantic instance variable (public)
    feature_store: FeatureStoreModel = Field(
        exclude=True,
        frozen=True,
        description="Provides information about the feature store that the exposure is connected to.",
    )

    def _get_create_payload(self) -> dict[str, Any]:
        data = ExposureCreate(**self.model_dump(by_alias=True))
        return data.json_dict()

    def _get_init_params_from_object(self) -> dict[str, Any]:
        return {"feature_store": self.feature_store}

    def validate_series_operation(self, other_series: Series) -> bool:
        """
        Validate that the other series is an Exposure.

        Parameters
        ----------
        other_series: Series
            The other series to validate.

        Returns
        -------
        bool
        """
        return isinstance(other_series, Exposure)

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
            >>> exposure = catalog.get_exposure("my_exposure")  # doctest: +SKIP
            >>> exposure.version  # doctest: +SKIP
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
            >>> exposure = catalog.get_exposure("my_exposure")  # doctest: +SKIP
            >>> new_exposure = exposure.isnull()  # doctest: +SKIP
            """
        ),
    )
    def isnull(self) -> Exposure:
        return super().isnull()

    @substitute_docstring(
        doc_template=NOTNULL_DOC,
        format_kwargs=DOCSTRING_FORMAT_PARAMS,
        examples=(
            """
            >>> exposure = catalog.get_exposure("my_exposure")  # doctest: +SKIP
            >>> new_exposure = exposure.notnull()  # doctest: +SKIP
            """
        ),
    )
    def notnull(self) -> Exposure:
        return super().notnull()

    @property
    def window(self) -> Optional[str]:
        """
        Returns the window of this exposure.

        Returns
        -------
        Optional[str]

        Raises
        ------
        ValueError
            If the exposure does not have a window.
        """
        window = self.exposure_namespace.window
        if window is None:
            raise ValueError("Exposure does not have a window")
        return window

    @property
    @substitute_docstring(
        doc_template=DEFINITION_DOC,
        examples=(
            """
            >>> exposure = catalog.get_exposure("my_exposure")  # doctest: +SKIP
            >>> exposure_definition = exposure.definition  # doctest: +SKIP
            """
        ),
        format_kwargs={"object_type": "exposure"},
    )
    def definition(self) -> str:
        return self._generate_definition()

    @substitute_docstring(
        doc_template=PREVIEW_DOC,
        description="Materializes an Exposure object using a small observation set of up to 50 rows.",
        format_kwargs={"object_type": "exposure"},
    )
    @enforce_observation_set_row_order
    @typechecked
    def preview(
        self,
        observation_set: Union[ObservationTable, pd.DataFrame],
    ) -> pd.DataFrame:
        return self._preview(observation_set=observation_set, url="/target/preview")

    @enforce_observation_set_row_order
    @typechecked
    def compute_exposures(
        self,
        observation_table: Union[ObservationTable, pd.DataFrame],
        serving_names_mapping: Optional[Dict[str, str]] = None,
        skip_entity_validation_checks: bool = False,
        context_id: Optional[ObjectId] = None,
    ) -> pd.DataFrame:
        """
        Returns a DataFrame with exposure values computed against the observation set.

        Parameters
        ----------
        observation_table : Union[ObservationTable, pd.DataFrame]
            Observation set DataFrame or ObservationTable object with `POINT_IN_TIME` and
            serving-name columns for the exposure's primary entity (or its descendants).
        serving_names_mapping : Optional[Dict[str, str]]
            Optional serving names mapping.
        skip_entity_validation_checks: bool
            Whether to skip entity validation checks.
        context_id: Optional[ObjectId]
            Optional context id whose forecast point schema should be used when the
            observation set is a DataFrame.

        Returns
        -------
        pd.DataFrame
            Materialized exposure values.

        Examples
        --------
        >>> exposure = catalog.get_exposure("my_exposure")  # doctest: +SKIP
        >>> exposure.compute_exposures(observation_table)  # doctest: +SKIP
        """
        temp_table_name = f"__TEMPORARY_EXPOSURE_TABLE_{ObjectId()}"
        temp_table = self.compute_exposure_table(
            observation_table=observation_table,
            observation_table_name=temp_table_name,
            serving_names_mapping=serving_names_mapping,
            skip_entity_validation_checks=skip_entity_validation_checks,
            context_id=context_id,
        )
        try:
            return temp_table.to_pandas()
        finally:
            temp_table.delete()

    @typechecked
    def compute_exposure_table(
        self,
        observation_table: Union[ObservationTable, pd.DataFrame],
        observation_table_name: str,
        serving_names_mapping: Optional[Dict[str, str]] = None,
        skip_entity_validation_checks: bool = False,
        context_id: Optional[ObjectId] = None,
    ) -> ObservationTable:
        """
        Materialize the exposure into a new observation table.

        Parameters
        ----------
        observation_table: Union[ObservationTable, pd.DataFrame]
            Observation set.
        observation_table_name: str
            Name of the observation table to be created with the exposure values.
        serving_names_mapping : Optional[Dict[str, str]]
            Optional serving names mapping.
        skip_entity_validation_checks: bool
            Whether to skip entity validation checks.
        context_id: Optional[ObjectId]
            Optional context id for DataFrame observation sets.

        Returns
        -------
        ObservationTable

        Examples
        --------
        >>> exposure = catalog.get_exposure("my_exposure")  # doctest: +SKIP
        >>> exposure.compute_exposure_table(  # doctest: +SKIP
        ...     observation_table, "exposure_table"
        ... )
        """
        is_input_observation_table = isinstance(observation_table, ObservationTable)
        observation_table_id = observation_table.id if is_input_observation_table else None

        # Exposure shares the same graph structure as Target so we reuse the target_table
        # infrastructure by passing graph and node_names directly (no target_id).
        graph = self.graph
        node_names = [self.node.name]

        if is_input_observation_table:
            resolved_context_id = observation_table.context_id
        else:
            resolved_context_id = context_id

        target_table_create_params = TargetTableCreate(
            name=observation_table_name,
            observation_table_id=observation_table_id,
            feature_store_id=self.feature_store.id,
            serving_names_mapping=serving_names_mapping,
            graph=graph,
            node_names=node_names,
            context_id=resolved_context_id,
            skip_entity_validation_checks=skip_entity_validation_checks,
            target_id=None,
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

    @typechecked
    def info(self, verbose: bool = False) -> Dict[str, Any]:
        """
        Returns a dictionary that summarizes the essential information of an Exposure object.

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
        >>> exposure = catalog.get_exposure("my_exposure")  # doctest: +SKIP
        >>> info = exposure.info()  # doctest: +SKIP
        """
        return super().info(verbose)

    @classmethod
    def list(
        cls,
        include_id: Optional[bool] = False,
    ) -> pd.DataFrame:
        """
        List saved exposures.

        Parameters
        ----------
        include_id: Optional[bool]
            Whether to include id in the list

        Returns
        -------
        pd.DataFrame
            Table of exposures
        """
        return ExposureNamespace.list(include_id=include_id)

    @property
    def exposure_namespace(self) -> ExposureNamespace:
        """
        ExposureNamespace object of current exposure

        Returns
        -------
        ExposureNamespace
        """
        exposure_namespace_id = cast(ExposureModel, self.cached_model).exposure_namespace_id
        return ExposureNamespace.get_by_id(id=exposure_namespace_id)

    @typechecked
    def update_description(self, description: Optional[str]) -> None:
        """
        Update exposure description

        Parameters
        ----------
        description: Optional[str]
            Description of exposure
        """
        self.exposure_namespace.update_description(description=description)

    @typechecked
    def update_version_description(self, description: Optional[str]) -> None:
        """
        Update exposure version description

        Parameters
        ----------
        description: Optional[str]
            Description of exposure version
        """
        super().update_description(description=description)

    def delete(self) -> None:
        """
        Delete an exposure from the persistent data store. An exposure can only be deleted from the
        persistent data store if

        - the exposure is not used in any use case

        Examples
        --------
        >>> exposure = catalog.get_exposure("my_exposure")  # doctest: +SKIP
        >>> exposure.delete()  # doctest: +SKIP
        """
        super()._delete()
