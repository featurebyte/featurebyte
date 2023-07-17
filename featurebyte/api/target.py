"""
Target API object
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence, Union

import pandas as pd
from bson import ObjectId
from pydantic import Field, root_validator
from typeguard import typechecked

from featurebyte.api.api_object_util import ForeignKeyMapping
from featurebyte.api.entity import Entity
from featurebyte.api.feature_or_target_mixin import FeatureOrTargetMixin
from featurebyte.api.feature_store import FeatureStore
from featurebyte.api.observation_table import ObservationTable
from featurebyte.api.savable_api_object import SavableApiObject
from featurebyte.api.target_table import TargetTable
from featurebyte.api.templates.doc_util import substitute_docstring
from featurebyte.api.templates.feature_or_target_doc import (
    CATALOG_ID_DOC,
    DEFINITION_DOC,
    ENTITY_IDS_DOC,
    PREVIEW_DOC,
    TABLE_IDS_DOC,
    VERSION_DOC,
)
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.common.utils import dataframe_to_arrow_bytes, enforce_observation_set_row_order
from featurebyte.core.series import Series
from featurebyte.exception import RecordRetrievalException
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.models.target import TargetModel
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.schema.target import TargetUpdate
from featurebyte.schema.target_table import TargetTableCreate


class Target(Series, SavableApiObject, FeatureOrTargetMixin):
    """
    Target class used to represent a Target in FeatureByte.
    """

    __fbautodoc__ = FBAutoDoc(proxy_class="featurebyte.Target")

    # pydantic instance variable (public)
    feature_store: FeatureStoreModel = Field(
        exclude=True,
        allow_mutation=False,
        description="Provides information about the feature store that the target is connected to.",
    )

    # class variables
    _route = "/target"
    _update_schema_class = TargetUpdate
    _list_schema = TargetModel
    _get_schema = TargetModel
    _list_fields = ["name", "entities"]
    _list_foreign_keys = [
        ForeignKeyMapping("entity_ids", Entity, "entities"),
    ]

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

    @root_validator(pre=True)
    @classmethod
    def _set_feature_store(cls, values: dict[str, Any]) -> dict[str, Any]:
        if "feature_store" not in values:
            tabular_source = values.get("tabular_source")
            if isinstance(tabular_source, dict):
                feature_store_id = TabularSource(**tabular_source).feature_store_id
                values["feature_store"] = FeatureStore.get_by_id(id=feature_store_id)
        return values

    @property  # type: ignore
    @substitute_docstring(
        doc_template=VERSION_DOC,
        examples=(
            """
            >>> target = catalog.get_feature("CustomerProductGroupCounts_7d")  # doctest: +SKIP
            >>> target.version  # doctest: +SKIP
            'V230323'
            """
        ),
        format_kwargs={"class_name": "Feature"},
    )
    def version(self) -> str:  # pylint: disable=missing-function-docstring
        return self._get_version()

    @property  # type: ignore
    @substitute_docstring(doc_template=CATALOG_ID_DOC, format_kwargs={"class_name": "Target"})
    def catalog_id(self) -> ObjectId:  # pylint: disable=missing-function-docstring
        return self._get_catalog_id()

    @property  # type: ignore
    @substitute_docstring(doc_template=ENTITY_IDS_DOC, format_kwargs={"class_name": "Target"})
    def entity_ids(self) -> Sequence[ObjectId]:  # pylint: disable=missing-function-docstring
        return self._get_entity_ids()

    @property  # type: ignore
    @substitute_docstring(doc_template=TABLE_IDS_DOC, format_kwargs={"class_name": "Target"})
    def table_ids(self) -> Sequence[ObjectId]:  # pylint: disable=missing-function-docstring
        return self._get_table_ids()

    @property
    def entities(self) -> List[Entity]:
        """
        Returns a list of entities associated with this target.

        Returns
        -------
        List[Entity]
        """
        return [Entity.get_by_id(entity_id) for entity_id in self.entity_ids]

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
        try:
            # TODO: Should use window value from TargetNamespace once it is available
            window = self.cached_model.graph.get_forward_aggregate_window(self.node_name)
        except RecordRetrievalException:
            window = self.graph.get_forward_aggregate_window(self.node_name)
        if window is None:
            raise ValueError("Target does not have a window")
        return window

    @property  # type: ignore
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
    def definition(self) -> str:  # pylint: disable=missing-function-docstring
        return self._generate_definition()

    @substitute_docstring(
        doc_template=PREVIEW_DOC,
        description="Materializes a Target object using a small observation set of up to 50 rows.",
        format_kwargs={"object_type": "target"},
    )
    @enforce_observation_set_row_order
    @typechecked
    def preview(  # pylint: disable=missing-function-docstring
        self,
        observation_set: pd.DataFrame,
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
    def compute_target(
        self,
        observation_set: pd.DataFrame,
        serving_names_mapping: Optional[Dict[str, str]] = None,
    ) -> pd.DataFrame:
        """
        Returns a DataFrame with target values for analysis, model training, or evaluation. The target
        request data consists of an observation set that combines points-in-time and key values of the
        primary entity from the target.

        Associated serving entities can also be utilized.

        Parameters
        ----------
        observation_set : pd.DataFrame
            Observation set DataFrame or ObservationTable object, which combines points-in-time and values
            of the target primary entity or its descendant (serving entities). The column containing the point-in-time
            values should be named `POINT_IN_TIME`, while the columns representing entity values should be named using
            accepted serving names for the entity.
        serving_names_mapping : Optional[Dict[str, str]]
            Optional serving names mapping if the training events table has different serving name columns than those
            defined in Entities, mapping from original serving name to new name.

        Returns
        -------
        pd.DataFrame
            Materialized target.

            **Note**: `POINT_IN_TIME` values will be converted to UTC time.
        """
        temp_target_table_name = f"__TEMPORARY_TARGET_TABLE_{ObjectId()}"
        temp_target_table = self.compute_target_table(
            observation_table=observation_set,
            target_table_name=temp_target_table_name,
            serving_names_mapping=serving_names_mapping,
        )
        try:
            return temp_target_table.to_pandas()
        finally:
            temp_target_table.delete()

    @typechecked
    def compute_target_table(
        self,
        observation_table: Union[ObservationTable, pd.DataFrame],
        target_table_name: str,
        serving_names_mapping: Optional[Dict[str, str]] = None,
    ) -> TargetTable:
        """
        Materialize feature list using an observation table asynchronously. The targets
        will be materialized into a target table.

        Parameters
        ----------
        observation_table: Union[ObservationTable, pd.DataFrame]
            Observation set with `POINT_IN_TIME` and serving names columns. This can be either an
            ObservationTable of a pandas DataFrame.
        target_table_name: str
            Name of the target table to be created
        serving_names_mapping : Optional[Dict[str, str]]
            Optional serving names mapping if the training events table has different serving name

        Returns
        -------
        TargetTable
        """
        target_table_create_params = TargetTableCreate(
            name=target_table_name,
            observation_table_id=(
                observation_table.id if isinstance(observation_table, ObservationTable) else None
            ),
            feature_store_id=self.feature_store.id,
            serving_names_mapping=serving_names_mapping,
            target_id=self.id,
            graph=self.graph,
            node_names=[self.node.name],
        )
        if isinstance(observation_table, ObservationTable):
            files = None
        else:
            assert isinstance(observation_table, pd.DataFrame)
            files = {"observation_set": dataframe_to_arrow_bytes(observation_table)}
        target_table_doc = self.post_async_task(
            route="/target_table",
            payload={"payload": target_table_create_params.json()},
            is_payload_json=False,
            files=files,
        )
        return TargetTable.get_by_id(target_table_doc["_id"])
