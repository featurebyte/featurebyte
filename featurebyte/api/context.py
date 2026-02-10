"""
Context module
"""

from typing import Any, ClassVar, Dict, List, Optional

import pandas as pd
from bson import ObjectId
from pandas import DataFrame
from typeguard import typechecked

from featurebyte.api.entity import Entity
from featurebyte.api.observation_table import ObservationTable
from featurebyte.api.request_column import RequestColumn
from featurebyte.api.savable_api_object import SavableApiObject
from featurebyte.api.treatment import Treatment
from featurebyte.api.use_case_or_context_mixin import UseCaseOrContextMixin
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.enum import ConflictResolution, DBVarType, SpecialColumnName
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.context import ContextModel, UserProvidedColumn
from featurebyte.query_graph.model.dtype import DBVarTypeInfo, DBVarTypeMetadata
from featurebyte.query_graph.model.forecast_point_schema import ForecastPointSchema
from featurebyte.query_graph.model.timestamp_schema import TimestampSchema
from featurebyte.schema.context import ContextUpdate


class Context(SavableApiObject, UseCaseOrContextMixin):
    """
    Context class to represent a Context in FeatureByte.

    A context defines the circumstances in which features are expected to be served.
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(proxy_class="featurebyte.Context")
    _route: ClassVar[str] = "/context"
    _list_schema: ClassVar[Any] = ContextModel
    _get_schema: ClassVar[Any] = ContextModel
    _update_schema_class: ClassVar[Any] = ContextUpdate
    _list_fields: ClassVar[List[str]] = [
        "name",
        "primary_entity_ids",
        "description",
        "treatment_id",
        "user_provided_columns",
        "forecast_point_schema",
    ]

    # pydantic instance variable (public)
    primary_entity_ids: List[PydanticObjectId]
    treatment_id: Optional[PydanticObjectId] = None
    user_provided_columns: List[UserProvidedColumn] = []
    forecast_point_schema: Optional[ForecastPointSchema] = None

    @property
    def primary_entities(self) -> List[Entity]:
        """
        Returns the list of primary Entity objects from the context.

        Returns
        -------
        list[Entity]
            list of Primary Entity objects.

        Examples
        --------
        Get the primary enitity objects of the Context object:

        >>> context_1 = catalog.get_context("context_1")  # doctest: +SKIP
        >>> context_1.primary_entities  # doctest: +SKIP
        """
        entities = []
        for entity_id in self.primary_entity_ids:
            entities.append(Entity.get_by_id(entity_id))
        return entities

    def get_forecast_point_feature(self) -> Any:
        """
        Returns a Feature representing the FORECAST_POINT column with timezone info
        from this Context's forecast_point_schema.

        The returned RequestColumn will have the correct dtype and timezone information
        as defined in the ForecastPointSchema, enabling local time date part extraction.

        Returns
        -------
        Any
            A Feature for FORECAST_POINT with timezone metadata from the schema.

        Raises
        ------
        ValueError
            If this Context does not have a forecast_point_schema defined.

        Examples
        --------
        Get forecast point from a Context and extract month in local time:

        >>> context = catalog.get_context("daily_forecast_context")  # doctest: +SKIP
        >>> forecast_month = context.get_forecast_point_feature().dt.month  # doctest: +SKIP
        >>> forecast_month.name = "Forecast Month (Local)"  # doctest: +SKIP

        Calculate forecast horizon from Context's forecast point:

        >>> forecast_horizon = (  # doctest: +SKIP
        ...     context.get_forecast_point_feature() - fb.RequestColumn.point_in_time()
        ... ).dt.day
        >>> forecast_horizon.name = "Forecast Horizon Days"  # doctest: +SKIP
        """
        # Import here to avoid circular import
        from featurebyte.api.feature import Feature  # pylint: disable=import-outside-toplevel

        if self.forecast_point_schema is None:
            raise ValueError(
                f"Context '{self.name}' does not have a forecast_point_schema defined. "
                "Please create a Context with forecast_point_schema to use this property."
            )

        schema = self.forecast_point_schema

        # Build dtype_info with timestamp schema derived from forecast_point_schema
        timestamp_schema = TimestampSchema(
            is_utc_time=schema.is_utc_time if schema.is_utc_time is not None else False,
            timezone=schema.timezone,
            format_string=schema.format_string,
        )
        dtype_info = DBVarTypeInfo(
            dtype=schema.dtype,
            metadata=DBVarTypeMetadata(timestamp_schema=timestamp_schema),
        )

        request_col = RequestColumn._create_request_column(
            SpecialColumnName.FORECAST_POINT.value,
            schema.dtype,
            dtype_info=dtype_info,
            context_id=self.id,
        )
        return Feature._from_request_column(
            request_col,
            name=SpecialColumnName.FORECAST_POINT.value,
            context_id=self.id,
        )

    @classmethod
    @typechecked
    def create(
        cls,
        name: str,
        primary_entity: List[str],
        description: Optional[str] = None,
        treatment_name: Optional[str] = None,
        user_provided_columns: Optional[List[Dict[str, Any]]] = None,
        forecast_point_schema: Optional[ForecastPointSchema] = None,
    ) -> "Context":
        """
        Create a new Context.

        Parameters
        ----------
        name: str
            Name of the UseCase.
        primary_entity: List[str]
            List of entity names.
        description: Optional[str]
            Description of the Context.
        treatment_name: Optional[str]
            treatment name if this is a causal modeling context.
        user_provided_columns: Optional[List[Dict[str, Any]]]
            List of user-provided column definitions. Each column should have:
            - name: str - Name of the column
            - dtype: DBVarType - Data type of the column
            - description: Optional[str] - Description of the column
        forecast_point_schema: Optional[ForecastPointSchema]
            Schema for forecast point column if this is a forecasting context.
            Defines the granularity (day, week, etc.) and timezone handling.

        Returns
        -------
        Context
            The newly created Context.

        Examples
        --------
        >>> fb.Context.create(  # doctest: +SKIP
        ...     name="context_1",
        ...     primary_entity=primary_entity,
        ... )
        >>> context_1 = catalog.get_context("context_1")  # doctest: +SKIP

        >>> # Example with user-provided columns
        >>> fb.Context.create(  # doctest: +SKIP
        ...     name="loan_approval_context",
        ...     primary_entity=["customer"],
        ...     user_provided_columns=[
        ...         {"name": "annual_income", "dtype": fb.DBVarType.FLOAT},
        ...         {"name": "credit_score", "dtype": fb.DBVarType.INT},
        ...     ],
        ... )

        >>> # Example with Observational Treatment and Estimated Unit-Level Propensity

        >>> observational_treatment = fb.Treatment.create(  # doctest: +SKIP
        ...     name="Churn Campaign A/B test",
        ...     dtype=DBVarType.INT,
        ...     treatment_type=fb.TreatmentType.BINARY,
        ...     source="observational",
        ...     design="business-rule",
        ...     time="static",
        ...     time_structure="none",
        ...     interference="none",
        ...     treatment_labels=[0, 1],
        ...     control_label=0,
        ...     propensity=fb.Propensity(
        ...         granularity="unit",
        ...         knowledge="estimated",  # Requires model-based p(T|X)
        ...     ),
        ... )
        >>> fb.Context.create(  # doctest: +SKIP
        ...     name="context_with_observational_treatment",
        ...     primary_entity=primary_entity,
        ...     treatment_name=observational_treatment.name,
        ... )
        >>> context_2 = catalog.get_context(  # doctest: +SKIP
        ...     "context_with_observational_treatment"
        ... )

        >>> # Example with Forecast Point Schema for daily forecasting

        >>> forecast_schema = fb.ForecastPointSchema(  # doctest: +SKIP
        ...     granularity=fb.TimeIntervalUnit.DAY,
        ...     dtype=fb.DBVarType.DATE,
        ...     timezone="America/New_York",
        ... )
        >>> fb.Context.create(  # doctest: +SKIP
        ...     name="daily_forecast_context",
        ...     primary_entity=primary_entity,
        ...     forecast_point_schema=forecast_schema,
        ... )

        """
        entity_ids = []
        for entity_name in primary_entity:
            entity_ids.append(Entity.get(entity_name).id)

        treatment_id: Optional[PydanticObjectId] = None
        if treatment_name:
            treatment_id = Treatment.get(treatment_name).id

        # Convert user_provided_columns dicts to UserProvidedColumn objects
        user_provided_column_objs: List[UserProvidedColumn] = []
        if user_provided_columns:
            for col in user_provided_columns:
                user_provided_column_objs.append(UserProvidedColumn(**col))

        context = Context(
            name=name,
            primary_entity_ids=entity_ids,
            description=description,
            treatment_id=treatment_id,
            user_provided_columns=user_provided_column_objs,
            forecast_point_schema=forecast_point_schema,
        )
        context.save()
        return context

    @typechecked
    def add_observation_table(self, observation_table_name: str) -> None:
        """
        Add observation table for the UseCase or Context.

        Parameters
        ----------
        observation_table_name: str
            New observation table to be added.

        Examples
        --------
        >>> context = catalog.get_context("context")
        >>> context.add_observation_table(observation_table_name)  # doctest: +SKIP
        """
        observation_table = ObservationTable.get(observation_table_name)
        observation_table.update(update_payload={"context_id": self.id}, allow_update_local=False)

    @typechecked
    def info(self, verbose: bool = False) -> Dict[str, Any]:
        """
        Returns a dictionary that summarizes the essential information of a Context object. The dictionary
        contains the following keys:

        - `author`: The name of the user who created the Context object.
        - `name`: The name of the Context object.
        - `created_at`: The timestamp indicating when the Context object was created.
        - `updated_at`: The timestamp indicating when the Context object was last updated.
        - `primary_entities`: List of primary entities of the Context.
        - `description`: Description of the Context.
        - `treatment`: Treatment associated with the Context.
        - `default_eda_table`: Default eda table of the Context.
        - `default_preview_table`: Default preview table of the Context.
        - `associated_use_cases`: UseCases associated of the Context.

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
        >>> context = catalog.get_context("context")  # doctest: +SKIP
        >>> info = context.info()  # doctest: +SKIP
        """
        return super().info(verbose)

    @classmethod
    def get(cls, name: str) -> "Context":
        """
        Gets a Context object by its name.

        Parameters
        ----------
        name: str
            Name of the context to retrieve.

        Returns
        -------
        Context
            Context object.

        Examples
        --------
        Get a Context object that is already saved.

        >>> context = fb.Context.get("context")  # doctest: +SKIP
        """
        return super().get(name)

    @classmethod
    def list(cls, include_id: Optional[bool] = True) -> DataFrame:
        """
        Returns a DataFrame that lists the contexts by their names, types and creation dates.

        Parameters
        ----------
        include_id: Optional[bool]
            Whether to include id in the list.

        Returns
        -------
        DataFrame
            Table of objects.

        Examples
        --------
        List all contexts.

        >>> contexts = fb.Context.list()
        """
        return super().list(include_id=include_id)

    @property
    def default_eda_table(self) -> Optional[ObservationTable]:
        """
        Returns the EDA table of the Context.

        Returns
        -------
        ObservationTable
            The ObservationTable object of the Context.
        """
        return super().default_eda_table

    @property
    def default_preview_table(self) -> Optional[ObservationTable]:
        """
        Returns the preview table object of the Context.

        Returns
        -------
        ObservationTable
            The ObservationTable object of the Context.
        """
        return super().default_preview_table

    @typechecked
    def update_default_preview_table(self, observation_table_name: str) -> None:
        """
        Update default preview table for the Context.

        Parameters
        ----------
        observation_table_name: str
            Name of default preview table.

        Examples
        --------
        >>> context = catalog.get_context("context")
        >>> context.update_default_preview_table(observation_table_name)  # doctest: +SKIP
        """
        super().update_default_preview_table(observation_table_name)

    @typechecked
    def update_default_eda_table(self, observation_table_name: str) -> None:
        """
        Update default eda table for the Context.

        Parameters
        ----------
        observation_table_name: str
            Name of default eda table.

        Examples
        --------
        >>> context = catalog.get_context("context")
        >>> context.update_default_eda_table(observation_table_name)  # doctest: +SKIP
        """
        super().update_default_eda_table(observation_table_name)

    @typechecked
    def update_description(self, description: Optional[str]) -> None:
        """
        Update description for the Context.

        Parameters
        ----------
        description: Optional[str]
            Description of the object

        Examples
        --------
        >>> context = catalog.get_context("context")
        >>> context.update_description(description)  # doctest: +SKIP
        """
        super().update_description(description)

    @typechecked
    def add_user_provided_column(
        self,
        name: str,
        dtype: DBVarType,
        description: Optional[str] = None,
    ) -> None:
        """
        Add a new user-provided column to this context.

        User-provided columns define data that users will provide as part of the observation table
        during feature materialization. Once added, columns cannot be removed or have their dtype
        changed (though descriptions can be updated).

        Parameters
        ----------
        name: str
            Name of the column
        dtype: DBVarType
            Data type of the column
        description: Optional[str]
            Description of the column

        Raises
        ------
        ValueError
            If column with same name already exists

        Examples
        --------
        >>> context = catalog.get_context("loan_approval_context")  # doctest: +SKIP
        >>> context.add_user_provided_column(  # doctest: +SKIP
        ...     name="annual_income",
        ...     dtype=fb.DBVarType.FLOAT,
        ...     description="Customer's annual income",
        ... )
        """
        # Check if column name already exists
        existing_names = {col.name for col in self.user_provided_columns}
        if name in existing_names:
            raise ValueError(f"User-provided column with name '{name}' already exists")

        # Create updated list with new column
        new_column = UserProvidedColumn(name=name, dtype=dtype, description=description)
        updated_columns = list(self.user_provided_columns) + [new_column]

        # Update via API
        self.update(
            update_payload={"user_provided_columns": [col.model_dump() for col in updated_columns]},
            allow_update_local=True,
        )

    def _get_user_provided_column_info(self, column_name: str) -> Optional[UserProvidedColumn]:
        """
        Look up a user-provided column definition by name.

        Parameters
        ----------
        column_name: str
            Name of the column to look up

        Returns
        -------
        Optional[UserProvidedColumn]
            The column definition, or None if not found
        """
        for col in self.user_provided_columns:
            if col.name == column_name:
                return col
        return None

    @typechecked
    def get_user_provided_feature(
        self,
        column_name: str,
        feature_name: Optional[str] = None,
    ) -> Any:
        """
        Get a Feature object for a user-provided column defined in this context.

        The returned feature will have context_id set, linking it to this context.
        Features from different contexts that use the same column name can coexist
        without name conflicts.

        Parameters
        ----------
        column_name: str
            Name of the user-provided column (must be defined in context)
        feature_name: Optional[str]
            Name for the feature (defaults to column_name)

        Returns
        -------
        Any
            A Feature object representing the user-provided column

        Raises
        ------
        ValueError
            If column_name is not defined in context's user_provided_columns

        Examples
        --------
        >>> context = fb.Context.get("loan_approval_context")  # doctest: +SKIP
        >>> income_feature = context.get_user_provided_feature("annual_income")  # doctest: +SKIP
        >>> income_feature.save()  # doctest: +SKIP
        """
        # Import here to avoid circular import
        from featurebyte.api.feature import Feature  # pylint: disable=import-outside-toplevel

        col_info = self._get_user_provided_column_info(column_name)
        if col_info is None:
            available = [col.name for col in self.user_provided_columns]
            raise ValueError(
                f"Column '{column_name}' not defined in context user_provided_columns. "
                f"Available columns: {available}"
            )

        # Create RequestColumn internally with context_id for SDK code generation
        request_col = RequestColumn._create_request_column(
            column_name, col_info.dtype, context_id=self.id
        )

        # Convert to Feature with context_id set
        return Feature._from_request_column(
            request_col,
            name=feature_name or column_name,
            context_id=self.id,
        )

    @typechecked
    def remove_observation_table(self, observation_table_name: str) -> None:
        """
        Remove observation table from the Context.

        Parameters
        ----------
        observation_table_name: str
            Name of new observation table to be removed.

        Examples
        --------
        >>> context = catalog.get_context("context")
        >>> context.remove_observation_table(observation_table_name)  # doctest: +SKIP
        """
        super().remove_observation_table(observation_table_name)

    def remove_default_eda_table(self) -> None:
        """
        Remove observation table from the Context.

        Examples
        --------
        >>> context = catalog.get_context("context")
        >>> context.remove_default_eda_table()  # doctest: +SKIP
        """
        super().remove_default_eda_table()

    def remove_default_preview_table(self) -> None:
        """
        Remove observation table from the Context.

        Examples
        --------
        >>> context = catalog.get_context("context")
        >>> context.remove_default_preview_table()  # doctest: +SKIP
        """
        super().remove_default_preview_table()

    def list_observation_tables(self) -> pd.DataFrame:
        """
        List observation tables associated with the Context.

        Returns
        -------
        pd.DataFrame

        Examples
        --------
        >>> context = catalog.get_context("context")
        >>> context.list_observation_tables()  # doctest: +SKIP
        """
        return super().list_observation_tables()

    @classmethod
    def get_by_id(cls, id: ObjectId) -> "Context":
        """
        Returns a Context object by its unique identifier (ID).

        Parameters
        ----------
        id: ObjectId
            Context unique identifier ID.

        Returns
        -------
        Context
            Context object.

        Examples
        --------
        Get a Context object that is already saved.

        >>> fb.Context.get_by_id(<context_id>)  # doctest: +SKIP
        """
        return cls._get_by_id(id=id)

    @typechecked
    def save(
        self, conflict_resolution: ConflictResolution = "raise", _id: Optional[ObjectId] = None
    ) -> None:
        """
        Adds a Context object to the catalog.

        A conflict could be triggered when the object being saved has violated a uniqueness check at the catalog.
        If uniqueness is violated, you can either raise an error or retrieve the object with the same name, depending
        on the conflict resolution parameter passed in. The default behavior is to raise an error.

        Parameters
        ----------
        conflict_resolution: ConflictResolution
            "raise" will raise an error when we encounter a conflict error.
            "retrieve" will handle the conflict error by retrieving the object with the same name.
        _id: Optional[ObjectId]
            The object ID to be used when saving the object. If not provided, a new object ID will be generated.

        Examples
        --------
        >>> context = Context(name=name, primary_entity_ids=entity_ids)  # doctest: +SKIP
        >>> context.save()  # doctest: +SKIP
        """

        super().save(conflict_resolution=conflict_resolution, _id=_id)
