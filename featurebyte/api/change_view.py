"""
ChangeView class
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, ClassVar, List, Optional, Tuple, Union

from pydantic import Field
from typeguard import typechecked

from featurebyte.api.lag import LaggableViewColumn
from featurebyte.api.scd_table import SCDTable
from featurebyte.api.view import GroupByMixin, View
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.exception import ChangeViewNoJoinColumnError
from featurebyte.query_graph.enum import GraphNodeType
from featurebyte.query_graph.model.feature_job_setting import FeatureJobSetting


class ChangeViewColumn(LaggableViewColumn):
    """
    ChangeViewColumn class
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc()

    @typechecked
    def lag(
        self: ChangeViewColumn, entity_columns: Union[str, List[str]], offset: int = 1
    ) -> ChangeViewColumn:
        """
        Lag is a transform that enables the retrieval of the preceding value associated with a particular entity in
        a view.

        This makes it feasible to compute essential features, such as those that depend on inter-event time
        and the proximity to the previous point.

        Parameters
        ----------
        entity_columns : str | list[str]
            Entity columns used when retrieving the lag value.
        offset : int
            The number of rows backward from which to retrieve a value.

        Returns
        -------
        ChangeViewColumn

        Raises
        ------
        ValueError
            If a lag operation has already been applied to the column.

        Examples
        --------
        Get a ChangeView to track changes in Customer's State.

        >>> scd_table = catalog.get_table("GROCERYCUSTOMER")
        >>> change_view = scd_table.get_change_view(track_changes_column="State")


        Create a new column that indicates the prior past_State for a Customer

        >>> lagged_column = change_view["past_State"].lag("GroceryCustomerGuid")

        # noqa: DAR402
        """
        return super().lag(entity_columns, offset)


class ChangeView(View, GroupByMixin):
    """
    A ChangeView object is used to capture changes in SCDTable object in an easy manner. This is useful as
    changes in SCDTable may constitute powerful features such as:

    - how many times has a customer moved in the past 6 months?
    - if they moved the past 6 months, where did they use to live?
    - did they get divorced recently?
    - did they have any new kids in the family?
    - do they have a new job?

    This new view tracks all changes for a given column (identified as track_changes_column when the view is created).
    The resulting view has 5 columns:

    - the natural key of the SCDView
    - past_<name_of_column_tracked>: value of the column before the change
    - new_<name_of_column_tracked>: value of the column after the change
    - past_valid_from_timestamp (equal to the effective timestamp of the SCD before the change)
    - new_valid_from_timestamp (equal to the effective timestamp of the SCD after the change)

    The ChangeView can be used to create Aggregates of Changes Over a Window features, similar to Aggregates Over a
    Window features created from an Event View.

    See Also
    --------
    - [get_change_view](/reference/featurebyte.api.scd_table.SCDTable.get_change_view/): get change view from a SCDTable
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(
        proxy_class="featurebyte.ChangeView",
        skip_params_and_signature_in_class_docs=True,
    )
    _series_class: ClassVar[Any] = ChangeViewColumn
    _view_graph_node_type: ClassVar[GraphNodeType] = GraphNodeType.CHANGE_VIEW

    # pydantic instance variables
    default_feature_job_setting: FeatureJobSetting = Field(
        description="Returns the default feature job setting for the view.\n\n"
        "The Default Feature Job Setting establishes the default setting used by features that aggregate "
        "data in the view, ensuring consistency of the Feature Job Setting across features created by "
        "different team members. While it's possible to override the setting during feature declaration, "
        "using the Default Feature Job Setting simplifies the process of setting up the Feature Job "
        "Setting for each feature."
    )
    effective_timestamp_column: str = Field(frozen=True)
    natural_key_column: str = Field(frozen=True)

    @property
    def timestamp_column(self) -> str:
        """
        Timestamp column of the change view

        Returns
        -------
        str
        """
        return self.effective_timestamp_column

    def _get_additional_inherited_columns(self) -> set[str]:
        return {self.timestamp_column}

    @staticmethod
    def _validate_prefixes(prefixes: Optional[Tuple[Optional[str], Optional[str]]] = None) -> None:
        """
        Validate prefixes

        Parameters
        ----------
        prefixes: Optional[Tuple[Optional[str], Optional[str]]]
            Optional prefixes where each element indicates the prefix to add to the new column names for the name of
            the column that we want to track. The first prefix will be used for the old, and the second for the new.
            Pass a value of None instead of a string to indicate that the column name will be prefixed with the default
            values of "past_", and "new_". At least one of the values must not be None. If two values are provided,
            they must be different.

        Raises
        ------
        ValueError
            raised when any of the validation checks fails
        """
        if prefixes is None:
            return

        before, after = prefixes
        if before is None and after is None:
            raise ValueError(
                "Prefixes provided are both None. Please indicate at least one prefix to update."
            )
        if before == "" or after == "":
            raise ValueError(
                "Please provide a non-empty string as a prefix value. If you want to use the default "
                "value, please provide a `None` type instead."
            )
        if before == after:
            raise ValueError(
                f"Prefixes provided need to be different values. Current prefix value: {before}"
            )

    @staticmethod
    def validate_inputs(
        scd_table: SCDTable,
        track_changes_column: str,
        prefixes: Optional[Tuple[Optional[str], Optional[str]]] = None,
    ) -> None:
        """
        Validate the inputs.

        Checks
        - column is not empty
        - column is a column in the scd table

        Parameters
        ----------
        scd_table: SCDTable
            table to create view from
        track_changes_column: str
            column to track changes for
        prefixes: Optional[Tuple[Optional[str], Optional[str]]]
            Optional prefixes where each element indicates the prefix to add to the new column names for the name of
            the column that we want to track. The first prefix will be used for the old, and the second for the new.
            Pass a value of None instead of a string to indicate that the column name will be prefixed with the default
            values of "past_", and "new_". At least one of the values must not be None. If two values are provided,
            they must be different.

        Raises
        ------
        ValueError
            raised when any of the validation checks fails
        """
        if track_changes_column == "":
            raise ValueError("Empty column provided. Please provide a valid column.")
        if track_changes_column not in scd_table.columns:
            raise ValueError(
                "Column provided is not a column in the SCDTable provided. Please pick a column "
                f"from: {sorted(scd_table.columns)}."
            )

        # Validate prefixes
        ChangeView._validate_prefixes(prefixes)

    @property
    def _getitem_frame_params(self) -> dict[str, Any]:
        params = super()._getitem_frame_params
        params.update({
            "default_feature_job_setting": self.default_feature_job_setting,
            "natural_key_column": self.natural_key_column,
            "effective_timestamp_column": self.effective_timestamp_column,
        })
        return params

    @typechecked
    def update_default_feature_job_setting(self, feature_job_setting: FeatureJobSetting) -> None:
        """
        This updates the default feature job setting, which ensures consistency across features created by different
        team members.

        The Default Feature Job Setting establishes the default setting used by features that aggregate data in a table.
        While it's possible to override the setting during feature declaration, using the Default Feature Job Setting
        simplifies the process of setting up the Feature Job Setting for each feature.

        The Feature Job Setting captures details about batch feature computations for the online feature store,
        including frequency, timing, and blind spot for the data.

        For example, if a data warehouse refreshes every hour starting 10 seconds after the hour, is usually finished
        within 2 minutes and missing the latest data up to the last 30 seconds before the hour, the feature job
        settings could be:

        - period: 60m
        - offset: 10s + 2m + 5s (a safety buffer) = 135s
        - blind_spot: 30s + 10s + 2m + 5s = 165s

        FeatureByte offers automated analysis of event table record creation to suggest appropriate setting values. If
        the source table contains record creation timestamps, you can run a new analysis via the method 'create_new_
        feature_job_setting_analysis'."

        Parameters
        ----------
        feature_job_setting: FeatureJobSetting
            Feature job setting
        """
        self.default_feature_job_setting = feature_job_setting

    @staticmethod
    def get_default_feature_job_setting(
        feature_job_setting: Optional[FeatureJobSetting] = None,
    ) -> FeatureJobSetting:
        """
        Returns the default feature job setting of the Change View. If none has been set, it returns the default job
        setting to be once a day, at the time of the view creation.

        Parameters
        ----------
        feature_job_setting: Optional[FeatureJobSetting]
            Feature job setting object (auto-detected if not provided)

        Returns
        -------
        FeatureJobSetting
            the feature job setting we want to use

        Examples
        --------
        >>> feature_job_setting = ChangeView.get_default_feature_job_setting()
        >>> feature_job_setting  # doctest: +SKIP
        FeatureJobSetting(blind_spot='0', period='24h', offset='6h24m')
        """
        if feature_job_setting is not None:
            return feature_job_setting
        # default job setting of once a day, at the time the view is created
        now = datetime.now()
        hour, minute = now.hour, now.minute
        return FeatureJobSetting(blind_spot="0", offset=f"{hour}h{minute}m", period="24h")

    def get_join_column(self) -> str:
        raise ChangeViewNoJoinColumnError

    def _get_join_column(self) -> Optional[str]:
        return None
