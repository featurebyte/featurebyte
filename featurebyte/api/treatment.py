"""
Feature Namespace module.
"""

from __future__ import annotations

from typing import Any, ClassVar, List, Optional

from pydantic import Field

from featurebyte.api.savable_api_object import DeletableApiObject, SavableApiObject
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.enum import DBVarType, TreatmentType
from featurebyte.exception import RecordRetrievalException
from featurebyte.models.treatment import (
    AssignmentDesign,
    AssignmentSource,
    Propensity,
    TreatmentInterference,
    TreatmentLabelType,
    TreatmentModel,
    TreatmentTime,
    TreatmentTimeStructure,
)
from featurebyte.schema.treatment import TreatmentUpdate


class Treatment(DeletableApiObject, SavableApiObject):
    """
    The `Treatment` class describes the causal treatment variable and its assignment
    mechanism for an experiment, quasi-experiment, or observational study. It provides
    structured metadata that downstream causal estimators can use to pick appropriate
    identification and modeling strategies.
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(proxy_class="featurebyte.Treatment")
    _route: ClassVar[str] = "/treatment"
    _update_schema_class: ClassVar[Any] = TreatmentUpdate
    _list_schema: ClassVar[Any] = TreatmentModel
    _get_schema: ClassVar[Any] = TreatmentModel
    _list_fields: ClassVar[List[str]] = [
        "name",
        "dtype",
        "created_at",
        "treatment_type",
        "source",
        "design",
        "time",
        "time_structure",
        "interference",
        "treatment_labels",
        "control_label",
        "propensity",
    ]

    # pydantic instance variables (internal, used for create / unsaved objects)
    internal_dtype: DBVarType = Field(alias="dtype")
    internal_treatment_type: TreatmentType = Field(alias="treatment_type")
    internal_source: AssignmentSource = Field(alias="source")
    internal_design: AssignmentDesign = Field(alias="design")
    internal_time: TreatmentTime = Field(default=TreatmentTime.STATIC, alias="time")
    internal_time_structure: TreatmentTimeStructure = Field(
        default=TreatmentTimeStructure.NONE,
        alias="time_structure",
    )
    internal_interference: TreatmentInterference = Field(
        default=TreatmentInterference.NONE,
        alias="interference",
    )
    internal_treatment_labels: Optional[List[TreatmentLabelType]] = Field(
        default=None,
        alias="treatment_labels",
    )
    internal_control_label: Optional[TreatmentLabelType] = Field(
        default=None,
        alias="control_label",
    )
    internal_propensity: Optional[Propensity] = Field(
        default=None,
        alias="propensity",
    )

    @classmethod
    def create(
        cls,
        name: str,
        dtype: DBVarType,
        treatment_type: TreatmentType,
        source: AssignmentSource,
        design: AssignmentDesign,
        time: TreatmentTime = TreatmentTime.STATIC,
        time_structure: TreatmentTimeStructure = TreatmentTimeStructure.NONE,
        interference: TreatmentInterference = TreatmentInterference.NONE,
        treatment_labels: Optional[
            List[TreatmentLabelType]
        ] = None,  # e.g. [0, 1] or ["A", "B", "C"]
        control_label: Optional[TreatmentLabelType] = None,
        propensity: Optional[Propensity] = None,
    ) -> Treatment:
        """
        Create a new Treatment.

        Parameters
        ----------
        name: str
            Name of the Treatment
        dtype: DBVarType
            Data type of the Treatment
        treatment_type: TreatmentType
            Scale of the treatment variable.

            Valid values:

            - `binary`:
              Two-level treatment (for example, exposed vs control, coupon vs no coupon).
            - `multi_arm`:
              Discrete treatment with more than two levels (dosage tiers, variants).
            - `continuous`:
              Numeric treatment representing a dose, price, spend, or intensity.
              Suitable for dose response estimators such as DR-learner and
              continuous treatment DML or orthogonal ML.

        source: AssignmentSource
            High level source of treatment assignment. Determines the identification
            strategy and whether uplift style modeling is valid.

        design: AssignmentDesign
            Specific assignment design within the chosen `source`.

        time: TreatmentTime
            Time at which treatment is defined, for example static or time varying.

        time_structure: TreatmentTimeStructure
            Detailed temporal structure of the treatment process.

        interference: TreatmentInterference
            Structure of interference between units (violations of SUTVA).

        treatment_labels: Optional[List[TreatmentLabelType]]
            List of raw treatment values used in the dataset.

            Constraints

            - For `continuous` treatments, this must be ``None``.
            - For `binary` treatments, this must be provided and contain exactly two values.
            - For `multi_arm` treatments, this must be provided and contain at least two values.

        control_label : TreatmentLabelType, optional
            Value representing the control or baseline arm.

            Constraints

            - For `continuous` treatments, this must be ``None``.
            - For `binary` and `multi_arm` treatments, this must be provided and
              must be one of ``treatment_labels``.

        propensity : Propensity, optional
            Optional `Propensity` specification describing how treatment assignment
            probabilities p(T | ·) are known or estimated.

            Constraints

            - For `continuous` treatments, `propensity` must be ``None``.

        Returns
        -------
        Treatment
            The created Treatment

        Examples
        --------
        **Example 1: Simple A/B Test With Known Global Propensity**

        >>> treatment = fb.Treatment.create(  # doctest: +SKIP
        ...     name="Churn Camppaign A/B test",
        ...     dtype=fb.DBVarType.INT,
        ...     treatment_type=fb.TreatmentType.BINARY,
        ...     source="randomized",
        ...     design="simple-randomization",
        ...     time="static",
        ...     time_structure="instantaneous",
        ...     interference="none",
        ...     treatment_labels=[0, 1],
        ...     control_label=0,
        ...     propensity=fb.Propensity(
        ...         granularity="global",
        ...         knowledge="design-known",
        ...         p_global=0.5,  # 50/50 experiment
        ...     ),
        ... )

        **Example 2: Observational Treatment With Estimated Unit-Level Propensity**

        >>> observational_treatment = fb.Treatment.create(  # doctest: +SKIP
        ...     name="Churn Camppaign",
        ...     dtype=fb.DBVarType.INT,
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

        See Also
        --------
        - [TreatmentType](/reference/featurebyte.enum.TreatmentType/):
            Enum for TreatmentType.
        - [Propensity](/reference/featurebyte.models.treatment.Propensity/):
            Schema for Propensity.
        - [AssignmentSource](/reference/featurebyte.models.treatment.AssignmentSource/):
            Enum for AssignmentSource.
        - [AssignmentDesign](/reference/featurebyte.models.treatment.AssignmentDesign/):
            Enum for AssignmentDesign.
        - [TreatmentTime](/reference/featurebyte.models.treatment.TreatmentTime/):
            Enum for TreatmentTime.
        - [TreatmentTimeStructure](/reference/featurebyte.models.treatment.TreatmentTimeStructure/):
            Enum for TreatmentTimeStructure.
        - [TreatmentInterference](/reference/featurebyte.models.treatment.TreatmentInterference/):
            Enum for TreatmentInterference.

        """
        treatment = Treatment(
            name=name,
            dtype=dtype,
            treatment_type=treatment_type,
            source=source,
            design=design,
            time=time,
            time_structure=time_structure,
            interference=interference,
            treatment_labels=treatment_labels,
            control_label=control_label,
            propensity=propensity,
        )
        treatment.save()
        return treatment

    # ------------------------------------------------------------------
    # Properties that read from cached_model with internal fallback
    # ------------------------------------------------------------------

    @property
    def dtype(self) -> DBVarType:
        """
        Database variable type of the treatment.

        Returns
        -------
        DBVarType
        """
        try:
            return self.cached_model.dtype
        except RecordRetrievalException:
            return self.internal_dtype

    @property
    def treatment_type(self) -> TreatmentType:
        """
        Type of the Treatment.

        Returns
        -------
        TreatmentType
        """
        try:
            return self.cached_model.treatment_type
        except RecordRetrievalException:
            return self.internal_treatment_type

    @property
    def source(self) -> AssignmentSource:
        """
        High level source of treatment assignment. Determines the identification
        strategy and whether uplift style modeling is valid.

        Returns
        -------
        AssignmentSource
        """
        try:
            return self.cached_model.source
        except RecordRetrievalException:
            return self.internal_source

    @property
    def design(self) -> AssignmentDesign:
        """
        Specific assignment design within the chosen `source`.

        Returns
        -------
        AssignmentDesign
        """
        try:
            return self.cached_model.design
        except RecordRetrievalException:
            return self.internal_design

    @property
    def time(self) -> TreatmentTime:
        """
        Time at which treatment is defined.

        Returns
        -------
        TreatmentTime
        """
        try:
            return self.cached_model.time
        except RecordRetrievalException:
            return self.internal_time

    @property
    def time_structure(self) -> TreatmentTimeStructure:
        """
        Detailed temporal structure of the treatment process.

        Returns
        -------
        TreatmentTimeStructure
        """
        try:
            return self.cached_model.time_structure
        except RecordRetrievalException:
            return self.internal_time_structure

    @property
    def interference(self) -> TreatmentInterference:
        """
        Structure of interference between units (violations of SUTVA).

        Returns
        -------
        TreatmentInterference
        """
        try:
            return self.cached_model.interference
        except RecordRetrievalException:
            return self.internal_interference

    @property
    def control_label(self) -> Optional[TreatmentLabelType]:
        """
        Value representing the control or baseline arm.

        Returns
        -------
        Optional[TreatmentLabelType]
        """
        try:
            return self.cached_model.control_label
        except RecordRetrievalException:
            return self.internal_control_label

    @property
    def treatment_labels(self) -> Optional[List[TreatmentLabelType]]:
        """
        List of raw treatment values used in the dataset.

        Returns
        -------
        Optional[List[TreatmentLabelType]]
        """
        try:
            return self.cached_model.treatment_labels
        except RecordRetrievalException:
            return self.internal_treatment_labels

    @property
    def propensity(self) -> Optional[Propensity]:
        """
        Optional `Propensity` specification describing how treatment assignment
        probabilities p(T | ·) are known or estimated.

        Returns
        -------
        Optional[Propensity]
        """
        try:
            return self.cached_model.propensity
        except RecordRetrievalException:
            return self.internal_propensity

    def delete(self) -> None:
        """
        Delete a treatment from the persistent data store. A treatment can only be deleted
        if it is not used in any context.

        Examples
        --------

        >>> treatment = fb.Treatment.create(  # doctest: +SKIP
        ...     treatment_type=fb.TreatmentType.BINARY,
        ...     source="randomized",
        ...     design="simple-randomization",
        ...     time="static",
        ...     time_structure="instantaneous",
        ...     interference="none",
        ...     treatment_labels=[0, 1],
        ...     control_label=0,
        ...     propensity=fb.Propensity(
        ...         granularity="global",
        ...         knowledge="design-known",
        ...         p_global=0.5,  # 50/50 experiment
        ...     ),
        ... )
        >>> treatment.delete()  # doctest: +SKIP
        """
        super()._delete()
