"""
Treatment schema
"""

from typing import Any, List, Optional, Tuple

from bson import ObjectId
from pydantic import Field, model_validator

from featurebyte.common.validator import validate_treatment_type
from featurebyte.enum import DBVarType, TreatmentType
from featurebyte.models.base import FeatureByteBaseModel, NameStr, PydanticObjectId
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
from featurebyte.schema.common.base import (
    BaseDocumentServiceUpdateSchema,
    PaginationMixin,
)


class TreatmentCreate(FeatureByteBaseModel):
    """
    Treatment Creation Schema
    """

    id: Optional[PydanticObjectId] = Field(default_factory=ObjectId, alias="_id")
    name: NameStr
    dtype: DBVarType
    treatment_type: TreatmentType
    source: AssignmentSource
    design: AssignmentDesign
    time: TreatmentTime = TreatmentTime.STATIC
    time_structure: TreatmentTimeStructure = TreatmentTimeStructure.NONE
    interference: TreatmentInterference = TreatmentInterference.NONE
    treatment_labels: Optional[List[TreatmentLabelType]] = None  # e.g. [0, 1] or ["A", "B", "C"]
    control_label: Optional[TreatmentLabelType] = None
    propensity: Optional[Propensity] = None

    def _get_expected_label_types(self) -> Tuple[type, ...]:
        numeric_dtypes = {DBVarType.INT, DBVarType.FLOAT}
        bool_dtypes = {DBVarType.BOOL}
        string_dtypes = {DBVarType.VARCHAR, DBVarType.CHAR}

        if self.dtype in numeric_dtypes:
            # allow both int and float for numeric treatments
            return (int, float)
        if self.dtype in bool_dtypes:
            return (bool,)
        if self.dtype in string_dtypes:
            return (str,)
        raise TypeError(
            f"dtype should INT, FLOAT, BOOL, VARCHAR or CHAR. {self.dtype} was provided."
        )

    def _validate_label_types(self) -> None:
        expected_types = self._get_expected_label_types()

        def _check_label(label: Any, which: str) -> None:
            if not isinstance(label, expected_types):
                expected_names = ", ".join(t.__name__ for t in expected_types)
                raise TypeError(
                    f"{which} {label!r} has type {type(label).__name__} which is not "
                    f"compatible with dtype '{self.dtype}'. Expected types: {expected_names}."
                )

        if self.treatment_labels is not None:
            for lbl in self.treatment_labels:
                _check_label(lbl, "treatment_label")

        if self.control_label is not None:
            _check_label(self.control_label, "control_label")

    @model_validator(mode="after")
    def _validate_settings(self) -> "TreatmentCreate":
        validate_treatment_type(treatment_type=self.treatment_type, dtype=self.dtype)

        # 1. Continuous treatments
        if self.treatment_type == TreatmentType.NUMERIC:
            if self.treatment_labels is not None:
                raise ValueError("treatment_labels must be None for numeric treatments")
            if self.control_label is not None:
                raise ValueError("control_label must be None for numeric treatments")
            if self.propensity is not None:
                raise ValueError("propensity must be None for numeric treatments")

        # 2. Non continuous treatments (binary / multi_arm) must have treatment_labels
        if self.treatment_type != TreatmentType.NUMERIC:
            if self.treatment_labels is None:
                raise ValueError(
                    "treatment_labels must be provided for non numeric treatments "
                    f"(treatment_type='{self.treatment_type}')."
                )
            num_vals = len(self.treatment_labels)
            if self.treatment_type == TreatmentType.BINARY and num_vals != 2:
                raise ValueError(
                    f"Binary treatment must define exactly two treatment_labels (got {num_vals})."
                )
            if self.treatment_type == TreatmentType.MULTI_ARM and num_vals < 3:
                raise ValueError(
                    "Multi arm treatment must define at least three treatment_labels "
                    f"(got {num_vals})."
                )

            # control_label must be provided and be one of treatment_labels
            if self.control_label is None:
                raise ValueError(
                    "control_label must be provided for non numeric treatments "
                    f"(treatment_type='{self.treatment_type}')."
                )
            if self.control_label not in self.treatment_labels:
                raise ValueError(
                    "control_label must be one of treatment_labels "
                    f"(control_label={self.control_label}, "
                    f"treatment_labels={self.treatment_labels})."
                )
            if self.propensity is None:
                raise ValueError(
                    "propensity must be provided for non numeric treatments "
                    f"(treatment_type='{self.treatment_type}')."
                )

        # 2b. Enforce label dtype compatibility
        # This runs after basic presence and membership checks
        self._validate_label_types()

        # 3. Coherence between time and time_structure
        if self.time == "static" and self.time_structure not in {
            TreatmentTimeStructure.NONE,
            TreatmentTimeStructure.INSTANTANEOUS,
        }:
            raise ValueError(
                "For time='static', time_structure must be 'none' or 'instantaneous' "
                f"(got '{self.time_structure}')."
            )

        if (
            self.time == TreatmentTime.CONTINUOUS
            and self.time_structure != TreatmentTimeStructure.CONTINUOUS_TIME
        ):
            raise ValueError(
                "For time='continuous', time_structure must be 'continuous-time' "
                f"(got '{self.time_structure}')."
            )

        return self


class TreatmentUpdate(BaseDocumentServiceUpdateSchema):
    """
    Treatment update schema - exposed to client
    """

    source: Optional[AssignmentSource] = None
    design: Optional[AssignmentDesign] = None
    time: Optional[TreatmentTime] = None
    time_structure: Optional[TreatmentTimeStructure] = None
    interference: Optional[TreatmentInterference] = None
    propensity: Optional[Propensity] = None


class TreatmentLabelsValidate(FeatureByteBaseModel):
    """
    Treatment Labels validate schema - used by server side only, not exposed to client
    """

    observation_table_id: PydanticObjectId


class TreatmentList(PaginationMixin):
    """
    Paginated list of Treatment
    """

    data: List[TreatmentModel]
