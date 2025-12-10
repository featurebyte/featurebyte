"""
Treatment and Propensity models
"""

from typing import Any, ClassVar, Dict, List, Optional, Union

import pymongo
from pydantic import model_validator

from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.enum import DBVarType, StrEnum, TreatmentType
from featurebyte.models.base import (
    FeatureByteBaseModel,
    FeatureByteCatalogBaseDocumentModel,
    UniqueConstraintResolutionSignature,
    UniqueValuesConstraint,
)

TreatmentLabelType = Union[str, int, bool]


class AssignmentSource(StrEnum):
    """High-level source of treatement assignment. Determines identification strategy"""

    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(proxy_class="featurebyte.AssignmentSource")

    RANDOMIZED = "randomized", "Treatment assigned by explicit randomization"
    # Safe for uplift modeling; ignorability is guaranteed.

    OBSERVATIONAL = "observational", "Treatment chosen by behavior or business process"
    # Requires ignorability + overlap; uplift is NOT generally valid.

    INSTRUMENTAL = (
        "instrumental",
        "Treatment is endogenous but there exists an exogenous instrument Z",
    )
    # Identifies LATE, not global uplift. Requires IV estimators.

    QUASI_EXPERIMENTAL = (
        "quasi-experimental",
        "Identification from natural experiments (RD, DiD, staggered adoption)",
    )
    # CATE possible, uplift rarely meaningful.

    ADAPTIVE = "adaptive", "Assignment depends on previous outcomes (bandits, RL, dynamic policies)"
    # Violates ignorability; requires off-policy evaluation (OPE).


class AssignmentDesign(StrEnum):
    """Specific Assignment design. More granular details inside each Source category."""

    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(proxy_class="featurebyte.AssignmentDesign")

    SIMPLE_RANDOMIZATION = (
        "simple-randomization",
        "Independent randomization of units (standard A/B test)",
    )

    BLOCK_RANDOMIZATION = (
        "block-randomization",
        "Randomization within strata (gender blocks, region blocks)",
    )
    # Requires block-adjusted estimators / FE.

    CLUSTER_RANDOMIZATION = (
        "cluster-randomization",
        "Clusters (stores, schools, users grouped by geography) are randomized",
    )
    # Requires cluster-aware models and SE correction.

    STEPPED_WEDGE = (
        "stepped-wedge",
        "Clusters switch from control→treatment over time in randomized order",
    )
    # Equivalent to STAGGERED_RANDOMIZED_ADOPTION in TreatmentTimeStructure.

    MATCHED_PAIR = "matched-pair", "Paired or matched units randomized within pairs (matched RCT)"

    ENCOURAGEMENT = (
        "encouragement",
        "Instrumental variable design where random encouragement affects T but not Y directly",
    )
    # Produces LATE.

    ADAPTIVE_BANDIT = (
        "adaptive-bandit",
        "Allocation probabilities updated over time based on reward estimates",
    )
    # Requires OPE; uplift invalid.

    BUSINESS_RULE = (
        "business-rule",
        "Deterministic rule-based assignment (tiers, thresholds, rollout order)",
    )
    # Used in SEQUENTIAL_ADOPTION Quasi-Experimental settings.

    OTHER = "other"
    # Other assignment design


class TreatmentTime(StrEnum):
    """Time axis. How treament evolves with time"""

    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(proxy_class="featurebyte.TreatmentTime")

    STATIC = "static", "One-shot assignment; each unit receives T once (A/B tests)"
    # Compatible with standard uplift and CATE.

    TIME_VARYING = (
        "time-varying",
        "Discrete-time longitudinal exposures (weekly ads, pricing over time)",
    )
    # Requires MSM / g-methods; uplift invalid.

    CONTINUOUS = (
        "continuous",
        "Treatment is defined in continuous time (hazard rate, intensity, dosage flow)",
    )
    # Requires survival or continuous-time causal models.


class TreatmentTimeStructure(StrEnum):
    """Detailed Treatment Time Structure. Determines modeling strategy."""

    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(proxy_class="featurebyte.TreatmentTimeStructure")

    NONE = "none", "No meaningful temporal structure; single snapshot"

    INSTANTANEOUS = "instantaneous", "Single assignment at a single timepoint (classic A/B test)"

    LONGITUDINAL = "longitudinal", "Multiple treatment decisions per unit over time"
    # TreatmentTime-varying confounding; requires sequential causal inference.

    SEQUENTIAL_ADOPTION = (
        "sequential-adoption",
        "Non-random rollout over time (regions launched in fixed order)",
    )
    # Identified via quasi-experimental DiD / event-study.

    STAGGERED_RANDOMIZED_ADOPTION = (
        "staggered-randomized-adoption",
        "Randomized adoption time (stepped-wedge RCT)",
    )
    # Must model cluster and time fixed effects.

    BANDIT_ADAPTIVE = "bandit-adaptive", "Assignment policy updates based on past outcomes"
    # Requires off-policy evaluation (IPS / DR-OPE).

    CONTINUOUS_TIME = (
        "continuous-time",
        "Treatment varies continuously with time (process-based exposure)",
    )


class TreatmentInterference(StrEnum):
    """Treatment Interference structure. Violation of Stable Unit Treatment Value Assumption (SUTVA)."""

    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(proxy_class="featurebyte.TreatmentInterference")

    NONE = "none", "Each unit’s outcome depends only on its own treatment (SUTVA holds)"
    # Standard causal estimators are valid.

    PARTIAL = "partial", "Spillovers exist but within known structure (clusters, networks)"
    # Requires cluster / network interference models.

    GENERAL = "general", "Arbitrary interference; standard identification fails"
    # Requires specialized causal graph / network methods.


class PropensityGranularity(StrEnum):
    """Treatment Propensity Granularity"""

    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(proxy_class="featurebyte.PropensityGranularity")

    GLOBAL = "global", "Same p(T | ⋅) for everyone (e.g., 50/50 A/B)"
    BLOCK = "block", "p(T | block) constant within blocks/strata, varies across them"
    CLUSTER = "cluster", "p(T | cluster) constant within clusters"
    UNIT = "unit", "p(T | X_i) varies by unit (observational, adaptive policies)"
    DETERMINISTIC = (
        "deterministic",
        "Given X, treatment is essentially 0/1 (threshold rules, hard tiers)",
    )


class PropensityKnowledge(StrEnum):
    """Knowledge of Treatment Propensity"""

    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(proxy_class="featurebyte.PropensityKnowledge")

    DESIGN_KNOWN = "design-known", "Analytically known from the experiment design or policy spec"
    ESTIMATED = "estimated", "From a model p(T | X)"


class Propensity(FeatureByteBaseModel):
    """
    The `Propensity` class specifies how treatment assignment probabilities
    :math:`p(T \\mid \\cdot)` are defined and obtained for a given experiment or policy.
    It is used to configure estimators that rely on inverse probability weighting or
    doubly robust corrections.

    Parameters
    ----------
    granularity : PropensityGranularity
        Level at which the propensity score is assumed to be constant.

        **Valid values:**

        - `"global"`:
            A single probability shared by all units
            (for example, a 50/50 A/B test).
        - `"block"`:
            Probability is constant within blocks / strata, and may differ across them.
        - `"cluster"`:
            Probability is constant within clusters (stores, schools, regions).
        - `"unit"`:
            Probability varies at the individual level :math:`p(T \\mid X_i)`.
        - `"deterministic"`:
            Assignment is essentially determined by covariates
            (threshold rules, hard tiers).

    knowledge : PropensityKnowledge
        How the propensity is obtained (known from design or estimated from data).

        **Valid values:**

        - `"design-known"`:
            Exact probabilities are available from the experimental design
            or policy specification.
        - `"estimated"`:
            Probabilities are estimated from data using a model :math:`p(T \\mid X)`.

    p_global : float or dict, optional
        Global assignment probabilities. This field is only meaningful when
        ``granularity == "global"``.

        **Valid values:**

        - ``float`` between 0 and 1 for a binary treatment (probability of the active arm).
        - ``Dict[Any, float]`` mapping each treatment value to its probability for
          multi-arm treatments. The probabilities are expected to sum to 1.

    Examples
    --------
    **Example 1: Known Global Propensity**

    ```python
    propensity = fb.Propensity(
        granularity="global",
        knowledge="design-known",
        p_global=0.5,  # 50/50 experiment
    )
    ```

    **Example 2: Estimated Unit-Level Propensity**

    ```python
    propensity = fb.Propensity(
        granularity="unit",
        knowledge="estimated",  # Requires model-based p(T|X)
    )
    ```

    See Also
    --------
    - [Treatment](/reference/featurebyte.api.treatment.Treatment/):
        Treatment Object.
    - [PropensityGranularity](/reference/featurebyte.models.treatment.PropensityGranularity/):
        Enum for PropensityGranularity.
    - [PropensityKnowledge](/reference/featurebyte.models.treatment.PropensityKnowledge/):
        Enum for PropensityKnowledge.
    """

    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(proxy_class="featurebyte.Propensity")

    granularity: PropensityGranularity
    knowledge: PropensityKnowledge
    p_global: Optional[Union[float, Dict[Any, float]]] = (
        None  # Only meaningful if granularity == "global"
    )

    @model_validator(mode="after")
    def _validate_p_global(self) -> "Propensity":
        if self.p_global is not None and self.granularity != "global":
            raise ValueError("p_global is only meaningful when granularity is 'global'")

        # 3) For design-known global propensity, p_global must be provided
        if (
            self.granularity == "global"
            and self.knowledge == "design-known"
            and self.p_global is None
        ):
            raise ValueError(
                "p_global must be provided when granularity is 'global' and "
                "knowledge is 'design-known'."
            )

        # 4) If p_global is provided, validate its numeric properties
        if isinstance(self.p_global, float):
            if not (0.0 <= self.p_global <= 1.0):
                raise ValueError(f"p_global float must be between 0 and 1 (got {self.p_global}).")

        elif isinstance(self.p_global, dict):
            if not self.p_global:
                raise ValueError("p_global dict must not be empty.")

            probs = []
            for key, value in self.p_global.items():
                if not isinstance(value, float):
                    raise ValueError(
                        f"p_global[{key}] must be numeric (got {type(value).__name__})."
                    )
                value_f = float(value)
                if not (0.0 <= value_f <= 1.0):
                    raise ValueError(f"p_global[{key}] must be between 0 and 1 (got {value_f}).")
                probs.append(value_f)

            total = sum(probs)
            if abs(total - 1.0) > 1e-6:
                raise ValueError(
                    f"Sum of p_global probabilities must be 1.0 (got {total}, tolerance=1e-6)."
                )

        return self


class TreatmentModel(FeatureByteCatalogBaseDocumentModel):
    """
    The `Treatment` class describes the causal treatment variable and its assignment
    mechanism for an experiment, quasi-experiment, or observational study. It provides
    structured metadata that downstream causal estimators can use to pick appropriate
    identification and modeling strategies.
    """

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

    def recommended_methods(self) -> List[str]:
        """
        Return a list of compatible causal estimation methods based on the
        treatment configuration (treatment_type, assignment source, time structure,
        interference, propensity knowledge, etc.).

        Returns
        -------
        list of str
            Names of supported estimator families.

        Notes
        -----
        This logic is intentionally high-level. It does **not** require the
        presence of feature or outcome metadata; it only checks structural
        compatibility implied by the treatment description.

        Method families:
        - "s-learner"
        - "t-learner"
        - "x-learner"
        - "r-learner"
        - "dr-learner"
        - "dml"                      (double / orthogonal ML)
        - "uplift"                   (incremental uplift models)
        - "iv"                       (instrumental variables)
        - "g-methods"                (MSM, sequential g-formula)
        - "panel-did"                (staggered / event-study)
        - "ope"                      (bandit off-policy evaluation)
        - "cluster-exposure"         (cluster interference methods)
        - "randomization-inference"  (permutation / randomization tests)
        - "network-causal"           (graph-based interference models)
        - "exposure-mapping"         (network exposure mapping)

        Examples
        --------
        **Example 1: Binary observational treatment (supports DR-Learner)**

        ```python
        observational_treatment = fb.Treatment(
            treatment_type=TreatmentType.BINARY,
            source="observational",
            design="business-rule",
            time="static",
            time_structure="none",
            interference="none",
            treatment_labels=[0, 1],
            control_label=0,
            propensity=fb.Propensity(
                granularity="unit",
                knowledge="estimated",  # model-based p(T | X)
            ),
        )

        compatible_methods = observational_treatment.recommended_methods()
        # e.g. ["s-learner", "t-learner", "x-learner", "r-learner", "dr-learner", "dml"]
        ```

        **Example 2: Continuous dose (price / spend / dosage) with static exposure**

        ```python
        continuous_treatment = fb.Treatment(
            treatment_type=TreatmentType.NUMERIC,
            source="observational",
            design="other",
            time="static",
            time_structure="none",
            interference="none",
            # For continuous treatments:
            #   - treatment_labels must be None
            #   - control_label must be None
            #   - propensity must be None
        )

        compatible_methods = continuous_treatment.recommended_methods()
        # e.g. ["dr-learner", "dml"]
        ```

        """
        methods = []

        # --------------- BASIC CATE META-LEARNERS -------------------------
        # Conditions:
        #   - binary or multi-arm
        #   - static treatment
        #   - no interference
        #   - overlap must be estimated -> requires an estimated propensity
        if self.treatment_type in {TreatmentType.BINARY, TreatmentType.MULTI_ARM}:
            if (
                self.time == TreatmentTime.STATIC
                and self.interference == TreatmentInterference.NONE
            ):
                # Need estimated OR design-known propensities for CATE
                if self.propensity is not None and self.propensity.knowledge in {
                    PropensityKnowledge.ESTIMATED,
                    PropensityKnowledge.DESIGN_KNOWN,
                }:
                    methods += [
                        "s-learner",
                        "t-learner",
                        "x-learner",
                        "r-learner",
                        "dr-learner",
                        "dml",
                    ]

        # --------------------- CONTINUOUS TREATMENT EFFECTS ----------------------
        # Static continuous dose-response / CATE:
        #   - treatment_type == TreatmentType.NUMERIC
        #   - static (no longitudinal structure)
        #   - no interference
        #
        # Propensity object is not used for continuous treatments in this schema;
        # nuisance models (m(X,T), e(T|X)) are expected to be estimated internally.
        if (
            self.treatment_type == TreatmentType.NUMERIC
            and self.time == TreatmentTime.STATIC
            and self.interference == TreatmentInterference.NONE
        ):
            methods += [
                "dr-learner",  # continuous dose-response DR-Learner
                "dml",  # continuous treatment DML / orthogonal ML
            ]

        # ------------------------ UPLIFT MODELS ----------------------------
        # Conditions:
        #   - binary
        #   - randomized (guaranteed ignorability)
        #   - static
        #   - no interference
        if (
            self.treatment_type == TreatmentType.BINARY
            and self.source == AssignmentSource.RANDOMIZED
            and self.time == TreatmentTime.STATIC
            and self.interference == TreatmentInterference.NONE
        ):
            methods.append("uplift")

        # ---------------------- INSTRUMENTAL VARIABLES ---------------------
        if self.source == AssignmentSource.INSTRUMENTAL:
            methods.append("iv")

        # -------------------------- G-METHODS ------------------------------
        # time-varying treatments with longitudinal confounding
        if (
            self.time == TreatmentTime.TIME_VARYING
            and self.time_structure == TreatmentTimeStructure.LONGITUDINAL
        ):
            methods.append("g-methods")  # MSM, sequential g-formula, etc.

        # --------------------------- PANEL DiD -----------------------------
        # staggered/quasi-exp adoption + panel time structure
        if self.time_structure in {
            TreatmentTimeStructure.SEQUENTIAL_ADOPTION,
            TreatmentTimeStructure.STAGGERED_RANDOMIZED_ADOPTION,
        }:
            methods.append("panel-did")

        # ----------------------------- OPE ---------------------------------
        # adaptive bandit setting
        if self.source == AssignmentSource.ADAPTIVE:
            methods.append("ope")

        # ----------------------- INTERFERENCE-AWARE METHODS -----------------------
        if self.interference == TreatmentInterference.PARTIAL:
            methods.append("cluster-exposure")
            methods.append("randomization-inference")

        if self.interference == TreatmentInterference.GENERAL:
            methods.append("network-causal")
            methods.append("exposure-mapping")

        return methods

    class Settings(FeatureByteCatalogBaseDocumentModel.Settings):
        """
        MongoDB settings
        """

        collection_name = "treatment"
        unique_constraints: List[UniqueValuesConstraint] = [
            UniqueValuesConstraint(
                fields=("_id",),
                conflict_fields_signature={"id": ["_id"]},
                resolution_signature=None,
            ),
            UniqueValuesConstraint(
                fields=("name",),
                conflict_fields_signature={"name": ["name"]},
                resolution_signature=UniqueConstraintResolutionSignature.RENAME,
            ),
        ]
        indexes = FeatureByteCatalogBaseDocumentModel.Settings.indexes + [
            pymongo.operations.IndexModel("dtype"),
            pymongo.operations.IndexModel(
                [
                    ("name", pymongo.TEXT),
                    ("description", pymongo.TEXT),
                ],
            ),
        ]
