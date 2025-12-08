"""
Treatment and Propensity models
"""

from typing import Any, ClassVar, Dict, List, Literal, Optional, Union

from pydantic import model_validator

from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.models.base import FeatureByteBaseModel


class Propensity(FeatureByteBaseModel):
    """
    The `Propensity` class specifies how treatment assignment probabilities
    :math:`p(T \\mid \\cdot)` are defined and obtained for a given experiment or policy.
    It is used to configure estimators that rely on inverse probability weighting or
    doubly robust corrections.

    Parameters
    ----------
    granularity : {"global", "block", "cluster", "unit", "deterministic"}
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

    knowledge : {"design-known", "estimated"}
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
        - ``Dict[str, float]`` mapping each treatment value to its probability for
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
    - [Treatment](/reference/featurebyte.model.treatment.Treatment/):
        Schema for Treatment.
    """

    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(proxy_class="featurebyte.Propensity")

    granularity: Literal["global", "block", "cluster", "unit", "deterministic"]
    knowledge: Literal["design-known", "estimated", "not-available"]
    p_global: Optional[Union[float, Dict[str, float]]] = (
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


class Treatment(FeatureByteBaseModel):
    """
    The `Treatment` class describes the causal treatment variable and its assignment
    mechanism for an experiment, quasi-experiment, or observational study. It provides
    structured metadata that downstream causal estimators can use to pick appropriate
    identification and modeling strategies.

    Parameters
    ----------
    scale : {"binary", "multi_arm", "continuous"}
        Scale of the treatment variable.

        **Valid values:**

        - `"binary"`:
            Two-level treatment (e.g., exposed vs control, coupon vs no coupon).
        - `"multi_arm"`:
            Discrete treatment with more than two levels (dosage tiers, variants).
        - `"continuous"`:
            Numeric treatment representing a dose, price, spend, or intensity.
            Suitable for dose–response estimators such as DR-learner and
            continuous-treatment DML / orthogonal ML.

    source : {"randomized", "observational", "instrumental",
              "quasi-experimental", "adaptive"}
        High-level source of treatment assignment. Determines the identification
        strategy and whether uplift-style modeling is valid.

    design : {"simple-randomization", "block-randomization", "cluster-randomization",
              "stepped-wedge", "matched-pair", "encouragement", "adaptive-bandit",
              "business-rule", "other"}
        Specific assignment design within the chosen `source`.

    time : {"static", "time-varying", "continuous"}, default = "static"
        Time scale at which treatment is defined.

    time_structure : {"none", "instantaneous", "longitudinal",
                      "sequential-adoption", "staggered-randomized-adoption",
                      "bandit-adaptive", "continuous-time"}, default = "none"
        Detailed temporal structure of the treatment process.

    interference : {"none", "partial", "general"}, default = "none"
        Structure of interference between units (violations of SUTVA).

    treatment_values : list, optional
        List of raw treatment values used in the dataset.

        **Constraints**

        - For `"continuous"` treatments, this must be ``None``.
        - For `"binary"` treatments, this must be provided and contain exactly two values.
        - For `"multi_arm"` treatments, this must be provided and contain at least two values.

    control_value : Any, optional
        Value representing the control or baseline arm.

        **Constraints**

        - For `"continuous"` treatments, this must be ``None``.
        - For `"binary"` and `"multi_arm"` treatments, this must be provided and
          must be one of ``treatment_values``.

    propensity : Propensity, optional
        Optional `Propensity` specification describing how treatment assignment
        probabilities :math:`p(T \\mid \\cdot)` are known or estimated.

        **Constraints**

        - For `"continuous"` treatments, `propensity` must be ``None``.


    Examples
    --------
    **Example 1: Simple A/B Test With Known Global Propensity**

    ```python
    treatment = fb.Treatment(
        scale="binary",
        source="randomized",
        design="simple-randomization",
        time="static",
        time_structure="instantaneous",
        interference="none",
        treatment_values=[0, 1],
        control_value=0,
        propensity=fb.Propensity(
            granularity="global",
            knowledge="design-known",
            p_global=0.5,  # 50/50 experiment
        ),
    )
    ```

    **Example 2: Observational Treatment With Estimated Unit-Level Propensity**

    ```python
    observational_treatment = fb.Treatment(
        scale="binary",
        source="observational",
        design="business-rule",
        time="static",
        time_structure="none",
        interference="none",
        treatment_values=[0, 1],
        control_value=0,
        propensity=fb.Propensity(
            granularity="unit",
            knowledge="estimated",  # Requires model-based p(T|X)
        ),
    )
    ```

    See Also
    --------
    - [Propensity](/reference/featurebyte.model.treatment.Propensity/):
        Schema for Propensity.
    """

    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(proxy_class="featurebyte.Treatment")

    scale: Literal["binary", "multi_arm", "continuous"]
    source: Literal["randomized", "observational", "instrumental", "quasi-experimental", "adaptive"]
    design: Literal[
        "simple-randomization",
        "block-randomization",
        "cluster-randomization",
        "stepped-wedge",
        "matched-pair",
        "encouragement",
        "adaptive-bandit",
        "business-rule",
        "other",
    ]
    time: Literal["static", "time-varying", "continuous"] = "static"
    time_structure: Literal[
        "none",
        "instantaneous",
        "longitudinal",
        "sequential-adoption",
        "staggered-randomized-adoption",
        "bandit-adaptive",
        "continuous-time",
    ] = "none"
    interference: Literal["none", "partial", "general"] = "none"
    treatment_values: Optional[List[Any]] = None  # e.g. [0, 1] or ["A", "B", "C"]
    control_value: Optional[Any] = None
    propensity: Optional["Propensity"] = None  # forward reference if needed

    @model_validator(mode="after")
    def _validate_compatibility(self) -> "Treatment":
        # 1. Continuous treatments
        if self.scale == "continuous":
            if self.treatment_values is not None:
                raise ValueError("treatment_values must be None for continuous treatments")
            if self.control_value is not None:
                raise ValueError("control_value must be None for continuous treatments")
            if self.propensity is not None:
                raise ValueError("propensity must be None for continuous treatments")

        # 2. Non-continuous treatments (binary / multi_arm) must have treatment_values
        if self.scale != "continuous":
            if self.treatment_values is None:
                raise ValueError(
                    "treatment_values must be provided for non-continuous treatments "
                    f"(scale='{self.scale}')."
                )
            num_vals = len(self.treatment_values)
            if self.scale == "binary" and num_vals != 2:
                raise ValueError(
                    f"Binary treatment must define exactly two treatment_values (got {num_vals})."
                )
            if self.scale == "multi_arm" and num_vals < 3:
                raise ValueError(
                    "Multi-arm treatment must define at least three treatment_values "
                    f"(got {num_vals})."
                )

            # control_value must be provided and be one of treatment_values
            if self.control_value is None:
                raise ValueError(
                    "control_value must be provided for non-continuous treatments "
                    f"(scale='{self.scale}')."
                )
            if self.control_value not in self.treatment_values:
                raise ValueError(
                    "control_value must be one of treatment_values "
                    f"(control_value={self.control_value}, "
                    f"treatment_values={self.treatment_values})."
                )

        # 3. Coherence between time and time_structure
        if self.time == "static" and self.time_structure not in ("none", "instantaneous"):
            raise ValueError(
                "For time='static', time_structure must be 'none' or 'instantaneous' "
                f"(got '{self.time_structure}')."
            )

        if self.time == "continuous" and self.time_structure != "continuous-time":
            raise ValueError(
                "For time='continuous', time_structure must be 'continuous-time' "
                f"(got '{self.time_structure}')."
            )

        return self

    def recommended_methods(self) -> List[str]:
        """
        Return a list of compatible causal estimation methods based on the
        treatment configuration (scale, assignment source, time structure,
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
            scale="binary",
            source="observational",
            design="business-rule",
            time="static",
            time_structure="none",
            interference="none",
            treatment_values=[0, 1],
            control_value=0,
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
            scale="continuous",
            source="observational",
            design="other",
            time="static",
            time_structure="none",
            interference="none",
            # For continuous treatments:
            #   - treatment_values must be None
            #   - control_value must be None
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
        if self.scale in ("binary", "multi_arm"):
            if self.time == "static" and self.interference == "none":
                # Need estimated OR design-known propensities for CATE
                if self.propensity is not None and self.propensity.knowledge in (
                    "estimated",
                    "design-known",
                ):
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
        #   - scale == "continuous"
        #   - static (no longitudinal structure)
        #   - no interference
        #
        # Propensity object is not used for continuous treatments in this schema;
        # nuisance models (m(X,T), e(T|X)) are expected to be estimated internally.
        if self.scale == "continuous" and self.time == "static" and self.interference == "none":
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
            self.scale == "binary"
            and self.source == "randomized"
            and self.time == "static"
            and self.interference == "none"
        ):
            methods.append("uplift")

        # ---------------------- INSTRUMENTAL VARIABLES ---------------------
        if self.source == "instrumental":
            methods.append("iv")

        # -------------------------- G-METHODS ------------------------------
        # time-varying treatments with longitudinal confounding
        if self.time == "time-varying" and self.time_structure == "longitudinal":
            methods.append("g-methods")  # MSM, sequential g-formula, etc.

        # --------------------------- PANEL DiD -----------------------------
        # staggered/quasi-exp adoption + panel time structure
        if self.time_structure in ("sequential-adoption", "staggered-randomized-adoption"):
            methods.append("panel-did")

        # ----------------------------- OPE ---------------------------------
        # adaptive bandit setting
        if self.source == "adaptive":
            methods.append("ope")

        # ----------------------- INTERFERENCE-AWARE METHODS -----------------------
        if self.interference == "partial":
            methods.append("cluster-exposure")
            methods.append("randomization-inference")

        if self.interference == "general":
            methods.append("network-causal")
            methods.append("exposure-mapping")

        return methods
