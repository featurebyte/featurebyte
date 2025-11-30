"""
Treatment model
"""

from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel

from featurebyte.enum import StrEnum


class TreatmentScale(StrEnum):
    """Scale of treatment Variable"""

    BINARY = "binary"
    # Treatment is 0/1 (e.g., exposed vs not, coupon vs no coupon).
    # Supports uplift modeling, meta-learners, and IV binary models.

    MULTI_ARM = "multi_arm"
    # Treatment takes discrete >2 ordered or unordered values
    # (e.g., low/medium/high dosage tier, 3-level product version).
    # Requires generalized propensity models; uplift-style models do NOT apply.

    CONTINUOUS = "continuous"
    # Treatment is numeric with a dose (price, spend, dosage).
    # Requires dose–response or continuous treatment effect estimators
    # (DRLearner, ForestDML, ContinuousTreatmentEffect).


class AssignmentSource(StrEnum):
    """High-level source of treatement assignment. Determines identification strategy"""

    RANDOMIZED = "randomized"
    # Treatment assigned by explicit randomization.
    # Safe for uplift modeling; ignorability is guaranteed.

    OBSERVATIONAL = "observational"
    # Treatment chosen by behavior or business process.
    # Requires ignorability + overlap; uplift is NOT generally valid.

    INSTRUMENTAL = "instrumental"
    # Treatment is endogenous but there exists an exogenous instrument Z.
    # Identifies LATE, not global uplift. Requires IV estimators.

    QUASI_EXPERIMENTAL = "quasi-experimental"
    # Identification from natural experiments (RD, DiD, staggered adoption).
    # CATE possible, uplift rarely meaningful.

    ADAPTIVE = "adaptive"
    # Assignment depends on previous outcomes (bandits, RL, dynamic policies).
    # Violates ignorability; requires off-policy evaluation (OPE).


class AssignmentDesign(StrEnum):
    """Specific Assignment design. More granular details inside each Source category."""

    SIMPLE_RANDOMIZATION = "simple-randomization"
    # Independent randomization of units (standard A/B test).

    BLOCK_RANDOMIZATION = "block-randomization"
    # Randomization within strata (gender blocks, region blocks).
    # Requires block-adjusted estimators / FE.

    CLUSTER_RANDOMIZATION = "cluster-randomization"
    # Clusters (stores, schools, users grouped by geography) are randomized.
    # Requires cluster-aware models and SE correction.

    STEPPED_WEDGE = "stepped-wedge"
    # Clusters switch from control→treatment over time in randomized order.
    # Equivalent to STAGGERED_RANDOMIZED_ADOPTION in TreatmentTimeStructure.

    MATCHED_PAIR = "matched-pair"
    # Paired or matched units randomized within pairs (matched RCT).

    ENCOURAGEMENT = "encouragemnt"
    # Instrumental variable design where random "encouragement" affects T
    # but not Y directly. Produces LATE.

    ADAPTIVE_BANDIT = "adaptive-bandit"
    # Allocation probabilities updated over time based on reward estimates.
    # Requires OPE; uplift invalid.

    BUSINESS_RULE = "business-rule"
    # Deterministic rule-based assignment (tiers, thresholds, rollout order).
    # Used in SEQUENTIAL_ADOPTION Quasi-Experimental settings.

    OTHER = "other"
    # Other assignment design


class TreatmentTime(StrEnum):
    """Time axis. How treament evolves with time"""

    STATIC = "static"
    # One-shot assignment; each unit receives T once (A/B tests).
    # Compatible with standard uplift and CATE.

    TIME_VARYING = "time-varying"
    # Discrete-time longitudinal exposures (weekly ads, pricing over time).
    # Requires MSM / g-methods; uplift invalid.

    CONTINUOUS = "continuous"
    # Treatment is defined in continuous time (hazard rate, intensity, dosage flow).
    # Requires survival or continuous-time causal models.


class TreatmentTimeStructure(StrEnum):
    """Detailed Treatment Time Structure. Determines modeling strategy."""

    NONE = "none"
    # No meaningful temporal structure; single snapshot.

    INSTANTANEOUS = "instantaneous"
    # Single assignment at a single timepoint (classic A/B test).

    LONGITUDINAL = "longitudinal"
    # Multiple treatment decisions per unit over time.
    # TreatmentTime-varying confounding; requires sequential causal inference.

    SEQUENTIAL_ADOPTION = "sequential-adoption"
    # Non-random rollout over time (regions launched in fixed order).
    # Identified via quasi-experimental DiD / event-study.

    STAGGERED_RANDOMIZED_ADOPTION = "staggered-randomized-adoption"
    # Randomized adoption time (stepped-wedge RCT).
    # Must model cluster and time fixed effects.

    BANDIT_ADAPTIVE = "bandit-adaptive"
    # Assignment policy updates based on past outcomes.
    # Requires off-policy evaluation (IPS / DR-OPE).

    CONTINUOUS_TIME = "continuous-time"
    # Treatment varies continuously with time (process-based exposure).


class TreatmentInterference(StrEnum):
    """Treatment Interference structure. Violation of Stable Unit Treatment Value Assumption (SUTVA)."""

    NONE = "none"
    # Each unit’s outcome depends only on its own treatment (SUTVA holds).
    # Standard causal estimators are valid.

    PARTIAL = "partial"
    # Spillovers exist but within known structure (clusters, networks).
    # Requires cluster / network interference models.

    GENERAL = "general"
    # Arbitrary interference; standard identification fails.
    # Requires specialized causal graph / network methods.


class PropensityGranularity(StrEnum):
    """Treatment Propensity Granularity"""

    GLOBAL = "global"
    # Same p(T | ⋅) for everyone (e.g., 50/50 A/B).

    BLOCK = "block"
    # p(T | block) constant within blocks/strata, varies across them.

    CLUSTER = "cluster"
    # p(T | cluster) constant within clusters.

    UNIT = "unit"
    # p(T | X_i) varies by unit (observational, adaptive policies).

    DETERMINISTIC = "deterministic"
    # Given X, treatment is essentially 0/1 (threshold rules, hard tiers).


class PropensityKnowledge(StrEnum):
    """Knowledge of Treatment Propensity"""

    DESIGN_KNOWN = "design-known"
    # Analytically known from the experiment design or policy spec.

    ESTIMATED = "estimated"
    # From a model p(T | X) (pscore, ML).

    NOT_AVAILABLE = "not-available"
    # We neither know nor estimate p(T | ⋅).


class PropensityModel(BaseModel):
    granularity: PropensityGranularity
    knowledge: PropensityKnowledge
    p_global: Optional[Union[float, Dict[Any, float]]] = (
        None  # Only meaningful if granularity == GLOBAL
    )


class TreatmentModel(BaseModel):
    scale: TreatmentScale
    source: AssignmentSource
    design: AssignmentDesign
    time: TreatmentTime = TreatmentTime.STATIC
    time_structure: TreatmentTimeStructure = TreatmentTimeStructure.NONE
    interference: TreatmentInterference = TreatmentInterference.NONE
    treatment_values: Optional[List[Any]] = None  # e.g. [0, 1] or ["A", "B", "C"]
    treatment_labels: Optional[Dict[Any, str]] = None  # optional human-readable names
    control_value: Optional[Any] = None
    propensity: Optional[PropensityModel] = None
