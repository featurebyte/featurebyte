"""
Feature and FeatureList classes
"""
from __future__ import annotations

from featurebyte.core.frame import Frame
from featurebyte.core.series import Series


class FeatureMixin:
    pass


class Feature(FeatureMixin, Series):
    pass


class FeatureList(FeatureMixin, Frame):
    pass
