"""
Feature type module
"""
from featurebyte.enum import StrEnum


class FeatureType(StrEnum):
    """
    Type of the feature
    """

    UNKNOWN = "unknown"
    DICTIONARY = "dictionary"
    LOOKUP = "lookup"
