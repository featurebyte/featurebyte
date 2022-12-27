"""
Feature type module
"""
from featurebyte.enum import StrEnum


class FeatureType(StrEnum):

    UNKNOWN = "unknown"
    DICTIONARY = "dictionary"
    LOOKUP = "lookup"
