"""Featurebyte-specific warnings."""


class QueryNoLimitWarning(RuntimeWarning):
    """
    A Featurebyte specific warning for running queries without a limit.
    """
