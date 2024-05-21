"""
Exception classes used within services
"""


class OfflineStoreFeatureTableBadStateError(Exception):
    """
    OfflineStoreFeatureTableBadStateError

    This exception is raised when an offline store feature table document is being updated but is in
    a bad state and cannot be recovered. This should only happen rarely and should be investigated.
    """
