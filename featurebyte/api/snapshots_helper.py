from typing import Optional

from featurebyte.typing import OffsetType


def validate_offset_for_view(offset: Optional[OffsetType], view_type_name: str) -> None:
    """
    Validate the offset parameter in as_features and as_target for a SnapshotsView like view.

    Parameters
    ----------
    offset: Optional[OffsetType]
        Offset for lookup feature / target. For SnapshotsView, should be an integer
        specifying the number of time interval steps.
    view_type_name: str
        View type name to be used in error messages, e.g. "SnapshotsView"

    Raises
    ------
    ValueError
        If offset is invalid
    """
    if offset is None:
        return
    if isinstance(offset, str):
        raise ValueError(
            f"String offset is not supported for {view_type_name}. Use integer offset instead."
        )
    if not isinstance(offset, int):
        raise ValueError(
            f"Offset for {view_type_name} must be an integer specifying the number of time interval steps."
        )
    if offset < 0:
        raise ValueError(f"Offset for {view_type_name} must be a non-negative integer.")
