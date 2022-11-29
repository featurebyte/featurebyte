"""
Validator module
"""
from featurebyte.api.dimension_view import DimensionView
from featurebyte.api.scd_view import SlowlyChangingView
from featurebyte.api.view import View
from featurebyte.exception import JoinViewMismatchError


def validate_view(other_view: View) -> None:
    """
    Validates that the other view being joined with is either a SlowlyChangingView, or a DimensionView.

    Parameters
    ----------
    other_view: View
        view being joined with

    Raises
    ------
    JoinViewMismatchError
       raised when the other view is not a SlowlyChangingView, or a DimensionView
    """
    if not isinstance(other_view, SlowlyChangingView) and not isinstance(other_view, DimensionView):
        raise JoinViewMismatchError
