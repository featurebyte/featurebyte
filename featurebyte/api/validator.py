"""
Validator module
"""
from featurebyte.api.scd_view import SlowlyChangingView
from featurebyte.api.view import View
from featurebyte.exception import JoinViewMismatchError


def validate_view(other_view: View) -> None:
    """
    Validates that the other view is not a SlowlyChangingView.
    We can remove this once we are able to generate the SQL correctly for joining with SlowlyChangingViews..

    Parameters
    ----------
    other_view: View
        view being joined with

    Raises
    ------
    JoinViewMismatchError
        raised when the other view is not a SlowlyChangingView
    """
    if isinstance(other_view, SlowlyChangingView):
        raise JoinViewMismatchError
