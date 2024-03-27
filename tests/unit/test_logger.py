"""
Test Logger
"""
import logging

from featurebyte.logging import get_logger

logger = get_logger(__name__)


def test_logging(mock_log_handler):
    """
    Test basic logging works
    """
    logger.addHandler(mock_log_handler)
    logger.setLevel(logging.DEBUG)  # ???
    logger.debug("Test Message", extra={"a": 1})

    # check logging format looks like:
    # 2022-06-20 14:33:41.328 | DEBUG    | Test Message | {'extra': {'a': 1}}
    assert len(mock_log_handler.records) == 1
    parts = mock_log_handler.records[0].split("|")
    assert (
        "|".join(parts[1:])
        == " DEBUG    | tests.unit.test_logger | test_logging:17 | Test Message | {'a': 1}"
    )
