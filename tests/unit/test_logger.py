"""
Test Logger
"""
import logging

from featurebyte.logging import CONSOLE_LOG_FORMATTER, get_logger

logger = get_logger(__name__)


class MockLogHandler(logging.Handler):
    """
    Mock LogHandler to record logs for testing
    """

    records = []

    def emit(self, record):
        self.records.append(self.format(record))


def test_logging():
    """
    Test basic logging works
    """
    mock_handler = MockLogHandler()
    mock_handler.setFormatter(CONSOLE_LOG_FORMATTER)
    logger.addHandler(mock_handler)
    logger.setLevel(logging.DEBUG)
    mock_handler.records.clear()
    logger.debug("Test Message", extra={"a": 1})

    # check logging format looks like:
    # 2022-06-20 14:33:41.328 | DEBUG    | Test Message | {'extra': {'a': 1}}
    assert len(mock_handler.records) == 1
    parts = mock_handler.records[0].split("|")
    assert (
        "|".join(parts[1:])
        == " DEBUG    | tests.unit.test_logger | test_logging:31 | Test Message | {'a': 1}"
    )
