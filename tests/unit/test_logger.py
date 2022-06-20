"""
Test Logger
"""
import logging
from datetime import datetime

from featurebyte.logger import logger


class MockLogHandler(logging.Handler):
    """
    Mock LogHandler to record logs for testing
    """

    records = []

    def emit(self, record):
        self.records.append(record.getMessage())


mock_handler = MockLogHandler()
logger.add(mock_handler)


def test_logging():
    """
    Test basic logging works
    """
    curtime = datetime.utcnow()
    mock_handler.records.clear()
    logger.debug("Test Message", extra={"a": 1})

    # check logging format looks like:
    # 2022-06-20 14:33:41.328 | DEBUG    | Test Message | {'extra': {'a': 1}}
    assert len(mock_handler.records) == 1
    parts = mock_handler.records[0].split("|")
    assert datetime.strptime(parts.pop(0).strip(), "%Y-%m-%d %H:%M:%S.%f") > curtime
    assert "|".join(parts) == " DEBUG    | test_logger:test_logging:26 - Test Message"
