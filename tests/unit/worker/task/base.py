"""
Base class for testing worker tasks
"""
import json

import pytest


class BaseTaskTestSuite:
    """
    Test suite for worker tasks
    """

    # class variables to be set at metaclass
    task_class = None
    payload_filename = None

    @staticmethod
    def load_payload(filename):
        """Load payload"""
        with open(filename) as fhandle:
            return json.loads(fhandle.read())
