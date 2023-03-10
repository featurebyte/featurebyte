"""
Utilities for udf testing
"""
import pytest

import tests.integration.udf.snowflake.util as snowflake_util
import tests.integration.udf.spark.util as spark_util


@pytest.fixture(name="to_object", scope="session")
def to_object_fixture(source_type):
    """
    Get function to construct a Map object from a dict for the given source_type from
    """
    if source_type == "snowflake":
        return snowflake_util.to_object
    if source_type == "spark":
        return spark_util.to_object
    raise NotImplementedError()
