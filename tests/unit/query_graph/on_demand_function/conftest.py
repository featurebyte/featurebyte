"""
This module contains common fixtures for on-demand function tests
"""

import pytest

from featurebyte.enum import DBVarType, SourceType
from featurebyte.query_graph.node.metadata.config import (
    OnDemandFunctionCodeGenConfig,
    OnDemandViewCodeGenConfig,
)


@pytest.fixture(name="odfv_config")
def fixture_odfv_config():
    """Fixture for the ODFV config"""
    # set a high limit to avoid the expression being split into multiple statements
    return OnDemandViewCodeGenConfig(max_expression_length=180, source_type=SourceType.SNOWFLAKE)


@pytest.fixture(name="udf_config")
def fixture_udf_config():
    """Fixture for the UDF config"""
    # set a high limit to avoid the expression being split into multiple statements
    return OnDemandFunctionCodeGenConfig(output_dtype=DBVarType.FLOAT, max_expression_length=180)
