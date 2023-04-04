"""
Tests for functions in tile_util.py module
"""
from unittest.mock import patch

import pytest

from featurebyte import SourceType
from featurebyte.common import tile_util
from featurebyte.tile.base import BaseTileManager


def test_tile_manager_from_session():
    """Test Derive implementing TileManager instance based on input sessions"""
    with patch("featurebyte.session.base.BaseSession") as mock_base_session:
        mock_base_session.source_type = SourceType.SNOWFLAKE
        tile_manger = tile_util.tile_manager_from_session(mock_base_session)
        assert isinstance(tile_manger, BaseTileManager)

        mock_base_session.source_type = SourceType.SPARK
        tile_manger = tile_util.tile_manager_from_session(mock_base_session)
        assert isinstance(tile_manger, BaseTileManager)

        with pytest.raises(ValueError) as excinfo:
            mock_base_session.source_type = SourceType.TEST
            tile_util.tile_manager_from_session(mock_base_session)

        assert "Tile Manager for test has not been implemented" in str(excinfo.value)
