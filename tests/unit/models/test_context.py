"""
Tests for Context model
"""

from datetime import datetime

from bson import ObjectId

from featurebyte.enum import DBVarType, FeatureType
from featurebyte.models.context import ContextModel, UserProvidedColumn


def test_context_model__entity_ids_set_primary_entity_ids():
    """Test set primary_entity_ids for Context object"""

    entity_id = ObjectId("651e2429604674470bacc80c")
    context_dict = {
        "_id": ObjectId("651e2429604674470bacc80e"),
        "block_modification_by": [],
        "catalog_id": ObjectId("23eda344d0313fb925f7883a"),
        "created_at": datetime(2022, 6, 30, 0, 0),
        "default_eda_table_id": None,
        "default_preview_table_id": None,
        "description": None,
        "entity_ids": [entity_id],
        "graph": None,
        "name": "context_1",
        "node_name": None,
        "updated_at": datetime(2022, 6, 30, 0, 0),
        "user_id": ObjectId("651e2429604674470bacc80d"),
    }

    context = ContextModel(**context_dict)
    assert context.primary_entity_ids == [entity_id]


class TestUserProvidedColumn:
    """Tests for UserProvidedColumn model"""

    def test_user_provided_column__basic(self):
        """Test basic UserProvidedColumn creation"""
        col = UserProvidedColumn(
            name="annual_income", dtype=DBVarType.FLOAT, feature_type=FeatureType.NUMERIC
        )
        assert col.name == "annual_income"
        assert col.dtype == DBVarType.FLOAT
        assert col.feature_type == FeatureType.NUMERIC
        assert col.description is None

    def test_user_provided_column__with_description(self):
        """Test UserProvidedColumn with description"""
        col = UserProvidedColumn(
            name="credit_score",
            dtype=DBVarType.INT,
            feature_type=FeatureType.NUMERIC,
            description="Customer's credit score",
        )
        assert col.name == "credit_score"
        assert col.dtype == DBVarType.INT
        assert col.feature_type == FeatureType.NUMERIC
        assert col.description == "Customer's credit score"

    def test_user_provided_column__serialization(self):
        """Test UserProvidedColumn serialization"""
        col = UserProvidedColumn(
            name="annual_income",
            dtype=DBVarType.FLOAT,
            feature_type=FeatureType.NUMERIC,
            description="Annual income",
        )
        col_dict = col.model_dump()
        assert col_dict == {
            "name": "annual_income",
            "dtype": "FLOAT",
            "feature_type": "numeric",
            "description": "Annual income",
        }

    def test_user_provided_column__from_dict(self):
        """Test UserProvidedColumn deserialization"""
        col_dict = {
            "name": "credit_score",
            "dtype": "INT",
            "feature_type": "numeric",
            "description": "Credit score",
        }
        col = UserProvidedColumn(**col_dict)
        assert col.name == "credit_score"
        assert col.dtype == DBVarType.INT
        assert col.feature_type == FeatureType.NUMERIC
        assert col.description == "Credit score"


class TestContextModelWithUserProvidedColumns:
    """Tests for ContextModel with user_provided_columns"""

    def test_context_model__empty_user_provided_columns(self):
        """Test ContextModel with default empty user_provided_columns"""
        entity_id = ObjectId("651e2429604674470bacc80c")
        context = ContextModel(
            _id=ObjectId("651e2429604674470bacc80e"),
            name="context_1",
            primary_entity_ids=[entity_id],
            catalog_id=ObjectId("23eda344d0313fb925f7883a"),
            user_id=ObjectId("651e2429604674470bacc80d"),
        )
        assert context.user_provided_columns == []

    def test_context_model__with_user_provided_columns(self):
        """Test ContextModel with user_provided_columns"""
        entity_id = ObjectId("651e2429604674470bacc80c")
        user_provided_columns = [
            UserProvidedColumn(
                name="annual_income", dtype=DBVarType.FLOAT, feature_type=FeatureType.NUMERIC
            ),
            UserProvidedColumn(
                name="credit_score",
                dtype=DBVarType.INT,
                feature_type=FeatureType.NUMERIC,
                description="Credit score",
            ),
        ]
        context = ContextModel(
            _id=ObjectId("651e2429604674470bacc80e"),
            name="context_1",
            primary_entity_ids=[entity_id],
            catalog_id=ObjectId("23eda344d0313fb925f7883a"),
            user_id=ObjectId("651e2429604674470bacc80d"),
            user_provided_columns=user_provided_columns,
        )
        assert len(context.user_provided_columns) == 2
        assert context.user_provided_columns[0].name == "annual_income"
        assert context.user_provided_columns[0].dtype == DBVarType.FLOAT
        assert context.user_provided_columns[0].feature_type == FeatureType.NUMERIC
        assert context.user_provided_columns[1].name == "credit_score"
        assert context.user_provided_columns[1].dtype == DBVarType.INT
        assert context.user_provided_columns[1].feature_type == FeatureType.NUMERIC

    def test_context_model__serialization_with_user_provided_columns(self):
        """Test ContextModel serialization with user_provided_columns"""
        entity_id = ObjectId("651e2429604674470bacc80c")
        user_provided_columns = [
            UserProvidedColumn(
                name="annual_income", dtype=DBVarType.FLOAT, feature_type=FeatureType.NUMERIC
            ),
        ]
        context = ContextModel(
            _id=ObjectId("651e2429604674470bacc80e"),
            name="context_1",
            primary_entity_ids=[entity_id],
            catalog_id=ObjectId("23eda344d0313fb925f7883a"),
            user_id=ObjectId("651e2429604674470bacc80d"),
            user_provided_columns=user_provided_columns,
        )
        context_dict = context.model_dump()
        assert context_dict["user_provided_columns"] == [
            {
                "name": "annual_income",
                "dtype": "FLOAT",
                "feature_type": "numeric",
                "description": None,
            }
        ]
