"""
Test DataUpdateService
"""
from unittest.mock import patch

import pytest
import pytest_asyncio
from bson.objectid import ObjectId

from featurebyte.exception import DocumentUpdateError
from featurebyte.models.entity import ParentEntity
from featurebyte.models.feature_store import DataStatus
from featurebyte.schema.dimension_data import DimensionDataServiceUpdate
from featurebyte.schema.entity import EntityCreate, EntityServiceUpdate
from featurebyte.schema.event_data import EventDataServiceUpdate
from featurebyte.schema.item_data import ItemDataServiceUpdate
from featurebyte.schema.scd_data import SCDDataServiceUpdate


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "from_status,to_status,is_valid",
    [
        (DataStatus.DRAFT, None, True),
        (DataStatus.DRAFT, DataStatus.DRAFT, True),
        (DataStatus.DRAFT, DataStatus.PUBLISHED, True),
        (DataStatus.PUBLISHED, DataStatus.DEPRECATED, True),
        (DataStatus.DRAFT, DataStatus.DEPRECATED, False),
        (DataStatus.PUBLISHED, DataStatus.DRAFT, False),
        (DataStatus.DEPRECATED, DataStatus.DRAFT, False),
    ],
)
async def test_update_data_status(
    data_update_service, event_data_service, event_data, from_status, to_status, is_valid
):
    """Test update_data_status"""
    # setup event data status for testing
    await event_data_service.update_document(
        document_id=event_data.id, data=EventDataServiceUpdate(status=from_status)
    )
    doc = await event_data_service.get_document(document_id=event_data.id)
    assert doc.status == from_status
    if is_valid:
        await data_update_service.update_data_status(
            service=event_data_service,
            document_id=event_data.id,
            data=EventDataServiceUpdate(status=to_status),
        )
        doc = await event_data_service.get_document(document_id=event_data.id)
        assert doc.status == (to_status or from_status)
    else:
        with pytest.raises(DocumentUpdateError) as exc:
            await data_update_service.update_data_status(
                service=event_data_service,
                document_id=event_data.id,
                data=EventDataServiceUpdate(status=to_status),
            )
        assert f"Invalid status transition from {from_status} to {to_status}" in str(exc.value)


@pytest.mark.asyncio
async def test_update_columns_info(
    data_update_service, event_data_service, entity_service, semantic_service, event_data, entity
):
    """Test update_columns_info"""
    _ = entity
    new_entity = await entity_service.create_document(
        data=EntityCreate(name="an_entity", serving_name="a_name")
    )
    other_data_id = ObjectId("6332fdb21e8f0aabbb414512")
    await entity_service.update_document(
        document_id=new_entity.id,
        data=EntityServiceUpdate(
            tabular_data_ids=[other_data_id],
            primary_tabular_data_ids=[other_data_id],
        ),
    )
    new_semantic = await semantic_service.get_or_create_document(name="a_semantic")
    columns_info = event_data.dict()["columns_info"]
    columns_info[0]["entity_id"] = new_entity.id
    columns_info[0]["semantic_id"] = new_semantic.id

    # update columns info
    await data_update_service.update_columns_info(
        service=event_data_service,
        document_id=event_data.id,
        data=EventDataServiceUpdate(columns_info=columns_info),
    )

    # check the updated column info
    updated_doc = await event_data_service.get_document(document_id=event_data.id)
    assert updated_doc.columns_info[0].entity_id == new_entity.id
    assert updated_doc.columns_info[0].semantic_id == new_semantic.id

    # check dataset is tracked in entity
    new_entity = await entity_service.get_document(document_id=new_entity.id)
    assert new_entity.tabular_data_ids == [other_data_id, event_data.id]
    assert new_entity.primary_tabular_data_ids == [other_data_id, event_data.id]

    # move entity to non-primary key
    columns_info[0]["entity_id"] = None
    columns_info[1]["entity_id"] = new_entity.id
    await data_update_service.update_columns_info(
        service=event_data_service,
        document_id=event_data.id,
        data=EventDataServiceUpdate(columns_info=columns_info),
    )
    new_entity = await entity_service.get_document(document_id=new_entity.id)
    assert new_entity.tabular_data_ids == [other_data_id, event_data.id]
    assert new_entity.primary_tabular_data_ids == [other_data_id]

    # remove entity
    columns_info[1]["entity_id"] = None
    await data_update_service.update_columns_info(
        service=event_data_service,
        document_id=event_data.id,
        data=EventDataServiceUpdate(columns_info=columns_info),
    )
    new_entity = await entity_service.get_document(document_id=new_entity.id)
    assert new_entity.tabular_data_ids == [other_data_id]
    assert new_entity.primary_tabular_data_ids == [other_data_id]

    # test unknown entity ID
    unknown_id = ObjectId()
    columns_info[0]["entity_id"] = unknown_id
    with pytest.raises(DocumentUpdateError) as exc:
        await data_update_service.update_columns_info(
            service=event_data_service,
            document_id=event_data.id,
            data=EventDataServiceUpdate(columns_info=columns_info),
        )

    expected_msg = f"Entity IDs ['{unknown_id}'] not found for columns ['col_int']"
    assert expected_msg in str(exc.value)

    # test unknown semantic ID
    columns_info[0]["entity_id"] = None
    columns_info[0]["semantic_id"] = unknown_id
    with pytest.raises(DocumentUpdateError) as exc:
        await data_update_service.update_columns_info(
            service=event_data_service,
            document_id=event_data.id,
            data=EventDataServiceUpdate(columns_info=columns_info),
        )

    expected_msg = f"Semantic IDs ['{unknown_id}'] not found for columns ['col_int']"
    assert expected_msg in str(exc.value)


@pytest.mark.asyncio
async def test_update_columns_info__critical_data_info(
    data_update_service, event_data_service, event_data, entity
):
    """Test update_columns_info (critical data info)"""
    # prepare columns info by adding critical data info & removing all entity ids & semantic ids
    _ = entity
    event_data_doc = event_data.dict()
    columns_info = event_data_doc["columns_info"]
    columns_info[0]["critical_data_info"] = {
        "cleaning_operations": [{"type": "missing", "imputed_value": 0}]
    }
    columns_info = [{**col, "entity_id": None, "semantic_id": None} for col in columns_info]

    # update columns info
    await data_update_service.update_columns_info(
        service=event_data_service,
        document_id=event_data.id,
        data=EventDataServiceUpdate(columns_info=columns_info),
    )

    # check the updated document
    updated_doc = await event_data_service.get_document(document_id=event_data.id)
    assert updated_doc.columns_info == columns_info
    assert updated_doc.node_name == "graph_1"
    assert updated_doc.graph.nodes[0] == event_data_doc["graph"]["nodes"][0]
    assert updated_doc.graph.nodes[1].parameters == {
        "graph": {
            "edges": [
                {"source": "proxy_input_1", "target": "project_1"},
                {"source": "project_1", "target": "is_null_1"},
                {"source": "project_1", "target": "conditional_1"},
                {"source": "is_null_1", "target": "conditional_1"},
                {"source": "proxy_input_1", "target": "assign_1"},
                {"source": "conditional_1", "target": "assign_1"},
            ],
            "nodes": [
                {
                    "name": "proxy_input_1",
                    "type": "proxy_input",
                    "output_type": "frame",
                    "parameters": {"input_order": 0},
                },
                {
                    "name": "project_1",
                    "type": "project",
                    "output_type": "series",
                    "parameters": {"columns": ["col_int"]},
                },
                {"name": "is_null_1", "type": "is_null", "output_type": "series", "parameters": {}},
                {
                    "name": "conditional_1",
                    "type": "conditional",
                    "output_type": "series",
                    "parameters": {"value": 0},
                },
                {
                    "name": "assign_1",
                    "type": "assign",
                    "output_type": "frame",
                    "parameters": {"name": "col_int", "value": None},
                },
            ],
        },
        "output_node_name": "assign_1",
        "type": "cleaning",
    }

    # test remove critical data info
    columns_info[0]["critical_data_info"] = {"cleaning_operations": []}
    await data_update_service.update_columns_info(
        service=event_data_service,
        document_id=event_data.id,
        data=EventDataServiceUpdate(columns_info=columns_info),
    )
    # check the updated document
    updated_doc = await event_data_service.get_document(document_id=event_data.id)
    assert updated_doc.columns_info == columns_info
    assert updated_doc.node_name == "input_1"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fixture_name,update_class,primary_key_column",
    [
        ("snowflake_event_data", EventDataServiceUpdate, "event_id_column"),
        ("snowflake_scd_data", SCDDataServiceUpdate, "surrogate_key_column"),
        ("snowflake_item_data", ItemDataServiceUpdate, "item_id_column"),
        ("snowflake_dimension_data", DimensionDataServiceUpdate, "dimension_id_column"),
    ],
)
async def test_update_entity_data_references(
    request, data_update_service, fixture_name, update_class, primary_key_column
):
    """Test update_entity_data_references"""
    data_model = request.getfixturevalue(fixture_name)
    columns_info = data_model.json_dict()["columns_info"]
    primary_index = [
        i
        for i, c in enumerate(data_model.columns_info)
        if c.name == getattr(data_model, primary_key_column)
    ][0]
    columns_info[primary_index]["entity_id"] = ObjectId()
    data = update_class(columns_info=columns_info)
    with patch.object(data_update_service.entity_service, "get_document") as mock_get_document:
        mock_get_document.return_value.tabular_data_ids = []
        mock_get_document.return_value.primary_tabular_data_ids = []
        with patch.object(
            data_update_service.entity_service, "update_document"
        ) as mock_update_document:
            with patch.object(data_update_service, "_update_entity_relationship"):
                await data_update_service.update_entity_data_references(
                    document=data_model, data=data
                )
                update_payload = mock_update_document.call_args[1]["data"]
                assert update_payload.tabular_data_ids == [data_model.id]
                assert update_payload.primary_tabular_data_ids == [data_model.id]


@pytest_asyncio.fixture(name="entity_foo")
async def entity_foo_fixture(entity_service):
    """Entity foo"""
    entity = await entity_service.create_document(data=EntityCreate(name="foo", serving_name="foo"))
    return entity


@pytest_asyncio.fixture(name="entity_bar")
async def entity_bar_fixture(entity_service):
    """Entity bar"""
    entity = await entity_service.create_document(data=EntityCreate(name="bar", serving_name="bar"))
    return entity


@pytest_asyncio.fixture(name="entity_baz")
async def entity_baz_fixture(entity_service):
    """Entity baz"""
    entity = await entity_service.create_document(data=EntityCreate(name="baz", serving_name="baz"))
    return entity


@pytest_asyncio.fixture(name="entity_qux")
async def entity_qux_fixture(entity_service):
    """Entity qux"""
    entity = await entity_service.create_document(data=EntityCreate(name="qux", serving_name="qux"))
    return entity


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "old_primary_entities,old_parent_entities,new_primary_entities,new_parent_entities,"
    "expected_old_primary_parents,expected_new_primary_parents",
    [
        # data has no entity at all
        (set(), set(), set(), set(), [], []),
        # data has no primary entity
        (set(), {"foo"}, set(), {"bar"}, [], []),
        # data has no changes in entities
        ({"foo"}, {"bar"}, {"foo"}, {"bar"}, ["bar"], ["bar"]),
        # add a new primary entity
        (set(), {"baz"}, {"foo"}, {"baz"}, [], ["baz"]),
        # add a new parent entity
        ({"foo"}, set(), {"foo"}, {"bar"}, ["bar"], ["bar"]),
        ({"foo"}, {"bar"}, {"foo"}, {"bar", "baz"}, ["bar", "baz"], ["bar", "baz"]),
        # remove a parent entity
        ({"foo"}, {"bar"}, {"foo"}, set(), [], []),
        ({"foo"}, {"bar", "baz"}, {"foo"}, {"bar"}, ["bar"], ["bar"]),
        # change of primary entity from foo to bar
        ({"foo"}, {"baz"}, {"bar"}, {"baz"}, [], ["baz"]),
        # change of parent entity from bar to baz
        ({"foo"}, {"bar"}, {"foo"}, {"baz"}, ["baz"], ["baz"]),
        # change of primary & parent entity
        ({"foo"}, {"bar"}, {"baz"}, {"qux"}, [], {"qux"}),
    ],
)
async def test_update_entity_relationship(  # pylint: disable=too-many-arguments
    data_update_service,
    entity_service,
    event_data,
    entity_foo,
    entity_bar,
    entity_baz,
    entity_qux,
    old_primary_entities,
    old_parent_entities,
    new_primary_entities,
    new_parent_entities,
    expected_old_primary_parents,
    expected_new_primary_parents,
):
    """Test _update_entity_relationship"""

    def convert_entity_name_to_ids(entity_names, entity_docs):
        # convert list of entity names to entity ids
        entity_name_id_map = {entity.name: entity.id for entity in entity_docs}
        return set(entity_name_id_map[name] for name in entity_names)

    # setup old primary entity
    entities = [entity_foo, entity_bar, entity_baz, entity_qux]
    old_primary_entities = convert_entity_name_to_ids(old_primary_entities, entities)
    old_parent_entities = convert_entity_name_to_ids(old_parent_entities, entities)
    new_primary_entities = convert_entity_name_to_ids(new_primary_entities, entities)
    new_parent_entities = convert_entity_name_to_ids(new_parent_entities, entities)
    for entity_id in old_primary_entities:
        await entity_service.update_document(
            document_id=entity_id,
            data=EntityServiceUpdate(
                parents=[
                    ParentEntity(id=parent_id, data_type="event_data", data_id=event_data.id)
                    for parent_id in old_parent_entities
                ]
            ),
        )

    # call update entity relationship
    await data_update_service._update_entity_relationship(
        document=event_data,
        old_primary_entities=old_primary_entities,
        old_parent_entities=old_parent_entities,
        new_primary_entities=new_primary_entities,
        new_parent_entities=new_parent_entities,
    )

    # check the output of the old & new primary entity
    def key_sorter(parent_entity):
        # helper function used to sort list of parent entity
        return parent_entity.id

    for entity_id in old_primary_entities:
        primary_entity = await entity_service.get_document(document_id=entity_id)
        expected_parent_ids = convert_entity_name_to_ids(expected_old_primary_parents, entities)
        expected_parents = [
            ParentEntity(id=entity_id, data_type="event_data", data_id=event_data.id)
            for entity_id in expected_parent_ids
        ]
        assert sorted(primary_entity.parents, key=key_sorter) == sorted(
            expected_parents, key=key_sorter
        )

    for entity_id in new_primary_entities:
        primary_entity = await entity_service.get_document(document_id=entity_id)
        expected_parent_ids = convert_entity_name_to_ids(expected_new_primary_parents, entities)
        expected_parents = [
            ParentEntity(id=entity_id, data_type="event_data", data_id=event_data.id)
            for entity_id in expected_parent_ids
        ]
        assert sorted(primary_entity.parents, key=key_sorter) == sorted(
            expected_parents, key=key_sorter
        )
