"""
Tests for RelationshipService class
"""
import pytest
import pytest_asyncio

from featurebyte.exception import DocumentUpdateError
from featurebyte.models.base import DEFAULT_WORKSPACE_ID
from featurebyte.models.relationship import Parent, Relationship
from featurebyte.service.base_document import BaseDocumentService
from featurebyte.service.relationship import RelationshipService


# create concrete classes for testing
class FamilyModel(Relationship):
    """FamilyModel class"""

    class Settings:
        """Settings class"""

        collection_name = "family"
        unique_constraints = []


class FamilyDocumentService(BaseDocumentService):
    """FamilyDocumentService"""

    document_class = FamilyModel

    async def create_document(self, data):
        insert_id = await self.persistent.insert_one(
            collection_name=self.collection_name,
            document=data.dict(by_alias=True),
            user_id=self.user.id,
        )
        return await self.get_document(document_id=insert_id)

    async def update_document(
        self,
        document_id,
        data,
        exclude_none=True,
        document=None,
        return_document=True,
    ):
        await self.persistent.update_one(
            collection_name=self.collection_name,
            query_filter=self._construct_get_query_filter(document_id=document_id),
            update={"$set": data.dict(exclude_none=exclude_none)},
            user_id=self.user.id,
        )
        if return_document:
            return await self.get_document(document_id=document_id)
        return None


class FamilyRelationshipService(RelationshipService):
    """FamilyRelationshipService class"""

    @property
    def document_service(self):
        return FamilyDocumentService(
            user=self.user, persistent=self.persistent, workspace_id=DEFAULT_WORKSPACE_ID
        )

    @classmethod
    def prepare_document_update_payload(cls, ancestor_ids, parents):
        return Relationship(ancestor_ids=ancestor_ids, parents=parents)


@pytest.fixture(name="family_document_service")
def family_document_service_fixture(user, persistent):
    """FamilyDocumentService object"""
    return FamilyDocumentService(
        user=user, persistent=persistent, workspace_id=DEFAULT_WORKSPACE_ID
    )


@pytest.fixture(name="family_relationship_service")
def family_relationship_service_fixture(user, persistent):
    """FamilyRelationshipService object"""
    return FamilyRelationshipService(
        user=user, persistent=persistent, workspace_id=DEFAULT_WORKSPACE_ID
    )


@pytest_asyncio.fixture(name="object_a")
async def object_a_fixture(family_document_service):
    """Object A fixture"""
    return await family_document_service.create_document(data=FamilyModel(name="object_a"))


@pytest_asyncio.fixture(name="object_b")
async def object_b_fixture(family_document_service):
    """Object B fixture"""
    return await family_document_service.create_document(data=FamilyModel(name="object_b"))


@pytest_asyncio.fixture(name="object_c")
async def object_c_fixture(family_document_service):
    """Object C fixture"""
    return await family_document_service.create_document(data=FamilyModel(name="object_c"))


@pytest_asyncio.fixture(name="object_d")
async def object_d_fixture(family_document_service):
    """Object D fixture"""
    return await family_document_service.create_document(data=FamilyModel(name="object_d"))


@pytest.mark.asyncio
async def test_add_relationship__both_parent_and_child(family_relationship_service, object_a):
    """Test add_relationship - parent & child are the same object"""
    with pytest.raises(DocumentUpdateError) as exc:
        await family_relationship_service.add_relationship(
            parent=Parent(id=object_a.id), child_id=object_a.id
        )
    assert 'Object "object_a" cannot be both parent & child.' in str(exc.value)


@pytest.mark.asyncio
async def test_add_relationship__cycle_relationship(
    family_relationship_service, object_a, object_b
):
    """Test add_relationship - cycle relationship detection"""
    await family_relationship_service.add_relationship(
        parent=Parent(id=object_a.id), child_id=object_b.id
    )
    with pytest.raises(DocumentUpdateError) as exc:
        await family_relationship_service.add_relationship(
            parent=Parent(id=object_b.id), child_id=object_a.id
        )
    expected = (
        'Object "object_b" should not be the parent of object "object_a" as '
        'object "object_a" is already an ancestor of object "object_b".'
    )
    assert expected in str(exc.value)


@pytest.mark.asyncio
async def test_add_relationship__duplicated_relationship(
    family_relationship_service, object_a, object_b
):
    """Test add_relationship when the ancestor & descendant relationship already established"""
    await family_relationship_service.add_relationship(
        parent=Parent(id=object_a.id), child_id=object_b.id
    )
    with pytest.raises(DocumentUpdateError) as exc:
        await family_relationship_service.add_relationship(
            parent=Parent(id=object_a.id), child_id=object_b.id
        )
    expected = 'Object "object_a" is already an ancestor of object "object_b".'
    assert expected in str(exc.value)


@pytest.mark.asyncio
async def test_add_relationship__case_1(
    family_document_service, family_relationship_service, object_a, object_b, object_c
):
    """Test add_relationship method, case 1:
    - ObjectB.add_parent(ObjectA)
    - ObjectC.add_parent(ObjectB)
    """
    # step 1: ObjectB.add_parent(ObjectA)
    updated_object_b = await family_relationship_service.add_relationship(
        parent=Parent(id=object_a.id), child_id=object_b.id
    )
    assert [parent.id for parent in updated_object_b.parents] == [object_a.id]
    assert updated_object_b.ancestor_ids == [object_a.id]

    # step 2: ObjectC.add_parent(ObjectB)
    updated_object_c = await family_relationship_service.add_relationship(
        parent=Parent(id=object_b.id), child_id=object_c.id
    )
    assert [parent.id for parent in updated_object_c.parents] == [object_b.id]
    assert updated_object_c.ancestor_ids == [object_a.id, object_b.id]

    updated_object_b = await family_document_service.get_document(document_id=object_b.id)
    assert [parent.id for parent in updated_object_b.parents] == [object_a.id]
    assert updated_object_b.ancestor_ids == [object_a.id]


@pytest.mark.asyncio
async def test_add_relationship__case_2(
    family_document_service, family_relationship_service, object_a, object_b, object_c
):
    """Test add_relationship method (same as case 1, reversing update order), case 2:
    - ObjectC.add_parent(ObjectB)
    - ObjectB.add_parent(ObjectA)
    """
    # step 1: ObjectC.add_parent(ObjectB)
    updated_object_c = await family_relationship_service.add_relationship(
        parent=Parent(id=object_b.id), child_id=object_c.id
    )
    assert [parent.id for parent in updated_object_c.parents] == [object_b.id]
    assert updated_object_c.ancestor_ids == [object_b.id]

    # step 2: ObjectB.add_parent(ObjectA)
    updated_object_b = await family_relationship_service.add_relationship(
        parent=Parent(id=object_a.id), child_id=object_b.id
    )
    assert [parent.id for parent in updated_object_b.parents] == [object_a.id]
    assert updated_object_b.ancestor_ids == [object_a.id]

    updated_object_c = await family_document_service.get_document(document_id=object_c.id)
    assert [parent.id for parent in updated_object_c.parents] == [object_b.id]
    assert updated_object_c.ancestor_ids == [object_a.id, object_b.id]


@pytest.mark.asyncio
async def test_add_relationship__case_3(
    family_document_service, family_relationship_service, object_a, object_b, object_c, object_d
):
    """Test add_relationship method, case 3:
    - ObjectB.add_parent(ObjectA)
    - ObjectD.add_parent(ObjectC)
    - ObjectC.add_parent(ObjectB)
    """
    await family_relationship_service.add_relationship(
        parent=Parent(id=object_a.id), child_id=object_b.id
    )
    await family_relationship_service.add_relationship(
        parent=Parent(id=object_c.id), child_id=object_d.id
    )
    await family_relationship_service.add_relationship(
        parent=Parent(id=object_b.id), child_id=object_c.id
    )

    expected_map = {
        object_a.id: {"parent_ids": [], "ancestor_ids": []},
        object_b.id: {"parent_ids": [object_a.id], "ancestor_ids": [object_a.id]},
        object_c.id: {"parent_ids": [object_b.id], "ancestor_ids": [object_a.id, object_b.id]},
        object_d.id: {
            "parent_ids": [object_c.id],
            "ancestor_ids": [object_a.id, object_b.id, object_c.id],
        },
    }
    for obj_id, expected in expected_map.items():
        obj = await family_document_service.get_document(document_id=obj_id)
        parent_ids = [parent.id for parent in obj.parents]
        assert parent_ids == expected["parent_ids"]
        assert obj.ancestor_ids == expected["ancestor_ids"]


@pytest.mark.asyncio
async def test_add_relationship__case_4(
    family_document_service, family_relationship_service, object_a, object_b, object_c, object_d
):
    """Test add_relationship method, case 4:
    - ObjectC.add_parent(ObjectA)
    - ObjectC.add_parent(ObjectB)
    - ObjectD.add_parent(ObjectC)
    """
    await family_relationship_service.add_relationship(
        parent=Parent(id=object_a.id), child_id=object_c.id
    )
    await family_relationship_service.add_relationship(
        parent=Parent(id=object_b.id), child_id=object_c.id
    )
    await family_relationship_service.add_relationship(
        parent=Parent(id=object_c.id), child_id=object_d.id
    )

    expected_map = {
        object_a.id: {"parent_ids": [], "ancestor_ids": []},
        object_b.id: {"parent_ids": [], "ancestor_ids": []},
        object_c.id: {
            "parent_ids": [object_a.id, object_b.id],
            "ancestor_ids": [object_a.id, object_b.id],
        },
        object_d.id: {
            "parent_ids": [object_c.id],
            "ancestor_ids": [object_a.id, object_b.id, object_c.id],
        },
    }
    for obj_id, expected in expected_map.items():
        obj = await family_document_service.get_document(document_id=obj_id)
        parent_ids = [parent.id for parent in obj.parents]
        assert parent_ids == expected["parent_ids"]
        assert obj.ancestor_ids == expected["ancestor_ids"]


@pytest.mark.asyncio
async def test_remove_relationship__not_a_parent(family_relationship_service, object_a, object_b):
    """Test remove_relationship when both objects are not in parent & child relationship"""
    with pytest.raises(DocumentUpdateError) as exc:
        await family_relationship_service.remove_relationship(
            parent_id=object_a.id, child_id=object_b.id
        )
    expected = 'Object "object_a" is not the parent of object "object_b".'
    assert expected in str(exc.value)


@pytest.mark.asyncio
async def test_remove_relationship__case_1(
    family_document_service, family_relationship_service, object_a, object_b, object_c, object_d
):
    """Test remove_relationship method, case 1:
    scenario:
    - A is the parent of B
    - B is the parent of C
    - B is the parent of D
    check:
    - ObjectA.remove_relationship(ObjectB)
    """
    # setup
    await family_relationship_service.add_relationship(
        parent=Parent(id=object_a.id), child_id=object_b.id
    )
    await family_relationship_service.add_relationship(
        parent=Parent(id=object_b.id), child_id=object_c.id
    )
    await family_relationship_service.add_relationship(
        parent=Parent(id=object_b.id), child_id=object_d.id
    )

    # check remove parent A from object B
    await family_relationship_service.remove_relationship(
        parent_id=object_a.id, child_id=object_b.id
    )

    expected_map = {
        object_a.id: {"parent_ids": [], "ancestor_ids": []},
        object_b.id: {"parent_ids": [], "ancestor_ids": []},
        object_c.id: {"parent_ids": [object_b.id], "ancestor_ids": [object_b.id]},
        object_d.id: {"parent_ids": [object_b.id], "ancestor_ids": [object_b.id]},
    }
    for obj_id, expected in expected_map.items():
        obj = await family_document_service.get_document(document_id=obj_id)
        parent_ids = [parent.id for parent in obj.parents]
        assert parent_ids == expected["parent_ids"]
        assert obj.ancestor_ids == expected["ancestor_ids"]


@pytest.mark.asyncio
async def test_remove_relationship__case_2(
    family_document_service, family_relationship_service, object_a, object_b, object_c, object_d
):
    """Test remove_relationship method, case 2:
    scenario:
    - A is the parent of B
    - B is the parent of C
    - C is the parent of D
    check:
    - ObjectC.remove_relationship(ObjectB)
    """
    # setup
    await family_relationship_service.add_relationship(
        parent=Parent(id=object_a.id), child_id=object_b.id
    )
    await family_relationship_service.add_relationship(
        parent=Parent(id=object_b.id), child_id=object_c.id
    )
    await family_relationship_service.add_relationship(
        parent=Parent(id=object_c.id), child_id=object_d.id
    )

    # check remove parent B from object C
    await family_relationship_service.remove_relationship(
        parent_id=object_b.id, child_id=object_c.id
    )

    expected_map = {
        object_a.id: {"parent_ids": [], "ancestor_ids": []},
        object_b.id: {"parent_ids": [object_a.id], "ancestor_ids": [object_a.id]},
        object_c.id: {"parent_ids": [], "ancestor_ids": []},
        object_d.id: {"parent_ids": [object_c.id], "ancestor_ids": [object_c.id]},
    }
    for obj_id, expected in expected_map.items():
        obj = await family_document_service.get_document(document_id=obj_id)
        parent_ids = [parent.id for parent in obj.parents]
        assert parent_ids == expected["parent_ids"]
        assert obj.ancestor_ids == expected["ancestor_ids"]


@pytest.mark.asyncio
async def test_remove_relationship__case_3(
    family_document_service, family_relationship_service, object_a, object_b, object_c, object_d
):
    """Test remove_relationship method, case 3:
    scenario:
    - A is the parent of C
    - B is the parent of C
    - C is the parent of D
    check:
    - ObjectC.remove_relationship(ObjectB)
    """
    # setup
    await family_relationship_service.add_relationship(
        parent=Parent(id=object_a.id), child_id=object_c.id
    )
    await family_relationship_service.add_relationship(
        parent=Parent(id=object_b.id), child_id=object_c.id
    )
    await family_relationship_service.add_relationship(
        parent=Parent(id=object_c.id), child_id=object_d.id
    )

    # check remove parent B from object C
    await family_relationship_service.remove_relationship(
        parent_id=object_b.id, child_id=object_c.id
    )

    expected_map = {
        object_a.id: {"parent_ids": [], "ancestor_ids": []},
        object_b.id: {"parent_ids": [], "ancestor_ids": []},
        object_c.id: {"parent_ids": [object_a.id], "ancestor_ids": [object_a.id]},
        object_d.id: {"parent_ids": [object_c.id], "ancestor_ids": [object_a.id, object_c.id]},
    }
    for obj_id, expected in expected_map.items():
        obj = await family_document_service.get_document(document_id=obj_id)
        parent_ids = [parent.id for parent in obj.parents]
        assert parent_ids == expected["parent_ids"]
        assert obj.ancestor_ids == expected["ancestor_ids"]
