"""
EventData API route controller
"""
from __future__ import annotations

from typing import Any

from http import HTTPStatus

from bson.objectid import ObjectId
from fastapi import HTTPException

from featurebyte.models.entity import EntityModel
from featurebyte.models.event_data import EventDataModel, EventDataStatus
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.persistent import Persistent
from featurebyte.routes.common.base import BaseController
from featurebyte.routes.common.operation import DictProject, DictTransform
from featurebyte.schema.event_data import EventDataCreate, EventDataList, EventDataUpdate


class EventDataController(BaseController[EventDataModel, EventDataList]):
    """
    EventData controller
    """

    collection_name = EventDataModel.collection_name()
    document_class = EventDataModel
    paginated_document_class = EventDataList
    info_transform = DictTransform(
        rule={
            **BaseController.base_info_transform_rule,
            "__root__": DictProject(
                rule=[
                    "event_timestamp_column",
                    "record_creation_date_column",
                ]
            ),
            "columns": DictProject(
                rule=("columns_info", ["name", "var_type", "entity"]), verbose_only=True
            ),
        }
    )
    foreign_key_map = {"entity_id": EntityModel.collection_name()}

    @classmethod
    async def create_event_data(
        cls,
        user: Any,
        persistent: Persistent,
        data: EventDataCreate,
    ) -> EventDataModel:
        """
        Create Event Data at persistent

        Parameters
        ----------
        user: Any
            User class to provide user identifier
        persistent: Persistent
            Object that entity will be saved to
        data: EventDataCreate
            EventData creation payload

        Returns
        -------
        EventDataModel
            Newly created event data object
        """
        # check the existence of the feature store at persistent
        _ = await cls.get_document(
            user=user,
            persistent=persistent,
            collection_name=FeatureStoreModel.collection_name(),
            document_id=data.tabular_source.feature_store_id,
        )

        document = EventDataModel(
            user_id=user.id,
            status=EventDataStatus.DRAFT,
            **data.json_dict(),
        )

        # check any conflict with existing documents
        await cls.check_document_unique_constraints(
            persistent=persistent,
            user_id=user.id,
            document=document,
        )

        insert_id = await persistent.insert_one(
            collection_name=cls.collection_name,
            document=document.dict(by_alias=True),
            user_id=user.id,
        )
        assert insert_id == document.id == data.id

        return await cls.get(user=user, persistent=persistent, document_id=insert_id)

    @classmethod
    async def update_event_data(
        cls,
        user: Any,
        persistent: Persistent,
        event_data_id: ObjectId,
        data: EventDataUpdate,
    ) -> EventDataModel:
        """
        Update EventData (for example, to update scheduled task) at persistent (GitDB or MongoDB)

        Parameters
        ----------
        user: Any
            User class to provide user identifier
        persistent: Persistent
            Object that entity will be saved to
        event_data_id: ObjectId
            EventData ID
        data: EventDataUpdate
            Event data update payload

        Returns
        -------
        EventDataModel
            EventData object with updated attribute(s)

        Raises
        ------
        HTTPException
            Invalid event data status transition
        """
        query_filter = {"_id": ObjectId(event_data_id), "user_id": user.id}
        event_data = await cls.get_document(
            user=user,
            persistent=persistent,
            collection_name=cls.collection_name,
            document_id=event_data_id,
        )

        # prepare update payload
        update_payload = data.dict()

        # check eligibility of status transition
        eligible_transitions = {
            EventDataStatus.DRAFT: {EventDataStatus.PUBLISHED},
            EventDataStatus.PUBLISHED: {EventDataStatus.DEPRECATED},
            EventDataStatus.DEPRECATED: {},
        }
        current_status = event_data["status"]
        if (
            current_status != data.status
            and data.status not in eligible_transitions[current_status]
        ):
            raise HTTPException(
                status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
                detail=f"Invalid status transition from {current_status} to {data.status}.",
            )

        await persistent.update_one(
            collection_name=cls.collection_name,
            query_filter=query_filter,
            update={"$set": update_payload},
            user_id=user.id,
        )

        return await cls.get(user=user, persistent=persistent, document_id=event_data_id)
