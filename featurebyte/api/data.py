"""
DataColumn class
"""
from __future__ import annotations

from typing import Any, ClassVar, Optional, Type, TypeVar

from http import HTTPStatus

from bson.objectid import ObjectId
from typeguard import typechecked

from featurebyte.api.api_object import SavableApiObject
from featurebyte.api.database_table import DatabaseTable
from featurebyte.api.entity import Entity
from featurebyte.config import Configurations
from featurebyte.core.mixin import GetAttrMixin, ParentMixin
from featurebyte.exception import DuplicatedRecordException, RecordRetrievalException
from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.models.feature_store import ColumnInfo

DataApiObjectT = TypeVar("DataApiObjectT", bound="DataApiObject")


class DataColumn(FeatureByteBaseModel, ParentMixin):
    """
    DataColumn class that is used to set metadata such as Entity column. It holds a reference to its
    parent, which is a data object (e.g. EventData)
    """

    info: ColumnInfo

    @typechecked
    def as_entity(self, entity_name: Optional[str]) -> None:
        """
        Set the column as the specified entity

        Parameters
        ----------
        entity_name: Optional[str]
            Associate column name to the entity, remove association if entity name is None
        """
        if entity_name is None:
            entity_id = None
        else:
            entity = Entity.get(entity_name)
            entity_id = entity.id

        columns_info = []
        for col in self.parent.columns_info:
            if col.name == self.info.name:
                self.info = ColumnInfo(**{**col.dict(), "entity_id": entity_id})
                columns_info.append(self.info)
            else:
                columns_info.append(col)

        self.parent.update(update_payload={"columns_info": columns_info}, allow_update_local=True)


class DataApiObject(DatabaseTable, SavableApiObject, GetAttrMixin):
    """
    Base class for all Data objects
    """

    _create_schema_class: ClassVar[Optional[Type[FeatureByteBaseModel]]] = None

    @classmethod
    @typechecked
    def create(
        cls: Type[DataApiObjectT],
        tabular_source: DatabaseTable,
        name: str,
        record_creation_date_column: Optional[str] = None,
        _id: Optional[ObjectId] = None,
        **kwargs: Any,
    ) -> DataApiObjectT:
        """
        Create EventData object from tabular source

        Parameters
        ----------
        tabular_source: DatabaseTable
            DatabaseTable object constructed from FeatureStore
        name: str
            Event data name
        record_creation_date_column: str
            Record creation datetime column from the given tabular source
        _id: Optional[ObjectId]
            Identity value for constructed object
        **kwargs: Any
            Additional parameters specific to variants of DataApiObject

        Returns
        -------
        EventData

        Raises
        ------
        DuplicatedRecordException
            When record with the same key exists at the persistent layer
        RecordRetrievalException
            When unexpected retrieval failure
        """
        assert cls._create_schema_class is not None
        data = cls._create_schema_class(  # pylint: disable=not-callable
            _id=_id or ObjectId(),
            name=name,
            tabular_source=tabular_source.tabular_source,
            columns_info=tabular_source.columns_info,
            record_creation_date_column=record_creation_date_column,
            **kwargs,
        )
        client = Configurations().get_client()
        response = client.get(url=cls._route, params={"name": name})
        if response.status_code == HTTPStatus.OK:
            response_dict = response.json()
            if not response_dict["data"]:
                return cls(
                    **data.json_dict(),
                    feature_store=tabular_source.feature_store,
                )
            existing_record = response_dict["data"][0]
            raise DuplicatedRecordException(
                response,
                f'{cls.__name__} ({existing_record["type"]}.name: "{name}") exists in saved record.',
            )
        raise RecordRetrievalException(response)

    def _get_init_params_from_object(self) -> dict[str, Any]:
        return {"feature_store": self.feature_store}

    @typechecked
    def __getitem__(self, item: str) -> DataColumn:
        """
        Retrieve column from the table

        Parameters
        ----------
        item: str
            Column name

        Returns
        -------
        DataColumn

        Raises
        ------
        KeyError
            when accessing non-exist column
        """
        info = None
        for col in self.columns_info:
            if col.name == item:
                info = col
        if info is None:
            raise KeyError(f'Column "{item}" does not exist!')
        output = DataColumn(info=info)
        output.set_parent(self)
        return output

    @typechecked
    def update_record_creation_date_column(self, record_creation_date_column: str) -> None:
        """
        Update record creation date column

        Parameters
        ----------
        record_creation_date_column: str
            Record creation date column used to perform feature job setting analysis
        """
        self.update(
            update_payload={"record_creation_date_column": record_creation_date_column},
            allow_update_local=True,
        )
