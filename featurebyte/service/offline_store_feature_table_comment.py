"""
OfflineStoreFeatureTableCommentService
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Coroutine, Dict, List, Optional, Sequence, Tuple, Union

from featurebyte.logging import get_logger
from featurebyte.models.entity import EntityModel
from featurebyte.models.feature import FeatureModel
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.models.offline_store_feature_table import OfflineStoreFeatureTableModel
from featurebyte.query_graph.model.feature_job_setting import FeatureJobSetting
from featurebyte.service.entity import EntityService
from featurebyte.service.feature_namespace import FeatureNamespaceService
from featurebyte.service.session_manager import SessionManagerService

logger = get_logger(__name__)


@dataclass
class TableComment:
    """
    Representation of a comment for a specific offline feature table
    """

    table_name: str
    comment: str


@dataclass
class ColumnComment:
    """
    Representation of a comment for a specific offline feature table column
    """

    table_name: str
    column_name: str
    comment: str


class OfflineStoreFeatureTableCommentService:
    """
    OfflineStoreFeatureTableCommentService is responsible for generating comments for offline store
    feature tables and columns, and applying them in the data warehouse.
    """

    def __init__(
        self,
        entity_service: EntityService,
        session_manager_service: SessionManagerService,
        feature_namespace_service: FeatureNamespaceService,
    ):
        self.entity_service = entity_service
        self.session_manager_service = session_manager_service
        self.feature_namespace_service = feature_namespace_service

    async def apply_comments(
        self,
        feature_store: FeatureStoreModel,
        comments: Sequence[Union[TableComment, ColumnComment]],
        update_progress: Optional[Callable[[int, str | None], Coroutine[Any, Any, None]]] = None,
    ) -> None:
        """
        Add the provided table or column comments in the data warehouse

        Parameters
        ----------
        feature_store: FeatureStoreModel
            Feature store model
        comments:  Sequence[Union[TableComment, ColumnComment]]
            List of comments to be added
        update_progress: Optional[Callable[[int, str | None], Coroutine[Any, Any, None]]
            Progress callback
        """
        session = await self.session_manager_service.get_feature_store_session(feature_store)
        for idx, entry in enumerate(comments):
            try:
                if isinstance(entry, TableComment):
                    await session.comment_table(entry.table_name, entry.comment)
                else:
                    await session.comment_column(entry.table_name, entry.column_name, entry.comment)
            except Exception as exc:
                if isinstance(entry, TableComment):
                    extra = {"table_name": entry.table_name}
                else:
                    extra = {"table_name": entry.table_name, "column_name": entry.column_name}
                logger.error("Failed to add comment: %s", exc, extra=extra)

            if update_progress:
                percent = int((idx + 1) / len(comments) * 100)
                if isinstance(entry, TableComment):
                    message = f"Added comment for table {entry.table_name}"
                else:
                    message = (
                        f"Added comment for column {entry.column_name} in table {entry.table_name}"
                    )
                await update_progress(percent, message)

    async def generate_table_comment(
        self, feature_table_model: OfflineStoreFeatureTableModel
    ) -> TableComment:
        """
        Generate comment for an offline feature table

        Parameters
        ----------
        feature_table_model: OfflineStoreFeatureTableModel
            Offline store feature table model

        Returns
        -------
        TableComment
        """
        primary_entities = await self.entity_service.get_entities(
            set(feature_table_model.primary_entity_ids)
        )

        def _format_entity(entity_model: EntityModel) -> str:
            return f"{entity_model.name} (serving name: {entity_model.serving_names[0]})"

        primary_entities_info = ", ".join([_format_entity(entity) for entity in primary_entities])
        if feature_table_model.primary_entity_ids:
            sentences = [
                f"This feature table consists of features for primary entity {primary_entities_info}"
            ]
        else:
            sentences = ["This feature table consists of features without a primary entity"]
        if feature_table_model.feature_job_setting:
            job_setting = feature_table_model.feature_job_setting
            if not isinstance(job_setting, FeatureJobSetting):
                sentences.append(
                    f"It is updated according to a cron schedule:"
                    f" '{job_setting.get_cron_expression()}' aligned to the "
                    f" '{job_setting.timezone}' time zone"
                )
            else:
                sentences.append(
                    f"It is updated every {job_setting.period_seconds} second(s), with a blind spot"
                    f" of {job_setting.blind_spot_seconds} second(s) and a time modulo frequency of"
                    f" {job_setting.offset_seconds} second(s)"
                )
        comment = ". ".join(sentences) + "."
        return TableComment(table_name=feature_table_model.name, comment=comment)

    async def generate_column_comments(
        self, feature_models: List[FeatureModel]
    ) -> List[ColumnComment]:
        """
        Generate comments for columns in offline feature tables corresponding to the features

        Parameters
        ----------
        feature_models: List[FeatureModel]
            Feature models

        Returns
        -------
        List[ColumnComment]
        """
        # Mapping to from table name and column name to comments
        comments: Dict[Tuple[str, str], str] = {}

        for feature in feature_models:
            feature_description = (
                await self.feature_namespace_service.get_document(feature.feature_namespace_id)
            ).description

            offline_ingest_graphs = (
                feature.offline_store_info.extract_offline_store_ingest_query_graphs()
            )
            for offline_ingest_graph in offline_ingest_graphs:
                table_name = offline_ingest_graph.offline_store_table_name
                if feature.offline_store_info.is_decomposed:
                    comment = (
                        f"This intermediate feature is used to compute the feature"
                        f" {feature.name} (version: {feature.version.name})"
                    )
                    if feature_description is not None:
                        comment += f". Description of {feature.name}: {feature_description}"
                    comments[(table_name, offline_ingest_graph.output_column_name)] = comment
                elif feature_description is not None:
                    comments[(table_name, offline_ingest_graph.output_column_name)] = (
                        feature_description
                    )

        out = [
            ColumnComment(
                table_name=table_name,
                column_name=column_name,
                comment=comment,
            )
            for ((table_name, column_name), comment) in comments.items()
        ]
        return out
