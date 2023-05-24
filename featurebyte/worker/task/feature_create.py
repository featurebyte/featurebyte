"""
Feature create task
"""
from __future__ import annotations

from typing import Any, Dict, cast

import asyncio
import concurrent.futures
import os
from pprint import pformat

from bson import ObjectId

from featurebyte.config import Configurations
from featurebyte.exception import DocumentInconsistencyError
from featurebyte.logging import get_logger
from featurebyte.models.base import activate_catalog
from featurebyte.models.feature import FeatureModel, FeatureReadiness
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.transform.sdk_code import SDKCodeExtractor
from featurebyte.schema.worker.task.feature_create import FeatureCreateTaskPayload
from featurebyte.service.feature import FeatureService
from featurebyte.service.table import TableService
from featurebyte.worker.task.base import BaseTask

logger = get_logger(__name__)


class FeatureCreateTask(BaseTask):
    """
    FeatureList Deploy Task
    """

    payload_class = FeatureCreateTaskPayload

    @staticmethod
    def _construct_expected_graph(graph: QueryGraphModel) -> QueryGraphModel:
        output = graph.dict()
        for node in output["nodes"]:
            if node["type"] == NodeType.GRAPH:
                # replace the view graph mode to manual
                if "view_mode" in node["parameters"]["metadata"]:
                    node["parameters"]["metadata"]["view_mode"] = "manual"
        return QueryGraphModel(**output)

    @staticmethod
    async def _execute_sdk_code(catalog_id: ObjectId, code: str):
        # activate the correct catalog before executing the code
        activate_catalog(catalog_id=catalog_id)

        # execute the code
        with concurrent.futures.ThreadPoolExecutor() as pool:
            await asyncio.get_event_loop().run_in_executor(pool, exec, code)

    async def execute(self) -> Any:
        """
        Execute Deployment Create & Update Task
        """

        payload = cast(FeatureCreateTaskPayload, self.payload)

        # pruning the graph & prepare the feature model
        feature_service: FeatureService = self.app_container.feature_service
        document = FeatureModel(
            **{
                **payload.json_dict(),
                "readiness": FeatureReadiness.DRAFT,
                "version": await feature_service.generate_feature_version(payload.name),
                "user_id": payload.user_id,
                "catalog_id": payload.catalog_id,
            }
        )
        graph, node_name = await self.app_container.feature_service.prepare_graph_to_store(
            feature=document
        )
        document = FeatureModel(
            **{
                **document.dict(),
                "graph": self._construct_expected_graph(graph),
                "node_name": node_name,
            }
        )

        # prepare feature definition
        table_id_to_info: Dict[ObjectId, Dict[str, Any]] = {}
        table_service: TableService = self.app_container.table_service
        for table_id in document.table_ids:
            table = await table_service.get_document(document_id=table_id)
            table_id_to_info[table_id] = table.dict()

        sdk_code_gen_state = SDKCodeExtractor(graph=document.graph).extract(
            node=document.graph.get_node_by_name(document.node_name),
            to_use_saved_data=True,
            table_id_to_info=table_id_to_info,
        )
        definition = sdk_code_gen_state.code_generator.generate(to_format=True)

        code = (
            f"{definition}\n"
            "# add statements to save feature\n"
            "from bson import ObjectId\n"
            f'output.save(_id=ObjectId("{payload.output_document_id}"))'
        )
        logger.debug(f"Prepare to execute feature definition: \n{code}")

        os.environ["SDK_EXECUTION_MODE"] = "SERVER"
        logger.debug(f"Configuration: {Configurations().profile}")

        # execute the code to save the feature
        await self._execute_sdk_code(catalog_id=payload.catalog_id, code=code)

        # retrieve the saved feature & check if it is the same as the expected feature
        feature = await feature_service.get_document(document_id=payload.output_document_id)
        generated_hash = feature.graph.node_name_to_ref[feature.node_name]
        expected_hash = document.graph.node_name_to_ref[document.node_name]
        if definition != feature.definition or expected_hash != generated_hash:
            # log the difference between the expected feature and the saved feature
            generated_ref = pformat(feature.graph.node_name_to_ref)
            expected_ref = pformat(document.graph.node_name_to_ref)
            logger.info(f">>> Generated node_name_to_ref: \n{generated_ref}")
            logger.info(f">>> Expected node_name_to_ref: \n{expected_ref}")
            logger.info(f">>> Generated graph: \n{pformat(feature.graph.dict())}")
            logger.info(f">>> Expected graph: \n{pformat(document.graph.dict())}")
            logger.info(f">>> Generated feature definition: \n{definition}")
            logger.info(f">>> Saved feature definition: \n{feature.definition}")

            # prepare the code to delete the feature
            code = (
                "from bson import ObjectId\n"
                "from featurebyte import Feature\n"
                f'feat = Feature.get_by_id(ObjectId("{payload.output_document_id}"))'
            )

            # execute the code to delete the feature
            await self._execute_sdk_code(catalog_id=payload.catalog_id, code=code)

            raise DocumentInconsistencyError("Inconsistent feature definition detected!")

        logger.debug("Complete feature create task")
