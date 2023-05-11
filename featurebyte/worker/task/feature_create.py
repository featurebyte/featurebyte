"""
Feature create task
"""
from __future__ import annotations

from typing import Any, Dict, cast

import asyncio
import concurrent.futures
import os

from bson import ObjectId

from featurebyte.config import Configurations
from featurebyte.logging import get_logger
from featurebyte.models.base import activate_catalog
from featurebyte.models.feature import FeatureModel, FeatureReadiness
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
        document = FeatureModel(**{**document.dict(), "graph": graph, "node_name": node_name})

        # prepare feature definition
        table_id_to_info: Dict[ObjectId, Dict[str, Any]] = {}
        table_service: TableService = self.app_container.table_service
        for table_id in document.table_ids:
            table = await table_service.get_document(document_id=table_id)
            table_id_to_info[table_id] = table.dict()

        sdk_code_gen_state = SDKCodeExtractor(graph=payload.graph).extract(
            node=payload.graph.get_node_by_name(payload.node_name),
            to_use_saved_data=True,
            table_id_to_info=table_id_to_info,
        )
        definition = sdk_code_gen_state.code_generator.generate(to_format=True)
        code = f'{definition}output.save(_id="{payload.output_document_id}")'
        logger.debug(f"Prepare to execute feature definition: \n{code}")

        os.environ["SDK_EXECUTION_MODE"] = "SERVER"
        logger.debug(f"Configuration: {Configurations().profile}")

        # activate the correct catalog before executing the code
        activate_catalog(catalog_id=payload.catalog_id)
        with concurrent.futures.ThreadPoolExecutor() as pool:
            await asyncio.get_event_loop().run_in_executor(pool, exec, code)

        feature = await feature_service.get_document(document_id=payload.output_document_id)
        generated_hash = feature.graph.node_name_to_ref[feature.node_name]
        expected_hash = graph.node_name_to_ref[node_name]
        if definition != feature.definition or expected_hash != generated_hash:
            # log the difference between the expected feature and the saved feature
            logger.debug(f">>> Generated node_name_to_ref: \n{graph.node_name_to_ref}")
            logger.debug(f">>> Expected node_name_to_ref: \n{feature.graph.node_name_to_ref}")
            logger.debug(f">>> Generated graph: \n{graph.dict()}")
            logger.debug(f">>> Expected graph: \n{feature.graph.dict()}")
            logger.debug(f">>> Generated feature definition: \n{definition}")
            logger.debug(f">>> Saved feature definition: \n{feature.definition}")

            import pickle

            from featurebyte.config import get_home_path

            home_path = get_home_path()
            feature_path = home_path.joinpath(f"{feature.name}_dict.pkl")
            graph_path = home_path.joinpath(f"{feature.name}_graph.pkl")
            definition_path = home_path.joinpath(f"{feature.name}_definition.txt")
            expected_definition_path = home_path.joinpath(f"{feature.name}_expected_definition.txt")
            payload_path = home_path.joinpath(f"{feature.name}_payload.pkl")
            with open(feature_path, "wb") as f:
                pickle.dump(feature.dict(by_alias=True), f)
            with open(graph_path, "wb") as f:
                pickle.dump([graph.dict(), node_name], f)
            with open(definition_path, "w") as f:
                f.write(feature.definition)
            with open(expected_definition_path, "w") as f:
                f.write(definition)
            with open(payload_path, "wb") as f:
                pickle.dump(payload.dict(by_alias=True), f)

            # delete the feature if the definition is not consistent
            # feat = Feature.get_by_id(payload.output_document_id)
            # feat.delete()
            # raise DocumentInconsistencyError("Inconsistent feature definition detected!")

        logger.debug("Complete feature create task")
