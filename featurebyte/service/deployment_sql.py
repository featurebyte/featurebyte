"""
DeploymentSqlService
"""

from bson import ObjectId

from featurebyte.models.deployment_sql import DeploymentSqlModel
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema
from featurebyte.schema.deployment_sql import DeploymentSqlCreate
from featurebyte.schema.worker.task.deployment_sql import DeploymentSqlCreateTaskPayload
from featurebyte.service.base_document import BaseDocumentService


class DeploymentSqlService(
    BaseDocumentService[DeploymentSqlModel, DeploymentSqlModel, BaseDocumentServiceUpdateSchema]
):
    """
    DeploymentSqlService class
    """

    document_class = DeploymentSqlModel

    async def get_deployment_sql_create_task_payload(
        self, data: DeploymentSqlCreate
    ) -> DeploymentSqlCreateTaskPayload:
        """
        Validate and convert a DeploymentSqlCreate schema to a DeploymentSqlCreateTaskPayload schema
        which will be used to initiate the DeploymentSql creation task.

        Parameters
        ----------
        data: DeploymentSqlCreate
            DeploymentSql creation payload

        Returns
        -------
        DeploymentSqlCreateTaskPayload
        """
        # Create the task payload
        payload = DeploymentSqlCreateTaskPayload(
            output_document_id=data.id or ObjectId(),
            deployment_id=data.deployment_id,
            user_id=self.user.id,
            catalog_id=self.catalog_id,
        )
        return payload
