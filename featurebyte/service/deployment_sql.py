"""
DeploymentSqlService
"""

from featurebyte.models.deployment_sql import DeploymentSqlModel
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema
from featurebyte.service.base_document import BaseDocumentService


class DeploymentSqlService(
    BaseDocumentService[DeploymentSqlModel, DeploymentSqlModel, BaseDocumentServiceUpdateSchema]
):
    """
    DeploymentSqlService class
    """
