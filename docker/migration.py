"""
Perform database migration
"""
import asyncio

from featurebyte.app import User
from featurebyte.logging import get_logger
from featurebyte.migration.run import run_migration
from featurebyte.utils.credential import MongoBackedCredentialProvider
from featurebyte.utils.persistent import get_persistent
from featurebyte.worker import get_celery

logger = get_logger(__name__)


if __name__ == "__main__":
    logger.info("Running database migration")
    credential_provider = MongoBackedCredentialProvider(persistent=get_persistent())
    asyncio.run(
        run_migration(user=User(), persistent=get_persistent(), get_credential=credential_provider.get_credential, celery=get_celery())
    )

    logger.info("Database migration completed")
