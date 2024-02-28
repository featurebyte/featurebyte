"""
Perform database migration
"""
import asyncio

from featurebyte.app import User
from featurebyte.logging import get_logger
from featurebyte.migration.run import run_migration
from featurebyte.utils.credential import MongoBackedCredentialProvider
from featurebyte.utils.persistent import MongoDBImpl
from featurebyte.utils.storage import get_storage
from featurebyte.worker import get_celery, get_redis

logger = get_logger(__name__)


if __name__ == "__main__":
    logger.info("Running database migration")
    credential_provider = MongoBackedCredentialProvider(persistent=MongoDBImpl())
    # Note that the user ID here is an arbitrary one. For collections that have user specific data, special handling
    # will be required.
    asyncio.run(
        run_migration(
            user=User(),
            persistent=MongoDBImpl(),
            get_credential=credential_provider.get_credential,
            celery=get_celery(),
            storage=get_storage(),
            redis=get_redis(),
        )
    )

    logger.info("Database migration completed")
