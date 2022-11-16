# This file is only used for local
# This file is needed so that __name__ is set properly in featurebyte.migration.run
import asyncio

from featurebyte.migration.run import run_mongo_migration
from featurebyte.persistent.mongo import MongoDB

if __name__ == "__main__":
    for db in ["app", "org1", "demo", "ccdemo"]:
        persistent_obj = MongoDB(
            "mongodb://localhost:27021,localhost:27022/replicaSet=rs0", database=db
        )
        asyncio.run(run_mongo_migration(persistent_obj))
