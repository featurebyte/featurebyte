#!/bin/bash

# exit on any command failure
set -e

MONGO_DUMP_FILENAME=mongodb-staging.tar.gz

# download staging data & dump it to the mongo
gsutil cp gs://featurebyte_staging_backup/$MONGO_DUMP_FILENAME
docker cp $MONGO_DUMP_FILENAME mongo-testrs:/
docker exec mongo-testrs mongorestore --gzip --drop --uri 'mongodb://localhost:27021,localhost:27022/?replicaSet=rs0' --archive $MONGO_DUMP_FILENAME
# rm -f $MONGO_DUMP_FILENAME
