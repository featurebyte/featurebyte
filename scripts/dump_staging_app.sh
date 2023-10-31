#!/bin/bash

# exit on any command failure
set -e

MONGO_DUMP_FILENAME=mongodb-staging.tar.gz

# download staging data & dump it to the mongo
gsutil cp gs://featurebyte_staging_backup/$MONGO_DUMP_FILENAME
docker cp $MONGO_DUMP_FILENAME mongo-rs:/
docker exec mongo-rs mongorestore --gzip --drop \
    --nsInclude="featurebyte.*catalog" \
    --nsInclude="featurebyte.*feature_store" \
    --nsInclude="featurebyte.*entity" \
    --nsInclude="featurebyte.*relationship_info" \
    --nsInclude="featurebyte.*table" \
    --nsInclude="featurebyte.*feature" \
    --nsInclude="featurebyte.*feature_namespace" \
    --nsExclude="featurebyte.feature_ideation_suggested_feature" \
    --archive=$MONGO_DUMP_FILENAME \
    --uri 'mongodb://localhost:27021,localhost:27022/?replicaSet=rs0'

rm -f $MONGO_DUMP_FILENAME
