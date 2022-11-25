#!/bin/bash

# download staging data & dump it to the mongo
gsutil cp gs://featurebyte_staging_bucket/mongodb.tar.gz .
docker cp mongodb.tar.gz mongo-testrs:/
docker exec mongo-testrs tar xvf /mongodb.tar.gz
docker exec mongo-testrs mongorestore --drop --uri 'mongodb://localhost:27021,localhost:27022/?replicaSet=rs0' $@
rm -f mongodb.tar.gz
