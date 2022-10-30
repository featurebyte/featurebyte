#!/bin/sh
mkdir -p /data/db1 /data/db2
/usr/bin/mongod --port=27021 --dbpath=/data/db1 --bind_ip_all --replSet rs0 -v --logpath /data/db1.log --logRotate reopen --logappend &
/usr/bin/mongod --port=27022 --dbpath=/data/db2 --bind_ip_all --replSet rs0 -v --logpath /data/db2.log --logRotate reopen --logappend &
sleep 10
/scripts/rs-init.sh
uvicorn featurebyte.app:app --host=0.0.0.0 --port=$API_PORT --workers=$WORKERS --timeout-keep-alive=300
