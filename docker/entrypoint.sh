#!/bin/sh

# start mongo service
mkdir -p /data/db1 /data/db2
/usr/bin/mongod --port=27021 --dbpath=/data/db1 --bind_ip_all --replSet rs0 -v --logpath /data/db1.log --logRotate reopen --logappend &
/usr/bin/mongod --port=27022 --dbpath=/data/db2 --bind_ip_all --replSet rs0 -v --logpath /data/db2.log --logRotate reopen --logappend &

# wait for mongo service to be up
mongosh --port=27021 --eval exit || sleep 1

# setup replica set
mongosh --port=27021 <<EOF
var config = {
    "_id": "rs0",
    "version": 1,
    "members": [
        {
            "_id": 1,
            "host": "localhost:27021",
            "priority": 1
        },
        {
            "_id": 2,
            "host": "localhost:27022",
            "priority": 2
        },
    ]
};
rs.initiate(config, { force: true });
rs.status();
EOF

# perform database migration
PYTHONPATH=$PWD python /scripts/migration.py

# start featurebyte service
uvicorn featurebyte.app:app --host=0.0.0.0 --port=$API_PORT --workers=$WORKERS --timeout-keep-alive=300
