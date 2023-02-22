#!/bin/bash

# Exit on any command failure
set -e

# start mongo service
mkdir -p /data/db1 /data/db2

echo "Starting mongo servers"
/usr/bin/mongod --port=27021 --dbpath=/data/db1 --bind_ip_all --replSet rs0 -v --logpath /data/db1.log --logRotate reopen --logappend &
/usr/bin/mongod --port=27022 --dbpath=/data/db2 --bind_ip_all --replSet rs0 -v --logpath /data/db2.log --logRotate reopen --logappend &

MONGO_MAJOR_VERSION="$(/usr/bin/mongod --version | grep -oP '"version":\s*"\K[^.]+')"
echo "MONGO_VERSION: ${MONGO_MAJOR_VERSION}"
# Switching based on mongodb version
# Mongodb ^5.0 uses mongosh
# Mongodb <5.0 uses mongo
if [[ 5 -gt ${MONGO_MAJOR_VERSION} ]]; then
  # Sleep and wait for server to start
  while ! mongo --quiet --port=27021 --eval "quit()" 2>/dev/null; do echo "Waiting for mongo1 to start"; sleep 1; done; echo "mongo1 started"
  while ! mongo --quiet --port=27022 --eval "quit()" 2>/dev/null; do echo "Waiting for mongo2 to start"; sleep 1; done; echo "mongo2 started"

  # If not bootstrapped, bootstrap
  if [[ 1 -ne "$(mongo --quiet --port=27021 --eval "rs.status().ok")" ]]; then
    mongo --quiet --port=27021 <<EOF
      var config = {
        "_id": "rs0",
        "version": 1,
        "members": [
          {
            "_id": 1,
            "host": "localhost:27021",
            "priority": 1
          }, {
            "_id": 2,
            "host": "localhost:27022",
            "priority": 2
          },
        ]
      };
      rs.initiate(config, { force: true });
EOF
  fi

  # Wait for replicaset to form
  while [[ 1 -ne "$(mongo --quiet --port=27021 --eval "rs.status().ok")" ]]; do
    echo "Waiting for replicaset to establish";
    sleep 1;
  done
  echo "mongo-testrs is running"
else
  # Sleep and wait for server to start
  while ! mongosh --quiet --port=27021 --eval "exit" 2>/dev/null; do echo "Waiting for mongo1 to start"; sleep 1; done; echo "mongo1 started"
  while ! mongosh --quiet --port=27022 --eval "exit" 2>/dev/null; do echo "Waiting for mongo2 to start"; sleep 1; done; echo "mongo2 started"

# If not bootstrapped, bootstrap
  if ! mongosh --quiet --port=27021 --eval "rs.status()" 1>/dev/null 2>&1; then
    mongosh --quiet --port=27021 <<EOF
      var config = {
        "_id": "rs0",
        "version": 1,
        "members": [
          {
            "_id": 1,
            "host": "localhost:27021",
            "priority": 1
          }, {
            "_id": 2,
            "host": "localhost:27022",
            "priority": 2
          },
        ]
      };
      rs.initiate(config, { force: true });
EOF
  fi

  # Wait for replicaset to form
  while [[ 1 -ne "$(mongosh --quiet --port=27021 --eval "rs.status().ok")" ]]; do
    echo "Waiting for replicaset to establish";
    sleep 1;
  done
  echo "mongo-testrs is running"
fi

# Sleep
sleep inf
