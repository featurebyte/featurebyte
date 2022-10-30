#!/bin/sh
mkdir /data/db1 /data/db2
/usr/bin/mongod --port=27021 --dbpath=/data/db1 --bind_ip_all --replSet rs0 &
/usr/bin/mongod --port=27022 --dbpath=/data/db2 --bind_ip_all --replSet rs0
