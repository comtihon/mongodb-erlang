#!/usr/bin/env bash

set -ex

rm   -rf single_node single_node.log
mkdir -p single_node

echo "Starting single node"
mongod --port 27017 --bind_ip localhost --dbpath single_node --oplogSize 128 > single_node.log &

echo "Waiting on single node MongoDB to start on 27017"
timeout 5m sh -c 'until nc -z localhost 27017; do sleep 1; done'

mongo admin --eval 'db.createUser(
         {user: "user",
          pwd: "test",
          roles: [{role: "userAdminAnyDatabase", db: "admin"},
                  "readWrite"]}
        )'

mongod --shutdown --dbpath single_node

mongod --port 27017 --bind_ip localhost --dbpath single_node --oplogSize 128 >> single_node.log &

echo "Waiting on single node MongoDB to restart with auth on 27017"
timeout 5m sh -c 'until nc -z localhost 27017; do sleep 1; done'

# Verify that we can auth to the restarted replica set
mongo --username user \
      --password test \
      --eval 'db.serverStatus()'
