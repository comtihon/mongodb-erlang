#!/usr/bin/env bash

set -ex

rm   -rf single_node single_node.log
mkdir -p single_node

echo "Starting single node"
mongod --port 27017 --bind_ip localhost --dbpath single_node --oplogSize 128 > single_node.log &

echo "Waiting on single node MongoDB to start on 27017"
timeout 5m sh -c 'until nc -z localhost 27017; do sleep 1; done'

# Use mongosh if available (MongoDB 6.0+), otherwise use mongo
if command -v mongosh &> /dev/null; then
  MONGO_SHELL="mongosh"
else
  MONGO_SHELL="mongo"
fi

$MONGO_SHELL admin --eval 'db.createUser(
         {user: "user",
          pwd: "test",
          roles: [{role: "userAdminAnyDatabase", db: "admin"},
                  "readWrite"]}
        )'

mongod --shutdown --dbpath single_node

mongod --port 27017 --bind_ip localhost --dbpath single_node --oplogSize 128 >> single_node.log &

echo "Waiting on single node MongoDB to restart with auth on 27017"
timeout 5m sh -c 'until nc -z localhost 27017; do sleep 1; done'

# Wait for MongoDB to be ready to accept authenticated connections
echo "Waiting for MongoDB to accept authenticated connections"
timeout 1m sh -c "until $MONGO_SHELL --username user --password test --eval 'db.serverStatus()' > /dev/null 2>&1; do sleep 2; done"

# Verify that we can auth to the restarted server
$MONGO_SHELL --username user \
      --password test \
      --eval 'db.serverStatus()'
