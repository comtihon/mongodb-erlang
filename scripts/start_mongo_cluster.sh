#!/usr/bin/env bash
# Based on Supercharge's MIT-licensed MongoDB GitHub Action
# https://github.com/supercharge/mongodb-github-action

set -ex

rm   -rf rs0-0 rs0-1 rs0-2 rs0-logs rs0-key
mkdir -p rs0-0 rs0-1 rs0-2 rs0-logs rs0-key

# Replica set key for inter-node auth
echo "notsosecretkey" > rs0-key/key

# MongoDB won't start if the replica set shared key is world-writable
chmod 600 rs0-key/key

echo "Starting replica set nodes"
mongod --replSet rs0 --port 27018 --bind_ip localhost --dbpath rs0-0 --oplogSize 128 >> rs0-logs/rs0-0.log.txt &
mongod --replSet rs0 --port 27019 --bind_ip localhost --dbpath rs0-1 --oplogSize 128 >> rs0-logs/rs0-1.log.txt &
mongod --replSet rs0 --port 27020 --bind_ip localhost --dbpath rs0-2 --oplogSize 128 >> rs0-logs/rs0-2.log.txt &

echo "Waiting on MongoDB to start on 27018"
timeout 5m sh -c 'until nc -z localhost 27018; do sleep 1; done'
echo "Waiting on MongoDB to start on 27019"
timeout 5m sh -c 'until nc -z localhost 27019; do sleep 1; done'
echo "Waiting on MongoDB to start on 27020"
timeout 5m sh -c 'until nc -z localhost 27020; do sleep 1; done'

# Create a replica set from the three nodes
echo "Initiating replica set config"
mongo --port 27018 admin --eval 'rs.initiate(
      {"_id": "rs0", "members": [
      {"_id": 0, "host": "localhost:27018"},
      {"_id": 1, "host": "localhost:27019"},
      {"_id": 2, "host": "localhost:27020"}]})'

echo "Waiting for replica set to come up"
timeout 1m sh -c "until mongo --quiet --host rs0/localhost:27018,localhost:27019,localhost:27020 admin --eval 'rs.status()'; do sleep 1; done"

# Add a user for authentication
mongo --host rs0/localhost:27018,localhost:27019,localhost:27020 \
      admin \
      --eval 'db.createUser(
                {user: "rs_user",
                 pwd: "rs_test",
                 roles: [{role: "clusterAdmin", db: "admin"},
                         {role: "userAdminAnyDatabase", db: "admin"},
                         "readWrite"]})'

# Shutdown nodes in replica set
mongod --shutdown --dbpath rs0-0
mongod --shutdown --dbpath rs0-1
mongod --shutdown --dbpath rs0-2

# Restart replica set nodes with authentication enabled
mongod --replSet rs0 --auth --keyFile rs0-key/key --port 27018 --bind_ip localhost --dbpath rs0-0 --oplogSize 128 >> rs0-logs/rs0-0-auth.log.txt &
mongod --replSet rs0 --auth --keyFile rs0-key/key --port 27019 --bind_ip localhost --dbpath rs0-1 --oplogSize 128 >> rs0-logs/rs0-1-auth.log.txt &
mongod --replSet rs0 --auth --keyFile rs0-key/key --port 27020 --bind_ip localhost --dbpath rs0-2 --oplogSize 128 >> rs0-logs/rs0-2-auth.log.txt &

echo "Waiting on MongoDB to restart on 27018"
timeout 5m sh -c 'until nc -z localhost 27018; do sleep 1; done'
echo "Waiting on MongoDB to restart on 27019"
timeout 5m sh -c 'until nc -z localhost 27019; do sleep 1; done'
echo "Waiting on MongoDB to restart on 27020"
timeout 5m sh -c 'until nc -z localhost 27020; do sleep 1; done'

# Verify that we can auth to the restarted replica set
mongo --host rs0/localhost:27018,localhost:27019,localhost:27020 \
      --username rs_user \
      --password rs_test \
      --authenticationDatabase admin \
      --eval 'db.serverStatus()' > /dev/null
