#!/bin/bash
set -e

# Start MongoDB with authentication on port 27021 for auth tests
# Note: Port 27017 is used by single node, 27018-27020 by replica set cluster
echo "Starting MongoDB with authentication on port 27021..."

# Clean up any existing data and lock files
rm -rf /tmp/mongodb-auth-data
mkdir -p /tmp/mongodb-auth-data

# Kill any existing mongod on port 27021
pkill -f "mongod --port 27021" || true
sleep 2

# Start MongoDB with auth in background
mongod --port 27021 --dbpath /tmp/mongodb-auth-data --logpath /tmp/mongodb-auth.log --fork --auth

# Wait for MongoDB to be ready
echo "Waiting for MongoDB to start..."
for i in {1..30}; do
  if mongosh --port 27021 --eval "db.adminCommand('ping')" --quiet > /dev/null 2>&1; then
    echo "MongoDB is ready"
    break
  fi
  if [ $i -eq 30 ]; then
    echo "MongoDB failed to start"
    cat /tmp/mongodb-auth.log
    exit 1
  fi
  sleep 1
done

# Create admin user
echo "Creating admin user..."
mongosh --port 27021 admin --eval "
  db.createUser({
    user: 'admin',
    pwd: 'admin123',
    roles: [ { role: 'root', db: 'admin' } ]
  })
" --quiet

# Create test user
echo "Creating test user..."
mongosh --port 27021 -u admin -p admin123 --authenticationDatabase admin test --eval "
  db.createUser({
    user: 'testuser',
    pwd: 'testpass',
    roles: [ { role: 'readWrite', db: 'test' } ]
  })
" --quiet

echo "MongoDB with authentication started successfully on port 27021"
