#!/usr/bin/env bash

set -ex
apt-get update
apt-get --yes install gnupg wget netcat
wget -qO - https://www.mongodb.org/static/pgp/server-${1}.asc | apt-key add -
echo "deb http://repo.mongodb.org/apt/debian buster/mongodb-org/${1} main" | tee /etc/apt/sources.list.d/mongodb-org-${1}.list
apt-get update
apt-get install -y mongodb-org=${2} mongodb-org-server=${2} mongodb-org-shell=${2} mongodb-org-mongos=${2} mongodb-org-tools=${2}
echo "mongodb-org hold" | dpkg --set-selections
echo "mongodb-org-server hold" | dpkg --set-selections
echo "mongodb-org-shell hold" | dpkg --set-selections
echo "mongodb-org-mongos hold" | dpkg --set-selections
echo "mongodb-org-tools hold" | dpkg --set-selections
mongod --version


set -ex
apt-get update
apt-get --yes install gnupg wget netcat
wget -qO - https://www.mongodb.org/static/pgp/server-4.4.asc | apt-key add -
echo "deb http://repo.mongodb.org/apt/debian buster/mongodb-org/4.4 main" | tee /etc/apt/sources.list.d/mongodb-org-4.4.list
apt-get update
apt-get install -y mongodb-org=4.4.8 mongodb-org-server=4.4.8 mongodb-org-shell=4.4.8 mongodb-org-mongos=4.4.8 mongodb-org-tools=4.4.8
echo "mongodb-org hold" | dpkg --set-selections
echo "mongodb-org-server hold" | dpkg --set-selections
echo "mongodb-org-shell hold" | dpkg --set-selections
echo "mongodb-org-mongos hold" | dpkg --set-selections
echo "mongodb-org-tools hold" | dpkg --set-selections
mongod --version
