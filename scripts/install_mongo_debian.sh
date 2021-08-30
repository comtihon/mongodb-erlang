#!/usr/bin/env bash

set -ex
# e.g. trim 4.4.8 to 4.4
group=`echo $1 | sed 's/\(.*\)[.].*/\1/'`
apt-get update
apt-get --yes install gnupg wget netcat
wget -qO - https://www.mongodb.org/static/pgp/server-${group}.asc | apt-key add -
echo "deb http://repo.mongodb.org/apt/debian buster/mongodb-org/${group} main" | tee /etc/apt/sources.list.d/mongodb-org-${group}.list
apt-get update
apt-get install -y mongodb-org=${1} mongodb-org-server=${1} mongodb-org-shell=${1} mongodb-org-mongos=${1} mongodb-org-tools=${1}
echo "mongodb-org hold" | dpkg --set-selections
echo "mongodb-org-server hold" | dpkg --set-selections
echo "mongodb-org-shell hold" | dpkg --set-selections
echo "mongodb-org-mongos hold" | dpkg --set-selections
echo "mongodb-org-tools hold" | dpkg --set-selections
mongod --version
