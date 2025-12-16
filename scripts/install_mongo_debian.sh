#!/usr/bin/env bash

set -ex
# e.g. trim 4.4.8 to 4.4
group=`echo $1 | sed 's/\(.*\)[.].*/\1/'`
apt-get update
apt-get --yes install gnupg wget netcat-openbsd curl

# Detect Debian version
debian_version=$(cat /etc/debian_version | cut -d. -f1)

# For Debian 12 (Bookworm) with MongoDB 6.0+, use Ubuntu 22.04 packages (compatible with libssl3)
if [[ "$debian_version" == "12" ]] && [[ "$group" == "6.0" || "$group" == "7.0" ]]; then
  wget -qO - https://www.mongodb.org/static/pgp/server-${group}.asc | apt-key add -
  echo "deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/ubuntu jammy/mongodb-org/${group} multiverse" | tee /etc/apt/sources.list.d/mongodb-org-${group}.list
  apt-get update

  if [[ "$group" == "7.0" ]]; then
    apt-get install -y mongodb-org mongodb-mongosh mongodb-org-database-tools-extra
    echo "mongodb-org hold" | dpkg --set-selections
    echo "mongodb-mongosh hold" | dpkg --set-selections
    echo "mongodb-org-database-tools-extra hold" | dpkg --set-selections
  else
    # MongoDB 6.0 still uses old package names
    apt-get install -y mongodb-org=${1} mongodb-org-server=${1} mongodb-org-shell=${1} mongodb-org-mongos=${1} mongodb-org-tools=${1}
    echo "mongodb-org hold" | dpkg --set-selections
    echo "mongodb-org-server hold" | dpkg --set-selections
    echo "mongodb-org-shell hold" | dpkg --set-selections
    echo "mongodb-org-mongos hold" | dpkg --set-selections
    echo "mongodb-org-tools hold" | dpkg --set-selections
  fi
else
  # For older Debian versions, use Debian Buster packages
  wget -qO - https://www.mongodb.org/static/pgp/server-${group}.asc | apt-key add -
  echo "deb http://repo.mongodb.org/apt/debian buster/mongodb-org/${group} main" | tee /etc/apt/sources.list.d/mongodb-org-${group}.list
  apt-get update

  if [[ "$group" == "7.0" ]]; then
    apt-get install -y mongodb-org mongodb-mongosh mongodb-org-database-tools-extra
    echo "mongodb-org hold" | dpkg --set-selections
    echo "mongodb-mongosh hold" | dpkg --set-selections
    echo "mongodb-org-database-tools-extra hold" | dpkg --set-selections
  else
    apt-get install -y mongodb-org=${1} mongodb-org-server=${1} mongodb-org-shell=${1} mongodb-org-mongos=${1} mongodb-org-tools=${1}
    echo "mongodb-org hold" | dpkg --set-selections
    echo "mongodb-org-server hold" | dpkg --set-selections
    echo "mongodb-org-shell hold" | dpkg --set-selections
    echo "mongodb-org-mongos hold" | dpkg --set-selections
    echo "mongodb-org-tools hold" | dpkg --set-selections
  fi
fi

mongod --version
