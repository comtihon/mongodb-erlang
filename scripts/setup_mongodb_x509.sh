#!/usr/bin/env bash

set -ex

CERT_DIR=/tmp/mongodb-x509-certs
DATA_DIR=/tmp/mongodb-x509-data
PORT=27022

rm -rf "$CERT_DIR" "$DATA_DIR"
mkdir -p "$CERT_DIR" "$DATA_DIR"
cd "$CERT_DIR"

# Generate server CA and certificate
openssl genrsa -out server-ca.key 4096
openssl req -new -x509 -days 365 -key server-ca.key -out server-ca.pem \
    -subj "/C=DE/ST=Berlin/L=Berlin/O=MongoDB Test/OU=Server CA/CN=MongoDB Server CA"

openssl genrsa -out server.key 4096
openssl req -new -key server.key -out server.csr \
    -subj "/C=DE/ST=Berlin/L=Berlin/O=MongoDB Test/OU=Server/CN=localhost"

cat > server-ext.cnf << EOF
basicConstraints = CA:FALSE
keyUsage = digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth
subjectAltName = DNS:localhost,IP:127.0.0.1
EOF

openssl x509 -req -days 365 -in server.csr -CA server-ca.pem -CAkey server-ca.key \
    -CAcreateserial -out server.pem -extfile server-ext.cnf
cat server.key server.pem > server-combined.pem

# Generate client CA and valid client certificate
openssl genrsa -out client-ca.key 4096
openssl req -new -x509 -days 365 -key client-ca.key -out client-ca.pem \
    -subj "/C=DE/ST=Berlin/L=Berlin/O=MongoDB Test/OU=Client CA/CN=MongoDB Client CA"

openssl genrsa -out client-valid.key 4096
openssl req -new -key client-valid.key -out client-valid.csr \
    -subj "/C=DE/ST=Berlin/L=Berlin/O=MongoDB Test/OU=Clients/CN=testclient"

cat > client-ext.cnf << EOF
basicConstraints = CA:FALSE
keyUsage = digitalSignature
extendedKeyUsage = clientAuth
EOF

openssl x509 -req -days 365 -in client-valid.csr -CA client-ca.pem -CAkey client-ca.key \
    -CAcreateserial -out client-valid.pem -extfile client-ext.cnf
cat client-valid.key client-valid.pem > client-valid-combined.pem

# Generate invalid client certificate (different CA)
openssl genrsa -out invalid-ca.key 4096
openssl req -new -x509 -days 365 -key invalid-ca.key -out invalid-ca.pem \
    -subj "/C=DE/ST=Berlin/L=Berlin/O=Untrusted Org/OU=Invalid CA/CN=Invalid CA"

openssl genrsa -out client-invalid.key 4096
openssl req -new -key client-invalid.key -out client-invalid.csr \
    -subj "/C=DE/ST=Berlin/L=Berlin/O=Untrusted Org/OU=Invalid/CN=badclient"

openssl x509 -req -days 365 -in client-invalid.csr -CA invalid-ca.pem -CAkey invalid-ca.key \
    -CAcreateserial -out client-invalid.pem -extfile client-ext.cnf
cat client-invalid.key client-invalid.pem > client-invalid-combined.pem

cat server-ca.pem client-ca.pem > combined-ca.pem

# Get client subject for MongoDB user
CLIENT_SUBJECT=$(openssl x509 -in client-valid.pem -noout -subject -nameopt RFC2253 | sed 's/subject=//')
echo "$CLIENT_SUBJECT" > client-valid-subject.txt

chmod 600 *.key *.pem

# Start MongoDB with TLS
mongod \
    --port $PORT \
    --dbpath "$DATA_DIR" \
    --logpath "$DATA_DIR/mongod.log" \
    --fork \
    --tlsMode requireTLS \
    --tlsCertificateKeyFile "$CERT_DIR/server-combined.pem" \
    --tlsCAFile "$CERT_DIR/combined-ca.pem" \
    --tlsAllowConnectionsWithoutCertificates \
    --auth

echo "Waiting for MongoDB to start on $PORT"
timeout 5m sh -c "until nc -z localhost $PORT; do sleep 1; done"

if command -v mongosh &> /dev/null; then
  MONGO_SHELL="mongosh"
else
  MONGO_SHELL="mongo"
fi

TLS_OPTS="--tls --tlsCertificateKeyFile $CERT_DIR/client-valid-combined.pem --tlsCAFile $CERT_DIR/server-ca.pem"

$MONGO_SHELL --port $PORT $TLS_OPTS admin --eval "
  db.createUser({
    user: 'admin',
    pwd: 'admin123',
    roles: ['root']
  })
"

$MONGO_SHELL --port $PORT $TLS_OPTS -u admin -p admin123 --authenticationDatabase admin --eval "
  db.getSiblingDB('\$external').runCommand({
    createUser: '$CLIENT_SUBJECT',
    roles: [
      { role: 'readWrite', db: 'test' },
      { role: 'userAdminAnyDatabase', db: 'admin' }
    ]
  })
"

echo "MongoDB X509 auth running on port $PORT"
echo "Client subject: $CLIENT_SUBJECT"
