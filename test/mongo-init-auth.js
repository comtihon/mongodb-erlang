// MongoDB initialization script for authentication tests
// This runs when the container first starts up

// Switch to the test database
db = db.getSiblingDB('test');

// Create test user with readWrite permissions
db.createUser({
  user: 'testuser',
  pwd: 'testpass',
  roles: [
    {
      role: 'readWrite',
      db: 'test'
    }
  ]
});

print('Created testuser in test database');
