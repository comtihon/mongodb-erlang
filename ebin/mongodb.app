{application, mongodb,
 [{description, "Client interface to MongoDB, also known as the driver. See www.mongodb.org"},
  {vsn, "0.0"},
  {modules, [mongodb_app, mongo, mongo_protocol, mongo_connect, mongo_query, mongo_cursor, mongodb_tests]},
  {registered, [oid_counter, oid_machineprocid, requestid_counter]},
  {applications, [kernel, stdlib]},
  {mod, {mongodb_app, []}}
 ]}.