#### 1.0

* changed `mongo:connect` interfaces. Now options in connect/6 is proplist, where user can set Host and Port. 
In connect/1,3,5 default host and port are used.
* set default host:port as 127.0.0.1:27017. Is used, when no other specified.
* removed `auth` from mongo module. Made auth automatically in connect/3,5,6
* made `mc_worker:start_link` accept one parameter - a proplist with configuration.
* add ability for mc_worker to be a registered process if needed.


#### 2.0

* added SCRAM-SHA-1 auth support, used in mongodb 3+
* added server version request before each auth to determine the default auth mechanizm.

#### 2.1

* moved to really binary bson. Atom interface exists for compatibility, but it converted to binary and all results return
in binary. For max speed use binary to avoid conversion. __Important__ custom commands in update/find/insert should be 
binary! F.e.:  


	Command3 = {<<"$set">>, {
        <<"tags.1">>, "rain gear",
        <<"ratings.0.rating">>, 2
    }},
    mongo:update(Connection, Collection, {<<"_id">>, 100}, Command3),
instead of '$set' as it will lead to crash.

#### 3.0

* changed interface of mongo `connect`, `update`, `find`, `find_one`
* make find, update and insert operations return map.
* added `mc_worker:hibernate/1` to put worker into hibernate.
* fasten mc_worker spawning by moving sending ack before auth 
* made mc_cursor return `error` instead `[]` when connection is down.
* mc_worker's socket is now `{active, true}`, which allows to handle disconnect at a moment

#### 3.3 (tag v0.8.2)

* add ssl support
* add return ok | error for write requests

### 4.0 (tag v0.9)
* mongo.erl -> mc_worker_api.erl
* mongo_api.erl with api functions for mongoc
* changed api for mc_worker_api and mongoc