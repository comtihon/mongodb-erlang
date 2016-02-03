This is the MongoDB driver for Erlang. [MongoDB](http://www.mongodb.org) is a document-oriented database management system.
A driver is a client library that provides an API for connecting to MongoDB servers, performing queries and updates on
those servers, and performing administrative tasks like creating indexes and viewing statistics. This version of the driver
supports connecting to a single server or replica set, and pooling of both types of connections. Both connection types
and pools are thread-safe, i.e. multiple processes can use the same connection/pool simultaneously without interfering
with each other. This driver is implemented as an Erlang application named *mongodb*. It depends on another Erlang library
application named [*bson*](http://github.com/mongodb/bson-erlang), which defines the document type and its standard binary
representation. You need both of these. Below we describe the mongodb application; you should also see the bson application
to understand the document type.

### Installing
Download and compile the application

	$ git clone git://github.com/comtihon/mongodb-erlang.git mongodb
	$ cd mongodb
	$ make

### Starting
The bson application needs to be started before starting mongodb application

    > application:start (bson).

The crypto application needs to be started if you plan to use authorization to mongodb-server 3+.
    
    > application:start (crypto).

The mongodb application needs be started before using (to initialize an internal ets table of counters)

	> application:start (mongodb).

Although the mongodb application includes several modules, you should only need to use top-level driver interfaces *mongo* or *mongoc*. *mongo* can be used for direct connections to a single mongod or mongos server. Use *mongoc* for connection to sharded or replica set Mongo deployments (As a matter of fact mongoc is built upon mongo module and also can be used for connections to a single server. Also it has built-in support of pooling connections so you won't be in any need of using extra pooling libraries like a poolboy). Likewise, you should only need to use the *bson* module in the bson application.


mongo -- direct connection client
---------------------------------

### Connecting
To connect to a database `test` on mongodb server listening on `localhost:27017` (or any address & port of your choosing)
use `mongo:connect/1`.

	> Database = <<"test">>.
	> {ok, Connection} = mongo:connect ([{database, Database}]).

`mongo:connect` returns `{error, Reason}` if it failed to connect. See arguments you can pass in `mongo.erl` type spec:

    -type arg() :: {database, database()}
    | {login, binary()}
    | {password, binary()}
    | {w_mode, write_mode()}
    | {r_mode, read_mode()}
    | {host, list()}
    | {port, integer()}
    | {register, atom() | fun()}.
To connect mc_worker in your supervised pool, use `mc_worker:start_link/1` instead and pass all args to it.

`safe`, along with `{safe, GetLastErrorParams}` and `unsafe`, are write-modes. Safe mode makes a *getLastError* request
after every write in the sequence. If the reply says it failed then the rest of the sequence is aborted and returns 
`{failure, {write_failure, Reason}}`, or `{failure, not_master}` when connected to a slave. An example write
failure is attempting to insert a duplicate key that is indexed to be unique. Alternatively, unsafe mode issues every
write without a confirmation, so if a write fails you won't know about it and remaining operations will be executed.
This is unsafe but faster because you there is no round-trip delay.  

`master`, along with `slave_ok`, are read-modes. `master` means every query in the sequence must read fresh data (from
a master/primary server). If the connected server is not a master then the first read will fail, the remaining operations
will be aborted, and `mongo:do` will return `{failure, not_master}`. `slave_ok` means every query is allowed to read
stale data from a slave/secondary (fresh data from a master is fine too).  

If you set `{register, Name}` option - mc_worker process will be registered on this Name, or you can pass function 
`fun(pid())`, which it runs with self pid.    
If you set `{login, Login}` and `{password, Password}` options - mc_worker will try to authenticate to the database.  

### Writing
After you connected to your database - you can carry out write operations, such as `insert`, `update` and `delete`:

    > Collection = <<"test">>.
    > mongo:insert(Connection, Collection, [
          {<<"name">>, <<"Yankees">>, <<"home">>, {<<"city">>, <<"New York">>, <<"state">>, <<"NY">>}, <<"league">>, <<"American">>},
          {<<"name">>, <<"Mets">>, <<"home">>, {<<"city">>, <<"New York">>, <<"state">>, <<"NY">>}, <<"league">>, <<"National">>},
          {<<"name">>, <<"Phillies">>, <<"home">>, {<<"city">>, <<"Philadelphia">>, <<"state">>, <<"PA">>}, <<"league">>, <<"National">>},
          {<<"name">>, <<"Red Sox">>, <<"home">>, {<<"city">>, <<"Boston">>, <<"state">>, <<"MA">>}, <<"league">>, <<"American">>}
        ]),
An insert example (from `mongo_SUITE` test module). `Connection` is your Connection, got `from mongo:connect`, `Collection`
is your collection name, `Doc` is something, you want to save.
Doc will be returned, if insert succeeded. If Doc doesn't contains `_id` field - an updated Doc will be returned - with
automatically generated '_id' fields. If error occurred - Connection will fall.

    > mongo:delete(Connection, Collection, Selector).
Delete example. `Connection` is your Connection, `Collection` - is a collection you want to clean. `Selector` is the
rules for cleaning. If you want to clean everything - pass empty `{}`.  
You can also use maps instead bson documents:

    > Collection = <<"test">>.
    > mongo:insert(Connection, Collection, #{<<"name">> => <<"Yankees">>, <<"home">> =>
      #{<<"city">> => <<"New York">>, <<"state">> => <<"NY">>}, <<"league">> => <<"American">>}),

### Reading
To call read operations use `find`, `find_one`:

    > Cursor = mongo:find(Connection, Collection, Selector)
All params similar to `delete`.
The difference between `find` and `find_one` is in return. Find_one just returns your result, while find returns you a
`Cursor` - special process' pid. You can query data through the process with the help of `mc_cursor' module.

    > Result = mc_cursor:rest(Cursor),
    > mc_cursor:close(Cursor),
__Important!__ Do not forget to close cursors after using them!

### Advance reading
To search for params - specify `Selector`:

    mongo:find_one(Connection, Collection, {<<"key">>, <<"123">>}).
will return one document from collection Collection with key == <<"123">>.

    mongo:find_one(Connection, Collection, {<<"key">>, <<"123">>, <<"value">>, <<"built_in">>}).
will return one document from collection Collection with key == <<"123">> `and` value == <<"built_in">>.
Tuples `{<<"key">>, <<"123">>}` in first example and `{<<"key">>, <<"123">>, <<"value">>, <<"built_in">>}` are selectors.

For filtering result - use `Projector`:

    mongo:find_one(Connection, Collection, {}, [{projector, {<<"value">>, true}]).
will return one document from collection Collection with fetching `only` _id and value.

    mongo:find_one(Connection, Collection, {}, [{projector, {<<"key">>, false, <<"value">>, false}}]).
will return your data without key and value params. If there is no other data - only _id will be returned.
__Important!__ For empty projector use `[]` instead `{}`. For empty selector use `{}`.

### Updating
To add or update field in document - use `mongo:update` function with `$set` param.
This updates selected fields:

    Command = {<<"$set">>, {
        <<"quantity">>, 500,
        <<"details">>, {<<"model">>, "14Q3", <<"make">>, "xyz"},
        <<"tags">>, ["coats", "outerwear", "clothing"]
    }},
    mongo:update(Connection, Collection, {<<"_id">>, 100}, Command),
This will add new field `expired`, if there is no such field, and set it to true.

    Command = {<<"$set">>, {<<"expired">>, true}},
    mongo:update(Connection, Collection, {<<"_id">>, 100}, Command),
This will update fields in nested documents.

    Command = {<<"$set">>, {<<"details.make">>, "zzz"}},
    mongo:update(Connection, Collection, {<<"_id">>, 100}, Command),
This will update elements in array.

    Command = {<<"$set">>, {
        <<"tags.1">>, "rain gear",
        <<"ratings.0.rating">>, 2
      }},
    mongo:update(Connection, Collection, {'_id', 100}, Command),
For result of executing this functions - see mongo_SUITE update test.

### Creating indexes
To create indexes - use `mongo:ensure_index/3` command:

    mongo:ensure_index(Connection, Collection, {<<"key">>, {<<"index">>, 1}}).  %simple
    mongo:ensure_index(Connection, Collection, {<<"key">>, {<<"index">>, 1}, <<"name">>, <<"MyI">>}).  %advanced
    mongo:ensure_index(Connection, Collection, {<<"key">>, {<<"index">>, 1}, <<"name">>, <<"MyI">>, <<"unique">>, true, <<"dropDups">>, true}).  %full

ensure_index takes `mc_worker`' pid or atom name as first parameter, collection, where to create index, as second
parameter and bson document with index
specification - as third parameter. In index specification one can set all or only some parameters.
If index specification is not full - it is automatically filled with values: `name, Name, unique, false, dropDups,
false`, where `Name` is index's key.

### Administering
This driver does not provide helper functions for commands. Use `mongo:command` directly and refer to the
[MongoDB documentation](http://www.mongodb.org/display/DOCS/Commands) for how to issue raw commands.

### Authentication
To authenticate use function `mongo:connect`, or `mc_worker:start_link([...{login, <<"login">>}, {password, <<"password">>}...]`  
Login and password should be binaries!

### Timeout

By default timeout for all connections to connection gen_server is `infinity`. If you found problems with it - you can
modify timeout.
To modify it just add `mc_worker_call_timeout` with new value to your applications's env config.

Timeout for operations with cursors may be explicity passed to `mc_cursor:next/2`, `mc_cursor:take/3`, `mc_cursor:rest/2`, and `mc_cursor:foldl/5` functions, by default used value of `cursor_timeout` from application config, or `infinity` if `cursor_timeout` not specified.

### Pooling

For pooling use [Poolboy](https://github.com/devinus/poolboy) with mc_worker as pool workers.


mongoc -- client with automatic MongoDB topology discovery and monitoring
-------------------------------------------------------------------------

### Connection

For opening connection to a MongoDB you can use this call of mongoc:connect method:


    {ok, Topology} = mongoc:connect( Seed, Options, WorkerOptions )


Where **Seed** contains information about host names and ports to connect and info about topology of MongoDB deployment.

So you can pass just a hostname with port (or tuple with single key) for connection to a single server deployment:

    "hostname:27017"
    { single, "hostname:27017" }


If you want to connect to a replica set _ReplicaSetName_ use this format of Seeds value:

    { rs, <<"ReplicaSetName">>, [ "hostname1:port1", "hostname2:port2"] }

To connect to a sharded cluster of mongos:

    { sharded,  "hostname1:port1", "hostname2:port2"] }

And if you want your MongoDB deployment metadata to be auto revered use unknow id in Seed tuple:   

    { unknown,  "hostname1:port1", "hostname2:port2"] }


mongoc topology **Options**

    [
        { name,  Name },    % Name should be used for mongoc topology process to be registered with

        { minPoolSize, 5 }, % Minimum and Maximum connections pool size
        { maxPoolSize, 10 },

        { localThresholdMS, 1000 }, % secondaries only which RTTs fit in window from lower RTT to lower RTT + localThresholdMS could be selected for handling user's requests

        { connectTimeoutMS, 20000 },
        { socketTimeoutMS, 100 },

        { serverSelectionTimeoutMS, 30000 }, % max time appropriate server should be select by
        { waitQueueTimeoutMS, 1000 }, % max time for waiting worker to be available in the pool

        { heartbeatFrequencyMS, 10000 },    %  delay between Topology rescans
        { minHeartbeatFrequencyMS, 1000 },

        { rp_mode, primary }, % default ReadPreference mode - primary, secondary, primaryPreferred, secondaryPreferred, nearest

        { rp_tags, [ {tag,1}, {tag2,2} ] }, % tags mongodb servers should be tagged with for becoming candidates for server selection ( [] is a default value, no tags will be checked )
    ]

mongoc **WorkerOptions** (as described in mongo Connecting chapter)

    -type arg() :: {database, database()}     % default database and it is also the initial db for auth purposes, later you can give other db name in requests
    | {login, binary()}
    | {password, binary()}
    | {w_mode, write_mode()}.



mongoc:insert, mongoc:update, mongoc:delete, mongoc:delete_one,  mongoc:ensure_index  methods are all the same as their mongo: analogs, only you must use the result of mongoc:connect result as a Connection identifier.

Also in all mongoc calls you can use tuple { Database, Collection } instead of just Collection name.


mongoc:find, mongoc:find_one, mongoc:count, mongoc:command are also similar with mongo: calls but you can pass readPreference options to this calls. 

For example: 


    mongoc:find_one(Topology, { <<"dbname", <<"collname">> }, {<<"key">>, <<"123">>}, [{ rp_mode, secondaryPreferred}, { rp_tags, [] } ] ).





### More Documentation

[API Docs](http://api.mongodb.org/erlang/mongodb/) - Documentation generated from source code comments
