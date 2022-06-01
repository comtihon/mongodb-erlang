This is the [MongoDB](https://www.mongodb.org/) driver for Erlang.

[![Run tests](https://github.com/comtihon/mongodb-erlang/actions/workflows/test.yml/badge.svg)](https://github.com/comtihon/mongodb-erlang/actions/workflows/test.yml)
[![Enot](https://enot.justtech.blog/badge?full_name=comtihon/mongodb-erlang)](https://enot.justtech.blog)

### Usage
Add this repo as the dependency:
Rebar

    {deps, [
      {mongodb, ".*",
       {git, "git://github.com/comtihon/mongodb-erlang", {tag, "<Latest tag>"}}}
       ]
    }
Erlang.mk

    DEPS = mongodb
    dep_mongodb = git https://github.com/comtihon/mongodb-erlang.git <Latest tag>
Where __Latest tag__ is the latest tag from github.

### Installing
If you want to use it from command line - download and compile the application:

	$ git clone git://github.com/comtihon/mongodb-erlang.git mongodb
	$ cd mongodb
	$ make
You will need `erlang` 18+ and `make` installed.

### Starting and using the api
Start all applications, needed by mongodb

	> application:ensure_all_started (mongodb).

__Important__:
`mongoc` API was changed in `3.0.0`.
`mc_cursor` API was changed in `3.0.0.`

This driver has two api modules - `mc_worker_api` and `mongo_api`.
`mc_worker_api` works directly with one connection, while all `mongo_api`
interfaces refer to `mongoc` pool. Although `mongoc` is not stable for now
you should use it if you have shard and need to determine mongo topology.
If you are choosing between using
[mongos](https://docs.mongodb.com/manual/reference/program/mongos/) and
using mongo shard with `mongo_api` - prefer mongos and use `mc_worker_api`.

mc_worker_api -- direct connection client
---------------------------------

### Connecting
To connect to a database `test` on mongodb server listening on
`localhost:27017` (or any address & port of your choosing)
use `mc_worker_api:connect/1`.

	> Database = <<"test">>.
	> {ok, Connection} = mc_worker_api:connect ([{database, Database}]).

`mc_worker_api:connect` returns `{error, Reason}` if it failed to connect.
 See arguments you can pass in `mc_worker_api.erl` type spec:

    -type arg() :: {database, database()}
    | {login, binary()}
    | {password, binary()}
    | {w_mode, write_mode()}
    | {r_mode, read_mode()}
    | {host, list()}
    | {port, integer()}
    | {register, atom() | fun()}
    | {next_req_fun, fun()}.
To connect mc_worker in your supervised pool, use `mc_worker:start_link/1`
instead and pass all args to it.

`safe`, along with `{safe, GetLastErrorParams}` and `unsafe`, are
write-modes. Safe mode makes a *getLastError* request
after every write in the sequence. If the reply says it failed then
the rest of the sequence is aborted and returns
`{failure, {write_failure, Reason}}`, or `{failure, not_master}` when
connected to a slave. An example write
failure is attempting to insert a duplicate key that is indexed to be
unique. Alternatively, unsafe mode issues every
write without a confirmation, so if a write fails you won't know about
it and remaining operations will be executed.
This is unsafe but faster because you there is no round-trip delay.

`master`, along with `slave_ok`, are read-modes. `master` means every
query in the sequence must read fresh data (from
a master/primary server). If the connected server is not a master then
the first read will fail, the remaining operations
will be aborted, and `mongo:do` will return `{failure, not_master}`.
`slave_ok` means every query is allowed to read
stale data from a slave/secondary (fresh data from a master is fine too).

Read-modes only apply to the deprecated mc_worker_api query commands. Pass
a `readopts` map, like `#{<<"mode">> => <<"primary">>}` to an `mc_worker_api`
function like
`find_one(Conn, Coll, Selector, [{readopts, #{<<"mode">> => <<"primary">>}}])`
for the new API.

If you set `{register, Name}` option - mc_worker process will be
registered on this Name, or you can pass function
`fun(pid())`, which it runs with self pid.
If you set `{login, Login}` and `{password, Password}` options -
mc_worker will try to authenticate to the database.

`next_req_fun` is a function caller every time, when worker sends
request to database. It can be use to optimise pool
usage. When you use poolboy transaction (or mongoc transaction, which
use poolboy transaction) - `mc_worker` sends request
to database and do nothing, waiting for reply. You can use
`{next_req_fun, fun() -> poolboy:checkin(?DBPOOL, self()) end}`
to make workers return to pool as soon as they finish request.
When response from database comes back - it will be saved in
mc_worker msgbox. Msgbox will be processed just before the next
call to mc_worker.
__Notice__, that poolboy's pool should be created with `{strategy, fifo}`
to make uniform usage of pool workers.

### Writing
After you connected to your database - you can carry out write operations,
such as `insert`, `update` and `delete`:

    > Collection = <<"test">>.
    > mc_worker_api:insert(Connection, Collection, [
                                                       #{<<"name">> => <<"Yankees">>,
                                                         <<"home">> => #{<<"city">> => <<"New York">>, <<"state">> => <<"NY">>},
                                                         <<"league">> => <<"American">>},
                                                       #{<<"name">> => <<"Mets">>,
                                                         <<"home">> => #{<<"city">> => <<"New York">>, <<"state">> => <<"NY">>},
                                                         <<"league">> => <<"National">>},
                                                       #{<<"name">> => <<"Phillies">>,
                                                         <<"home">> => #{<<"city">> => <<"Philadelphia">>, <<"state">> => <<"PA">>},
                                                         <<"league">> => <<"National">>},
                                                       #{<<"name">> => <<"Red Sox">>,
                                                         <<"home">>=> #{<<"city">> => <<"Boston">>, <<"state">> => <<"MA">>},
                                                         <<"league">> => <<"American">>}
                                                     ]),
An insert example (from `mongo_SUITE` test module). `Connection` is your
Connection, got `from mc_worker_api:connect`, `Collection`
is your collection name, `Doc` is something, you want to save.
Doc will be returned, if insert succeeded. If Doc doesn't contains `_id`
 field - an updated Doc will be returned - with
automatically generated '_id' fields. If error occurred - Connection will
 fall.

    > mc_worker_api:delete(Connection, Collection, Selector).
Delete example. `Connection` is your Connection, `Collection` - is a
collection you want to clean. `Selector` is the
rules for cleaning. If you want to clean everything - pass empty `{}`.
You can also use maps instead bson documents:

    > Collection = <<"test">>.
    > mc_worker_api:insert(Connection, Collection, #{<<"name">> => <<"Yankees">>, <<"home">> =>
      #{<<"city">> => <<"New York">>, <<"state">> => <<"NY">>}, <<"league">> => <<"American">>}),

### Reading
To call read operations use `find`, `find_one`:

    > {ok, Cursor} = mc_worker_api:find(Connection, Collection, Selector)
All params similar to `delete`.
The difference between `find` and `find_one` is in return. Find_one just
returns your result, while find returns you a
`Cursor` - special process' pid. You can query data through the process
with the help of `mc_cursor' module.

    > Result = mc_cursor:next(Cursor),
    > mc_cursor:close(Cursor),
__Important!__ Do not forget to close cursors after using them!
`mc_cursor:rest` closes the cursor automatically.

To search for params - specify `Selector`:

    mc_worker_api:find_one(Connection, Collection, #{<<"key">> => <<"123">>}).
will return one document from collection Collection with key == <<"123">>.

    mc_worker_api:find_one(Connection, Collection, #{<<"key">> => <<"123">>, <<"value">> => <<"built_in">>}).
will return one document from collection Collection with key == <<"123">>
`and` value == <<"built_in">>.
Tuples `{<<"key">>, <<"123">>}` in first example and `{<<"key">>, <<"123">>,
 <<"value">>, <<"built_in">>}` are selectors.

For filtering result - use `Projector`:

    mc_worker_api:find_one(Connection, Collection, {}, #{projector => #{<<"value">> => true}).
will return one document from collection Collection with fetching `only`
_id and value.

    mc_worker_api:find_one(Connection, Collection, {}, #{projector => #{<<"key">> => false, <<"value">> => false}}).
will return your data without key and value params. If there is no other
data - only _id will be returned.

### Updating
To add or update field in document - use `mc_worker_api:update` function
 with `$set` param.
This updates selected fields:

    Command = #{<<"$set">> => #{
        <<"quantity">> => 500,
        <<"details">> => #{<<"model">> => "14Q3", <<"make">> => "xyz"},
        <<"tags">> => ["coats", "outerwear", "clothing"]
    }},
    mc_worker_api:update(Connection, Collection, #{<<"_id">> => 100}, Command),
This will add new field `expired`, if there is no such field, and set it
to true.

    Command = #{<<"$set">> => #{<<"expired">> => true}},
    mc_worker_api:update(Connection, Collection, #{<<"_id">> => 100}, Command),
This will update fields in nested documents.

    Command = #{<<"$set">> => #{<<"details.make">> => "zzz"}},
    mc_worker_api:update(Connection, Collection, #{<<"_id">> => 100}, Command),
This will update elements in array.

    Command = #{<<"$set">> => #{
        <<"tags.1">> => "rain gear",
        <<"ratings.0.rating">> => 2
      }},
    mc_worker_api:update(Connection, Collection, #{'_id' => 100}, Command),
For result of executing this functions - see mongo_SUITE update test.

### Creating indexes
To create indexes - use `mc_worker_api:ensure_index/3` command:

    mc_worker_api:ensure_index(Connection, Collection, #{<<"key">> => #{<<"index">> => 1}}).  %simple
    mc_worker_api:ensure_index(Connection, Collection, #{<<"key">> => #{<<"index">> => 1}, <<"name">> => <<"MyI">>}).  %advanced
    mc_worker_api:ensure_index(Connection, Collection, #{<<"key">> => #{<<"index">> => 1}, <<"name">> => <<"MyI">>, <<"unique">> => true, <<"dropDups">> => true}).  %full

ensure_index takes `mc_worker`' pid or atom name as first parameter,
collection, where to create index, as second
parameter and bson document with index
specification - as third parameter. In index specification one can set all
or only some parameters.
If index specification is not full - it is automatically filled with
values: `name, Name, unique, false, dropDups,
false`, where `Name` is index's key.

### Administering
This driver does not provide helper functions for commands. Use
`mc_worker_api:command` directly and refer to the
[MongoDB documentation](http://www.mongodb.org/display/DOCS/Commands)
for how to issue raw commands.

### Authentication
To authenticate use function `mc_worker_api:connect`, or
`mc_worker:start_link([...{login, <<"login">>}, {password, <<"password">>}...]`

### Timeout

By default timeout for all connections to connection gen_server is `infinity`.
If you found problems with it - you can
modify timeout.
To modify it just add `mc_worker_call_timeout` with new value to your
 applications's env config.

Timeout for operations with cursors may be explicitly passed to `mc_cursor:next/2`,
 `mc_cursor:take/3`, `mc_cursor:rest/2`, and `mc_cursor:foldl/5` functions,
 by default used value of `cursor_timeout` from application config, or `
 infinity` if `cursor_timeout` not specified.

### Pooling

If you need simple pool - use modified [Poolboy](https://github.com/comtihon/poolboy),
which is included in this app's deps. As a worker module use `mc_worker_api`.
If you need pool to mongo shard with determining topology - use `mongo_api`
for automatic topology discovery and monitoring. It uses poolboy inside.


mongoc -- client with automatic MongoDB topology discovery and monitoring
-------------------------------------------------------------------------

You can use `mongo_api.erl` for easy working with mongoc.

### Connection

For opening a connection to a MongoDB server you can call `mongoc:connect/3`:

```erlang
{ok, Topology} = mongoc:connect(Seed, Options, WorkerOptions)
```

Where `Seed` contains information about host names and ports to connect
 and info about topology of MongoDB deployment.

So you can pass just a hostname with port (or tuple with single key) for
connection to a single server deployment:

```erlang
SingleSeed = "hostname:27017",
SingleSeedTuple = { single, "hostname:27017" }
```

If you want to connect to a replica set _ReplicaSetName_ use this format
 of the `Seed` value:

```erlang
ReplicaSeed = { rs, <<"ReplicaSetName">>, [ "hostname1:port1", "hostname2:port2"] }
```

To connect to a sharded cluster of mongos:

```erlang
ShardedSeed = { sharded,  ["hostname1:port1", "hostname2:port2"] }
```

And if you want your MongoDB deployment metadata to be auto-discovered use
the `unknown` type in the `Seed` tuple:

```erlang
AutoDiscoveredSeed = { unknown,  ["hostname1:port1", "hostname2:port2"] }
```

mongoc topology **Options**

```erlang
[
    { name,  Name },    % Name should be used for mongoc pool to be registered with
    { register,  Name },    % Name should be used for mongoc topology process to be registered with

    { pool_size, 5 }, % pool size on start
    { max_overflow, 10 },	% number of overflow workers be created, when all workers from pool are busy
    { overflow_ttl, 1000 }, % number of milliseconds for overflow workers to stay in pool before terminating
    { overflow_check_period, 1000 }, % overflow_ttl check period for workers (in milliseconds)

    { localThresholdMS, 1000 }, % secondaries only which RTTs fit in window from lower RTT to lower RTT + localThresholdMS could be selected for handling user's requests

    { connectTimeoutMS, 20000 },
    { socketTimeoutMS, 100 },

    { serverSelectionTimeoutMS, 30000 }, % max time appropriate server should be select by
    { waitQueueTimeoutMS, 1000 }, % max time for waiting worker to be available in the pool

    { heartbeatFrequencyMS, 10000 },    %  delay between Topology rescans
    { minHeartbeatFrequencyMS, 1000 },

    { rp_mode, primary }, % default ReadPreference mode - primary, secondary, primaryPreferred, secondaryPreferred, nearest

    { rp_tags, [{tag,1}] }, % tags that servers shoul be tagged by for becoming candidates for server selection  (may be an empty list)
]
```

mongoc **WorkerOptions** (as described in mongo Connecting chapter)

```erlang
-type arg() :: {database, database()}
| {login, binary()}
| {password, binary()}
| {w_mode, write_mode()}.
```


### More Documentation

[API Docs](http://api.mongodb.org/erlang/mongodb/) - Documentation generated from source code comments
