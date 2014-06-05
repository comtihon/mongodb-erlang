This is the MongoDB driver for Erlang. [MongoDB](http://www.mongodb.org) is a document-oriented database management system. A driver is a client library that provides an API for connecting to MongoDB servers, performing queries and updates on those servers, and performing administrative tasks like creating indexes and viewing statistics.

This version of the driver supports connecting to a single server or replica set, and pooling of both types of connections. Both connection types and pools are thread-safe, i.e. multiple processes can use the same connection/pool simultaneously without interfering with each other.

This driver is implemented as an Erlang application named *mongodb*. It depends on another Erlang library application named [*bson*](http://github.com/mongodb/bson-erlang), which defines the document type and its standard binary representation. You need both of these. Below we describe the mongodb application; you should also see the bson application to understand the document type.

### Installing

Download and compile the application

	$ git clone git://github.com/mongodb/mongodb-erlang.git mongodb
	$ cd mongodb
	$ make

### Starting

The bson application needs to be started before starting mongodb application

    > application:start (bson).

The mongodb application needs be started before using (to initialize an internal ets table of counters)

	> application:start (mongodb).

Although the mongodb application includes several modules, you should only need to use *mongo*, which is the top-level interface for the driver. Likewise, you should only need to use the *bson* module in the bson application.

### Connecting

To connect to a database `test` on mongodb server listening on `localhost:27017` (or any address & port of your choosing) use `mongo:connect/3/4/6`.

	> Host = localhost.
	> Port = 27017.
	> Database = test.
	> {ok, Connection} = mongo:connect (Host, Port, Database).

`mongo:connect` returns `{error, Reason}` if it failed to connect.  

`mongo:connect/4` also takes extra options as a param. It is `timeout` - time to wait for connection to database.  

`mongo:connect/6` also adds read and write modes config. They are `master` and `slave_ok` for read and `unsafe`, `safe` and `{safe, bson:document()}` for write.  

`safe`, along with `{safe, GetLastErrorParams}` and `unsafe`, are write-modes. Safe mode makes a *getLastError* request after every write in the sequence. If the reply says it failed then the rest of the sequence is aborted and `mongo:do` returns `{failure, {write_failure, Reason}}`, or `{failure, not_master}` when connected to a slave. An example write failure is attempting to insert a duplicate key that is indexed to be unique. Alternatively, unsafe mode issues every write without a confirmation, so if a write fails you won't know about it and remaining operations will be executed. This is unsafe but faster because you there is no round-trip delay.

`master`, along with `slave_ok`, are read-modes. `master` means every query in the sequence must read fresh data (from a master/primary server). If the connected server is not a master then the first read will fail, the remaining operations will be aborted, and `mongo:do` will return `{failure, not_master}`. `slave_ok` means every query is allowed to read stale data from a slave/secondary (fresh data from a master is fine too).


### Writing

After you connected to your database - you can carry out write operations, such as `insert`, `update` and `delete`:
    
    > mongo:insert(Connection, Collection, [
    		{name, <<"Yankees">>, home, {city, <<"New York">>, state, <<"NY">>}, league, <<"American">>},
    		{name, <<"Mets">>, home, {city, <<"New York">>, state, <<"NY">>}, league, <<"National">>},
    		{name, <<"Phillies">>, home, {city, <<"Philadelphia">>, state, <<"PA">>}, league, <<"National">>},
    		{name, <<"Red Sox">>, home, {city, <<"Boston">>, state, <<"MA">>}, league, <<"American">>}
    	])
An insert example (from `mongo_SUITE` test module). `Connection` is your Connection, got `from mongo:connect`, `Collection` is your collection name, `Doc` is something, you want to save.  
Doc will be returned, if insert succeeded. If Doc doesn't contains `_id` field - an updated Doc will be returned - with automatically generated '_id' fields. If error occurred - Connection will fall.  

    > mongo:delete(Connection, Collection, Selector).
Delete example. `Connection` is your Connection, `Collection` - is a collection you want to clean. `Selector` is the rules for cleaning. If you want to clean everything - pass empty `{}`.  

### Reading

To call read operations use `find`, `find_one`:

    > Cursor = mongo:find(Connection, Collection, Selector)
All params similar to `delete`.  
The difference between `find` and `find_one` is in return. Find_one just returns your result, while find returns you a `Cursor` - special process' pid. You can query data through the process with the help of `mc_cursor' module.  

    > Result = mc_cursor:rest(Cursor),
    > mc_cursor:close(Cursor),
__Important!__ Do not forget to close cursors after using them!

### Administering

This driver does not provide helper functions for commands. Use `mongo:command` directly and refer to the [MongoDB documentation](http://www.mongodb.org/display/DOCS/Commands) for how to issue raw commands.

### Authentication

To authenticate use function `mongo:auth`.

Plain Erlang string is interpreted as a BSON array of integers, so **make sure** to always encode your strings, as in `<<"hello">>` or `bson:utf8("hello")`.

### Pooling

For pooling use [Poolboy](https://github.com/devinus/poolboy) with mc_worker as pool workers. It is just a client, so pool realisation should not be here.

### More Documentation

[API Docs](http://api.mongodb.org/erlang/mongodb/) - Documentation generated from source code comments