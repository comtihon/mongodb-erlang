This is the MongoDB driver for Erlang. [MongoDB](http://www.mongodb.org) is a document-oriented database management system. A driver is a client library that provides an API for connecting to MongoDB servers, performing queries and updates on those servers, and performing administrative tasks like creating indexes and viewing performance statistics.

This first version of the driver only supports individual connections to single servers, although multiple processes can use the same connection simultaneously without interfering with each other. This version does not support connection pooling and smart replica-set connection, which will be included in the next version coming out in a couple of weeks.

This driver is implemented as an Erlang application named *mongodb*. It depends on another Erlang library application named [*bson*](http://github.com/TonyGen/bson-erlang), which defines the document type and its standard binary representation. You need to download both of these applications. Below we describe the mongodb application; you will also need to read the bson documentation to understand the document type.

Once downloaded, compile each application

	$ cd bson
	$ erlc -o ebin -I include src/*.erl
	$ cd ../mongodb
	$ erlc -o ebin -I include -I .. src/*.erl
	$ cd ..

Then install them in your standard Erlang library location or include them in your path on startup

	$ erl -pa bson/ebin mongodb/ebin

The mongodb application needs be started before using (to initialize an internal ets table of counters)

	> application:start (mongodb).

Although the mongodb application includes several modules, you should only need to use *mongo*, which is the top-level interface for the driver. Likewise, you should only need to use the *bson* module in the bson application.

Start a mongodb server listening on `localhost:27017` (or any address & port of your choosing), then connect to it

	> {ok, Conn} = mongo:connect ({localhost, 27017}).

`27017` is the default port so you could elide it in this case and just supply `localhost` as the argument. `mongo:connect` will return `{error, Reason}` if it failed to connect.

A database operation happens with respect to a connection, database, read-mode, and write-mode. These four parameters are supplied once at the beginning of a sequence of read/write operations. Furthermore, if one of the operations fails no further operations in the sequence are executed and an error is returned for the entire sequence.

	> mongo:do (safe, master, Conn, test, fun() ->
		mongo:delete (foo, []),
		mongo:insert (foo, [x,1]),
		mongo:find (foo, [x,1]) end).

`safe` is a write-mode; `{safe, GetLastErrorParams}` and `unsafe` are the other write-modes. Safe mode makes a *getLastError* request after every write in the sequence, and if the reply says it failed then the rest of the sequence is aborted and `mongo:do` returns `{error, {write_failure, Reason}}`. Some possible write failures are: attempting to write to a slave server (connected server must be a master), and attempting to insert a duplicate key that is indexed to be unique. Unsafe mode issues every write without a confirmation, so if a write fails (even if the connection fails) you won't know about it and remaining operations will be executed. This is unsafe but faster because you there is no round-trip delay.

`master` is a read-mode; `slave_ok` is the other read-mode. Master means every query in the sequence must read fresh data (from a master/primary server). If the connected server is not a master then the first read will fail, the remaining operations will be aborted, and `mongo:do` will return {error, not_master} [1]. Slave-ok means every query is allowed to read stale data from a slave (master is fine too).

`Conn` is the connection we send the operations to. If it fails during one of the operations then the remaining operations are aborted and `{error, {connection_failure, Reason}}` is returned.

`test` is the name of the database in this example. All collections accessed (`foo` in this example) are taken from the database given in the fourth argument. If a collection is missing from a database it will be automatically created upon first access.

If there are no errors in the sequence of operations then the result of the last operation is returned as the result of the entire `mongo:do` command. It is wrapped in an *ok* tuple as in `{ok, Result}` to distinguish it from an error.

`mongo:find` returns a *cursor* holding the pending list of results. They are accessed using `mongo:next` to get the next result, and `mongo:rest` to get the remaining results. `mongo:rest` also closes the cursor, otherwise you should close the cursor when finished using `mongo:close_cursor`.

Finally, you should close the connection when finished using `mongo:disconnect`.

See the [*mongo* module](http://github.com/TonyGen/mongodb-erlang/blob/master/src/mongo.erl) for details on each action such as `update`, `find` and `command`. A type specification is provided with each function so you know the expected arguments and results. The spec line also has a comment if it performs a side-effect such as IO and what exceptions it may throw. No comment means it is a pure function. Also, see the [*bson* module](http://github.com/TonyGen/bson-erlang/blob/master/src/bson.erl) in the bson application for details on the document type and value types.

This driver does not provide helper functions for commands. Use `mongo:command` directly and refer to the [MongoDB documentation](http://www.mongodb.org/display/DOCS/Commands) for how to issue raw commands. A future version will probably include helper functions for the most common commands. In particular, `ensure_index` will be added in the next minor revision because it doesn't even use a command but requires updating a meta-collection directly. In the meantime, you should create indexes from the mongo (javascript) shell.

[1] Currently not-master exception will raise an erlang:error instead of being returned as an error from `mongo:do`. This is a bug that will be fixed in the next minor revision expected in a few days.
