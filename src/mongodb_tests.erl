% unit tests. For tests to work, a mongodb server must be listening on 127.0.0.1:27017.
-module(mongodb_tests).

-include_lib("eunit/include/eunit.hrl").
-include ("mongo_protocol.hrl").

-export ([test/0]).

test() -> eunit:test ({setup,
	fun () -> application:start (mongodb),
		io:format (user, "~n** Make sure mongod is running on 127.0.0.1:27017 **~n~n", []) end,
	fun (_) -> application:stop (mongodb) end,
	[fun app_test/0,
	 fun connect_test/0
	]}).

% This test must be run first right after application start (assumes vars contain initial value)
app_test() ->
	0 = mongodb_app:next_requestid(),
	{unixtime, Ms} = bson:timenow(),
	{oid, <<T3, T2, _:80>>} = bson:objectid (Ms div 1000, <<1,2,3,4,5>>, 0),
	{oid, <<T3, T2, _:56, 0:24>>} = mongodb_app:gen_objectid(). % high two timestamp bytes should match

connect_test() ->
	Conn = mongo_connect:connect ("127.0.0.1", 27017),
	DbConn = {test, Conn},
	Doc = mongo_query:write (DbConn, #delete {collection = foo, selector = []}, []),
	{null} = bson:lookup (err, Doc),
	Docs = [['_id', 0, text, <<"hello">>], ['_id', 1, text, <<"world">>]],
	Doc1 = mongo_query:write (DbConn, #insert {collection = foo, documents = Docs}, []),
	{null} = bson:lookup (err, Doc1),
	Cursor = mongo_query:find (DbConn, #'query' {collection = foo, selector = []}),
	Docs = mongo_cursor:rest (Cursor).
