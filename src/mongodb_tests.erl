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
	 fun connect_test/0,
	 fun mongo_test/0
	]}).

% This test must be run first right after application start (assumes vars contain initial value)
app_test() ->
	1 = mongodb_app:next_requestid(),
	UnixSecs = bson:unixtime_to_secs (bson:timenow()),
	{<<T3, T2, _:80>>} = bson:objectid (UnixSecs, <<1,2,3,4,5>>, 0),
	{<<T3, T2, _:56, 1:24/big>>} = mongodb_app:gen_objectid(). % high two timestamp bytes should match

% Mongod server must be running on 127.0.0.1:27017
connect_test() ->
	{ok, Conn} = mongo_connect:connect ({"127.0.0.1", 27017}),
	DbConn = {test, Conn},
	Res = mongo_query:write (DbConn, #delete {collection = foo, selector = {}}, {}),
	{null} = bson:lookup (err, Res),
	Doc0 = {'_id', 0, text, <<"hello">>},
	Doc1 = {'_id', 1, text, <<"world">>},
	Res1 = mongo_query:write (DbConn, #insert {collection = foo, documents = [Doc0, Doc1]}, {}),
	{null} = bson:lookup (err, Res1),
	ok = mongo_query:write (DbConn, #update {collection = foo, selector = {'_id', 1}, updater = {'$set', {text, <<"world!!">>}}}),
	Doc1X = bson:update (text, <<"world!!">>, Doc1),
	Cursor = mongo_query:find (DbConn, #'query' {collection = foo, selector = {}}),
	[Doc0, Doc1X] = mongo_cursor:rest (Cursor),
	mongo_connect:close (Conn).

% Mongod server must be running on 127.0.0.1:27017
mongo_test() ->
	{ok, Conn} = mongo:connect ("127.0.0.1"),
	mongo:do (safe, master, Conn, baseball, fun () ->
		mongo:delete (team, {}),
		Teams0 = [
			{name, <<"Yankees">>, home, {city, <<"New York">>, state, <<"NY">>}, league, <<"American">>},
			{name, <<"Mets">>, home, {city, <<"New York">>, state, <<"NY">>}, league, <<"National">>},
			{name, <<"Phillies">>, home, {city, <<"Philadelphia">>, state, <<"PA">>}, league, <<"National">>},
			{name, <<"Red Sox">>, home, {city, <<"Boston">>, state, <<"MA">>}, league, <<"American">>} ],
		Ids = mongo:insert_all (team, Teams0),
		4 = mongo:count (team, {}),
		Teams = lists:zipwith (fun (Id, Team) -> bson:append ({'_id', Id}, Team) end, Ids, Teams0),
		Teams = mongo:rest (mongo:find (team, {})),
		NationalTeams = lists:filter (fun (Team) -> bson:lookup (league, Team) == {<<"National">>} end, Teams),
		NationalTeams = mongo:rest (mongo:find (team, {league, <<"National">>})),
		TeamNames = lists:map (fun (Team) -> {name, bson:at (name, Team)} end, Teams),
		TeamNames = mongo:rest (mongo:find (team, {}, {'_id', 0, name, 1})),
		BostonTeam = lists:last (Teams),
		{BostonTeam} = mongo:find_one (team, {home, {city, <<"Boston">>, state, <<"MA">>}})
	end),
	mongo:disconnect (Conn).
