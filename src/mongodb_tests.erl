%@doc Unit tests.
% For test to work, a mongodb server must be listening on 127.0.0.1:27017.
% For test_rs to work, a mongodb replica set named "rs1" must be listening on 127.0.0.1:27017 and 127.0.0.1:27018.
-module(mongodb_tests).

-include_lib("eunit/include/eunit.hrl").
-include ("mongo_protocol.hrl").

-export ([test/0, test_rs/0]).

test() -> eunit:test ({setup,
	fun () -> application:start (mongodb),
		io:format (user, "~n** Make sure mongod is running on 127.0.0.1:27017 **~n~n", []) end,
	fun (_) -> application:stop (mongodb) end,
	[fun var_test/0,
	 fun var_finalize_test/0,
	 fun app_test/0,
	 fun connect_test/0,
	 fun mongo_test/0,
	 fun resource_pool_test/0
	]}).

test_rs() -> eunit:test ({setup,
	fun () -> application:start (mongodb),
		io:format (user, "~n** Make sure replica set is running on 127.0.0.1:27017 & 27018 **~n~n", []) end,
	fun (_) -> application:stop (mongodb) end,
	[fun replset_test/0,
	 fun mongo_rs_test/0
	]}).

var_test() ->
	Var = mvar:new (0),
	0 = mvar:write (Var, 1),
	1 = mvar:read (Var),
	foo = mvar:modify (Var, fun (N) -> {N+1, foo} end),
	2 = mvar:read (Var),
	foo = (catch mvar:with (Var, fun (_) -> throw (foo) end)),
	mvar:terminate (Var),
	{exit, {noproc, _}} = try mvar:read (Var) catch C:E -> {C, E} end,
	mvar:terminate (Var). % repeat termination is no-op (not failure)

var_finalize_test() ->
	Var0 = mvar:new ({}),
	Var = mvar:new (0, fun (N) -> mvar:write (Var0, N) end),
	{} = mvar:read (Var0),
	0 = mvar:read (Var),
	mvar:terminate (Var),
	0 = mvar:read (Var0),
	mvar:terminate (Var0).

% This test must be run first right after application start (assumes counter table contain initial values)
app_test() ->
	1 = mongodb_app:next_requestid(),
	UnixSecs = bson:unixtime_to_secs (bson:timenow()),
	{<<T3, T2, _:80>>} = bson:objectid (UnixSecs, <<1,2,3,4,5>>, 0),
	{<<T3, T2, _:56, 1:24/big>>} = mongodb_app:gen_objectid(). % high two timestamp bytes should match

% Mongod server must be running on 127.0.0.1:27017
connect_test() ->
	{error, _} = mongo_connect:connect ({"127.0.0.1", 26555}),
	{ok, Conn} = mongo_connect:connect ({"127.0.0.1", 27017}),
	DbConn = {test, Conn},
	Res = mongo_query:write (DbConn, #delete {collection = foo, selector = {}}, {}),
	{undefined} = bson:lookup (err, Res),
	Doc0 = {'_id', 0, text, <<"hello">>},
	Doc1 = {'_id', 1, text, <<"world">>},
	Res1 = mongo_query:write (DbConn, #insert {collection = foo, documents = [Doc0, Doc1]}, {}),
	{undefined} = bson:lookup (err, Res1),
	ok = mongo_query:write (DbConn, #update {collection = foo, selector = {'_id', 1}, updater = {'$set', {text, <<"world!!">>}}}),
	Doc1X = bson:update (text, <<"world!!">>, Doc1),
	Cursor = mongo_query:find (DbConn, #'query' {collection = foo, selector = {}}),
	[Doc0, Doc1X] = mongo_cursor:rest (Cursor),
	true = mongo_cursor:is_closed (Cursor),
	#reply {cursornotfound = true} = mongo_connect:call (DbConn, [], #getmore {collection = foo, cursorid = 2938725639}),
	mongo_connect:close (Conn),
	true = mongo_connect:is_closed (Conn).

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
		{BostonTeam} = mongo:find_one (team, {home, {city, <<"Boston">>, state, <<"MA">>}}),
		mongo:delete_one (team, {}),
		3 = mongo:count (team, {})
	end),
	mongo:disconnect (Conn).

% Mongod server must be running on 127.0.0.1:27017
resource_pool_test() ->
	Pool = resource_pool:new (mongo:connect_factory ({"127.0.0.1", 27017}), 2),
	Do = fun (Conn) -> mongo:do (safe, master, Conn, admin, fun () -> mongo:command ({listDatabases, 1}) end) end,
	lists:foreach (fun (_) ->
			{ok, Conn} = resource_pool:get (Pool),
			{ok, Doc} = Do (Conn),
			{_} = bson:lookup (databases, Doc) end,
		lists:seq (1,8)),
	resource_pool:close (Pool),
	true = resource_pool:is_closed (Pool).

% Replica set named "rs1" must be running on localhost:27017 & 27018
replset_test() -> % TODO: change from connect_test
	RS0 = mongo_replset:connect ({<<"rs0">>,[localhost]}),
	{error, [{not_member, _, _, _} | _]} = mongo_replset:primary (RS0),
	mongo_replset:close (RS0),
	RS1 = mongo_replset:connect ({<<"rs1">>,[localhost]}),
	{ok, Conn} = mongo_replset:primary (RS1),
	DbConn = {test, Conn},
	Res = mongo_query:write (DbConn, #delete {collection = foo, selector = {}}, {}),
	{undefined} = bson:lookup (err, Res),
	Doc0 = {'_id', 0, text, <<"hello">>},
	Doc1 = {'_id', 1, text, <<"world">>},
	Res1 = mongo_query:write (DbConn, #insert {collection = foo, documents = [Doc0, Doc1]}, {}),
	{undefined} = bson:lookup (err, Res1),
	ok = mongo_query:write (DbConn, #update {collection = foo, selector = {'_id', 1}, updater = {'$set', {text, <<"world!!">>}}}),
	Doc1X = bson:update (text, <<"world!!">>, Doc1),
	Cursor = mongo_query:find (DbConn, #'query' {collection = foo, selector = {}}),
	[Doc0, Doc1X] = mongo_cursor:rest (Cursor),
	{ok, Conn2} = mongo_replset:secondary_ok (RS1),
	DbConn2 = {test, Conn2},
	Cursor2 = mongo_query:find (DbConn2, #'query' {collection = foo, selector = {}, slaveok = true}),
	[Doc0, Doc1X] = mongo_cursor:rest (Cursor2),
	mongo_replset:close (RS1),
	true = mongo_replset:is_closed (RS1).

% Replica set named "rs1" must be running on localhost:27017 & 27018
mongo_rs_test() ->
	RsConn = mongo:rs_connect ({<<"rs1">>,["127.0.0.1"]}),
	{ok, {Teams1, Ids1}} = mongo:do (safe, master, RsConn, baseball, fun () ->
		mongo:delete (team, {}),
		Teams0 = [
			{name, <<"Yankees">>, home, {city, <<"New York">>, state, <<"NY">>}, league, <<"American">>},
			{name, <<"Mets">>, home, {city, <<"New York">>, state, <<"NY">>}, league, <<"National">>},
			{name, <<"Phillies">>, home, {city, <<"Philadelphia">>, state, <<"PA">>}, league, <<"National">>},
			{name, <<"Red Sox">>, home, {city, <<"Boston">>, state, <<"MA">>}, league, <<"American">>} ],
		Ids0 = mongo:insert_all (team, Teams0),
		{Teams0, Ids0}
	end),
	timer:sleep (200),
	mongo:do (safe, slave_ok, RsConn, baseball, fun () ->
		4 = mongo:count (team, {}),
		Teams = lists:zipwith (fun (Id, Team) -> bson:append ({'_id', Id}, Team) end, Ids1, Teams1),
		Teams = mongo:rest (mongo:find (team, {})),
		NationalTeams = lists:filter (fun (Team) -> bson:lookup (league, Team) == {<<"National">>} end, Teams),
		NationalTeams = mongo:rest (mongo:find (team, {league, <<"National">>})),
		TeamNames = lists:map (fun (Team) -> {name, bson:at (name, Team)} end, Teams),
		TeamNames = mongo:rest (mongo:find (team, {}, {'_id', 0, name, 1})),
		BostonTeam = lists:last (Teams),
		{BostonTeam} = mongo:find_one (team, {home, {city, <<"Boston">>, state, <<"MA">>}})
	end),
	mongo:rs_disconnect (RsConn).
