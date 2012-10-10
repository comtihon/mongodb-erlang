-module(mongo_SUITE).

-include_lib("common_test/include/ct.hrl").
-export([
	all/0,
	init_per_suite/1,
	end_per_suite/1,
	init_per_testcase/2,
	end_per_testcase/2
]).


-export([
	insert_and_find/1,
	insert_and_delete/1
]).


all() ->
	[insert_and_find, insert_and_delete].

init_per_suite(Config) ->
	application:start(bson),
	application:start(mongodb),
	[{database, test} | Config].

end_per_suite(_Config) ->
	application:stop(mongodb),
	application:stop(bson),
	ok.

init_per_testcase(Case, Config) ->
	{ok, Connection} = mongo_connection:start_link({"127.0.0.1", 27017}, []),
	[{connection, Connection}, {collection, collection(Case)} | Config].

end_per_testcase(_Case, Config) ->
	Connection = ?config(connection, Config),
	Database   = ?config(database, Config),
	Collection = ?config(collection, Config),
	ok = mongo:do(safe, master, Connection, Database, fun() ->
		mongo:delete(Collection, {})
	end).

%% Tests
insert_and_find(Config) ->
	Connection = ?config(connection, Config),
	Database   = ?config(database, Config),
	Collection = ?config(collection, Config),
	mongo:do(safe, master, Connection, Database, fun () ->
		Teams = mongo:insert(Collection, [
			{name, <<"Yankees">>, home, {city, <<"New York">>, state, <<"NY">>}, league, <<"American">>},
			{name, <<"Mets">>, home, {city, <<"New York">>, state, <<"NY">>}, league, <<"National">>},
			{name, <<"Phillies">>, home, {city, <<"Philadelphia">>, state, <<"PA">>}, league, <<"National">>},
			{name, <<"Red Sox">>, home, {city, <<"Boston">>, state, <<"MA">>}, league, <<"American">>}
		]),
		4 = mongo:count(Collection, {}),
		Teams = find(Collection, {}),

		NationalTeams = [Team || Team <- Teams, bson:at(league, Team) == <<"National">>],
		NationalTeams = find(Collection, {league, <<"National">>}),
		2 = mongo:count(Collection, {league, <<"National">>}),

		TeamNames = [bson:include([name], Team) || Team <- Teams],
		TeamNames = find(Collection, {}, {'_id', 0, name, 1}),


		BostonTeam = lists:last(Teams),
		{BostonTeam} = mongo:find_one(Collection, {home, {city, <<"Boston">>, state, <<"MA">>}})
	end).

insert_and_delete(Config) ->
	Connection = ?config(connection, Config),
	Database   = ?config(database, Config),
	Collection = ?config(collection, Config),
	mongo:do(safe, master, Connection, Database, fun () ->
		_Teams = mongo:insert(Collection, [
			{name, <<"Yankees">>, home, {city, <<"New York">>, state, <<"NY">>}, league, <<"American">>},
			{name, <<"Mets">>, home, {city, <<"New York">>, state, <<"NY">>}, league, <<"National">>},
			{name, <<"Phillies">>, home, {city, <<"Philadelphia">>, state, <<"PA">>}, league, <<"National">>},
			{name, <<"Red Sox">>, home, {city, <<"Boston">>, state, <<"MA">>}, league, <<"American">>}
		]),
		4 = mongo:count(Collection, {}),

		mongo:delete_one(Collection, {}),
		3 = mongo:count(Collection, {})
	end).



%% @private
find(Collection, Selector) ->
	find(Collection, Selector, []).

find(Collection, Selector, Projector) ->
	Cursor = mongo:find(Collection, Selector, Projector),
	Result = mongo_cursor:rest(Cursor),
	mongo_cursor:close(Cursor),
	Result.

%% @private
collection(Case) ->
	Now = now_to_seconds(erlang:now()),
	list_to_atom(atom_to_list(?MODULE) ++ "-" ++ atom_to_list(Case) ++ "-" ++ integer_to_list(Now)).

%% @private
now_to_seconds({Mega, Sec, _}) ->
	(Mega * 1000000) + Sec.

