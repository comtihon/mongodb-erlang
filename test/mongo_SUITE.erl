-module(mongo_SUITE).

-include_lib("common_test/include/ct.hrl").
-include("mongo_protocol.hrl").
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
	{ok, Connection} = mongo:connect("127.0.0.1", 27017, ?config(database, Config), safe, master, []),
	[{connection, Connection}, {collection, collection(Case)} | Config].

end_per_testcase(_Case, Config) ->
	Connection = ?config(connection, Config),
	Collection = ?config(collection, Config),
	mongo:delete(Connection, Collection, {}).

%% Tests
insert_and_find(Config) ->
	Connection = ?config(connection, Config),
	Collection = ?config(collection, Config),

	Teams = mongo:insert(Connection, Collection, [
		{name, <<"Yankees">>, home, {city, <<"New York">>, state, <<"NY">>}, league, <<"American">>},
		{name, <<"Mets">>, home, {city, <<"New York">>, state, <<"NY">>}, league, <<"National">>},
		{name, <<"Phillies">>, home, {city, <<"Philadelphia">>, state, <<"PA">>}, league, <<"National">>},
		{name, <<"Red Sox">>, home, {city, <<"Boston">>, state, <<"MA">>}, league, <<"American">>}
	]),
	4 = mongo:count(Connection, Collection, {}),
	Teams = find(Collection, {}),

	NationalTeams = [Team || Team <- Teams, bson:at(league, Team) == <<"National">>],
	NationalTeams = find(Collection, {league, <<"National">>}),
	2 = mongo:count(Connection, Collection, {league, <<"National">>}),

	TeamNames = [bson:include([name], Team) || Team <- Teams],
	TeamNames = find(Collection, {}, {'_id', 0, name, 1}),


	BostonTeam = lists:last(Teams),
	{BostonTeam} = mongo:find_one(Connection, Collection, {home, {city, <<"Boston">>, state, <<"MA">>}}).

insert_and_delete(Config) ->
	Connection = ?config(connection, Config),
	Collection = ?config(collection, Config),

	mongo:insert(Connection, Collection, [
		{name, <<"Yankees">>, home, {city, <<"New York">>, state, <<"NY">>}, league, <<"American">>},
		{name, <<"Mets">>, home, {city, <<"New York">>, state, <<"NY">>}, league, <<"National">>},
		{name, <<"Phillies">>, home, {city, <<"Philadelphia">>, state, <<"PA">>}, league, <<"National">>},
		{name, <<"Red Sox">>, home, {city, <<"Boston">>, state, <<"MA">>}, league, <<"American">>}
	]),
	4 = mongo:count(Connection, Collection, {}),

	mongo:delete_one(Connection, Collection, {}),
	3 = mongo:count(Connection, Collection, {}).


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

