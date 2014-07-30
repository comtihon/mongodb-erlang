-module(mongo_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-include("mongo_protocol.hrl").


-export([
	all/0,
	init_per_suite/1,
	end_per_suite/1,
	init_per_testcase/2,
	end_per_testcase/2,
	search_and_query/1]).

-export([
	insert_and_find/1,
	insert_and_delete/1
]).

all() ->
	[
		insert_and_find,
		insert_and_delete,
		search_and_query
	].

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
	Teams2 = find(Connection, Collection, {}),
	true = match_bson(Teams, Teams2),

	NationalTeams = [Team || Team <- Teams, bson:at(league, Team) == <<"National">>],
	NationalTeams2 = find(Connection, Collection, {league, <<"National">>}),
	true = match_bson(NationalTeams, NationalTeams2),

	2 = mongo:count(Connection, Collection, {league, <<"National">>}),

	TeamNames = [bson:include([name], Team) || Team <- Teams],
	TeamNames = find(Connection, Collection, {}, {'_id', 0, name, 1}),


	BostonTeam = lists:last(Teams),
	{BostonTeam2} = mongo:find_one(Connection, Collection, {home, {city, <<"Boston">>, state, <<"MA">>}}),
	true = match_bson([BostonTeam], [BostonTeam2]).

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

search_and_query(Config) ->
	Connection = ?config(connection, Config),
	Collection = ?config(collection, Config),

	%insert test data
	mongo:insert(Connection, Collection, [
		{name, <<"Yankees">>, home, {city, <<"New York">>, state, <<"NY">>}, league, <<"American">>},
		{name, <<"Mets">>, home, {city, <<"New York">>, state, <<"NY">>}, league, <<"National">>},
		{name, <<"Phillies">>, home, {city, <<"Philadelphia">>, state, <<"PA">>}, league, <<"National">>},
		{name, <<"Red Sox">>, home, {city, <<"Boston">>, state, <<"MA">>}, league, <<"American">>}
	]),

	%test selector
	Res = find(Connection, Collection, {home, {city, <<"New York">>, state, <<"NY">>}}),
	?debugFmt("Got ~p", [Res]),
	[{'_id', _,
		name, <<"Yankees">>, home,
		{city, <<"New York">>, state, <<"NY">>},
		league, <<"American">>},
		{'_id', _,
			name, <<"Mets">>, home,
			{city, <<"New York">>, state, <<"NY">>},
			league, <<"National">>}] = Res,

	%test projector
	Res2 = find(Connection, Collection, {home, {city, <<"New York">>, state, <<"NY">>}}, {name, true, league, true}),
	?debugFmt("Got ~p", [Res2]),
	[{'_id', _,
		name, <<"Yankees">>,
		league, <<"American">>},
		{'_id', _,
			name, <<"Mets">>,
			league, <<"National">>}] = Res2,

	Config.


%% @private
find(Connection, Collection, Selector) ->
	find(Connection, Collection, Selector, []).

find(Connection, Collection, Selector, Projector) ->
	Cursor = mongo:find(Connection, Collection, Selector, Projector),
	Result = mc_cursor:rest(Cursor),
	mc_cursor:close(Cursor),
	Result.

%% @private
collection(Case) ->
	Now = now_to_seconds(erlang:now()),
	<<(atom_to_binary(?MODULE, utf8))/binary, $-,
	(atom_to_binary(Case, utf8))/binary, $-,
	(list_to_binary(integer_to_list(Now)))/binary>>.

%% @private
now_to_seconds({Mega, Sec, _}) ->
	(Mega * 1000000) + Sec.

match_bson(Tuple1, Tuple2) when length(Tuple1) /= length(Tuple2) -> false;
match_bson(Tuple1, Tuple2) ->
	try
		lists:foldr(
			fun(Elem, Num) ->
				Elem2 = lists:nth(Num, Tuple2),
				Sorted = lists:sort(bson:fields(Elem)),
				Sorted = lists:sort(bson:fields(Elem2))
			end, 1, Tuple1)
	catch
		_:_ -> false
	end,
	true.