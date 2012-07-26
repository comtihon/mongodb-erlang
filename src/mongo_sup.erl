-module(mongo_sup).
-export([
	start_link/0,
	start_cursor/6
]).

-behaviour(supervisor).
-export ([
	init/1
]).

-define(WORKER(M),        {M, {M, start_link, []}, permanent, 5000, worker, [M]}).
-define(SUPERVISOR(M, T), {M, {supervisor, start_link, [{local, M}, ?MODULE, T]}, permanent, infinity, supervisor, [M]}).


-spec start_link() -> {ok, pid()}.
start_link() ->
	supervisor:start_link({local, ?MODULE}, ?MODULE, app).

-spec start_cursor(mongo_connection:connection(), atom(), atom(), integer(), integer(), [bson:document()]) -> pid().
start_cursor(Connection, Database, Collection, Cursor, BatchSize, Batch) ->
	supervisor:start_child(mongo_cursors_sup, [Connection, Database, Collection, Cursor, BatchSize, Batch]).


%% @hidden
init(app) ->
	{ok, {
		{one_for_one, 5, 10}, [
			{mongo_id_server, {mongo_id_server, start_link, []}, temporary, 5000, worker, [mongo_id_server]},
			?SUPERVISOR(mongo_cursors_sup, cursors)
		]
	}};

init(cursors) ->
	{ok, {
		{simple_one_for_one, 5, 10}, [
			{undefined, {mongo_cursor, start_link, []}, temporary, 5000, worker, [mongo_cursor]}
		]
	}}.
