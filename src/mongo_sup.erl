-module(mongo_sup).
-behaviour(supervisor).

-export([
	start_link/0,
	start_cursor/1
]).

-export([
	init/1
]).

-define(SUPERVISOR(Id, Tag), {Id, {supervisor, start_link, [?MODULE, Tag]}, permanent, infinity, supervisor, [?MODULE]}).
-define(SUPERVISOR(Id, Name, Tag), {Id, {supervisor, start_link, [{local, Name}, ?MODULE, Tag]}, permanent, infinity, supervisor, [?MODULE]}).
-define(WORKER(M, A, R), {M, {M, start_link, A}, R, 5000, worker, [M]}).

-spec start_link() -> {ok, pid()}.
start_link() ->
	supervisor:start_link({local, ?MODULE}, ?MODULE, app).

-spec start_cursor([term()]) -> {ok, pid()}.
start_cursor(Args) ->
	supervisor:start_child(?MODULE, [Args]).

%% @hidden
init(app) ->
	MongoIdServer = ?WORKER(mongo_id_server, [], permanent),
	{ok, {{one_for_one, 5, 10}, [MongoIdServer]}};
init(cursors) ->
	{ok, {
		{simple_one_for_one, 5, 10}, [
			?WORKER(mongo_cursor, [], temporary)
		]
	}};
init({connections, Service, Options}) ->
	{ok, {
		{simple_one_for_one, 5, 10}, [
			?WORKER(mongo_connection, [Service, Options], temporary)
		]
	}}.