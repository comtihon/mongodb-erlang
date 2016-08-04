-module(mc_super_sup).
-behaviour(supervisor).

-export([start_link/0]).

-export([
	init/1
]).

-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).

-spec start_link() -> {ok, pid()}.
start_link() ->
	supervisor:start_link({local, ?MODULE}, ?MODULE, app).

%% @hidden
init(app) ->
	MongoIdServer = ?CHILD(mongo_id_server, worker),
	{ok, {{one_for_one, 1000, 3600}, [MongoIdServer]}}.