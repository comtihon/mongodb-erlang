%%%-------------------------------------------------------------------
%%% @author tihon
%%% @copyright (C) 2014, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 28. май 2014 17:21
%%%-------------------------------------------------------------------
-module(mc_sup).
-author("tihon").

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the supervisor
%%
%% @end
%%--------------------------------------------------------------------
-spec(start_link() ->
	{ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
	supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Example:
%% {
%%    db_pool, % pool for working with database
%%    [
%%      [
%%        {size, 20},
%%        {max_overflow, 10}
%%      ],
%%      [
%%        {host, "127.0.0.1"},
%%        {port, 5432},
%%        {user, "user"},
%%        {password, "password"},
%%        {database, "database"},
%%      ]
%%    ]
%% }
%%  see https://github.com/devinus/poolboy poolboy.app for more complete example
%% @end
%%--------------------------------------------------------------------
-spec(init(Conf :: term()) ->
	{ok, {SupFlags :: {RestartStrategy :: supervisor:strategy(),
		MaxR :: non_neg_integer(), MaxT :: non_neg_integer()},
		[ChildSpec :: supervisor:child_spec()]
	}} |
	ignore |
	{error, Reason :: term()}).
init({Name, [SizeArgs, WorkerArgs]}) ->
	PoolArgs = [{name, {local, Name}}, {worker_module, mc_worker}] ++ SizeArgs,
	PoolSpecs = [poolboy:child_spec(Name, PoolArgs, WorkerArgs)],
	{ok, {{one_for_one, 10, 10}, PoolSpecs}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================