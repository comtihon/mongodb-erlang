%%%-------------------------------------------------------------------
%%% @author tihon
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 17. Nov 2015 12:47
%%%-------------------------------------------------------------------
-module(mc_pool_sup).
-author("tihon").

-behaviour(supervisor).

%% API
-export([start_link/0, start_pool/3, stop_pool/1, ensure_started/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).


%%%===================================================================
%%% API functions
%%%===================================================================
-spec start_pool(tuple(), proplists:proplist(), proplists:proplist()) -> {ok, pid()}.
start_pool({Host, Port}, TopologyOptions, WorkerOpts) ->
  PoolConf = form_pool_conf(TopologyOptions),
  WO = lists:append([{host, Host}, {port, Port}], WorkerOpts),
  PoolArgs = [{worker_module, mc_worker}] ++ PoolConf,
  supervisor:start_child(?MODULE, [PoolArgs, WO]).

stop_pool(Pid) when is_pid(Pid) ->
  poolboy:stop(Pid);
stop_pool(_) ->
  ok.

-spec ensure_started() -> ok | {error, term()}.
ensure_started() ->
  case start_link() of
    {ok, _} -> ok;
    {error, {already_started, _}} -> ok;
    {error, _} = Err -> Err
  end.

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

init([]) ->
  {ok, {{simple_one_for_one, 1000, 3600},
    [{worker_pool, {poolboy, start_link, []}, transient, 5000, worker, [poolboy]}]}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
form_pool_conf(TopologyOptions) ->
  Size = mc_utils:get_value(pool_size, TopologyOptions, 10),
  Overflow = mc_utils:get_value(max_overflow, TopologyOptions, 10),
  OverflowTtl = mc_utils:get_value(overflow_ttl, TopologyOptions, 0),
  OverflowCheckPeriod = mc_utils:get_value(overflow_check_period, TopologyOptions),
  [{size, Size}, {max_overflow, Overflow}, {overflow_ttl, OverflowTtl}, {overflow_check_period, OverflowCheckPeriod}].