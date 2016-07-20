%%%-------------------------------------------------------------------
%%% @author tihon
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 20. Jul 2016 18:12
%%%-------------------------------------------------------------------
-module(mc_seed_logics).
-author("tihon").

%% API
-export([init_pool/3]).


-spec init_pool(string(), proplists:proplist(), proplists:proplist()) -> {ok, pid()}.
init_pool(Addr, TopologyOptions, WorkerOpts) ->
  {Host, Port} = parse_seed(Addr),
  PoolConf = form_pool_conf(TopologyOptions),
  WO = lists:append([{host, Host}, {port, Port}], WorkerOpts),
  mc_pool_sup:start_pool(PoolConf, WO).

%% @private
parse_seed(Addr) when is_binary(Addr) ->
  parse_seed(binary_to_list(Addr));
parse_seed(Addr) when is_list(Addr) ->
  [Host, Port] = string:tokens(Addr, ":"),
  {Host, list_to_integer(Port)}.

%% @private
form_pool_conf(TopologyOptions) ->
  Size = mc_utils:get_value(pool_size, TopologyOptions, 10),
  Overflow = mc_utils:get_value(max_overflow, TopologyOptions, 10),
  OverflowTtl = mc_utils:get_value(overflow_ttl, TopologyOptions, 0),
  OverflowCheckPeriod = mc_utils:get_value(overflow_check_period, TopologyOptions),
  [{size, Size}, {max_overflow, Overflow}, {overflow_ttl, OverflowTtl}, {overflow_check_period, OverflowCheckPeriod}].