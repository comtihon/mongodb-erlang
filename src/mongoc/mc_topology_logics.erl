%%%-------------------------------------------------------------------
%%% @author tihon
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 20. Jul 2016 15:15
%%%-------------------------------------------------------------------
-module(mc_topology_logics).
-author("tihon").

-include("mongoc.hrl").

-define(NON_SHARDED(ST),
  ST =:= standalone; ST =:= rsPrimary; ST =:= rsSecondary; ST =:= rsArbiter; ST =:= rsOther; ST =:= rsGhost).
-define(SEC_ARB_OTH(ST), ST =:= rsSecondary; ST =:= rsArbiter; ST =:= rsOther).
-define(STAL_MONGS(ST), ST =:= standalone; ST =:= mongos).
-define(UNKN_GHST(ST), ST =:= unknown; ST =:= rsGhost).
-define(NOT_MAX(E, M), E =/= undefined, M =/= undefined, E < M).

%% API
-export([update_topology_state/2, init_seeds/4, init_seeds/1]).

update_topology_state(#mc_server{type = SType, pid = Pid}, State = #topology_state{type = sharded}) when ?NON_SHARDED(SType) -> %% SHARDED
  exit(Pid, kill),
  State;
update_topology_state(_, State = #topology_state{type = sharded}) ->
  State;
update_topology_state(#mc_server{type = unknown}, State = #topology_state{type = unknown}) -> %% UNKNOWN
  State;
update_topology_state(#mc_server{type = rsGhost}, State = #topology_state{type = unknown}) ->
  State;
update_topology_state(#mc_server{type = standalone}, State = #topology_state{type = unknown, seeds = Seeds}) when length(Seeds) =< 1 ->
  State#topology_state{type = standalone};
update_topology_state(#mc_server{type = standalone, pid = Pid}, State = #topology_state{type = unknown}) ->
  exit(Pid, kill),
  State;
update_topology_state(#mc_server{type = mongos}, State = #topology_state{type = unknown}) ->
  State#topology_state{type = sharded};
update_topology_state(Server = #mc_server{type = SType, setName = SetName},
    State = #topology_state{type = unknown, setName = SetName}) when ?SEC_ARB_OTH(SType) ->
  update_topology_state(Server, State#topology_state{setName = undefined});
update_topology_state(
    #mc_server{type = SType, setName = SetName, hosts = Hosts, arbiters = Arbiters, passives = Passives, primary = Primary},
    State = #topology_state{type = unknown, setName = undefined, topology_opts = Topts, worker_opts = Wopts, servers = Tab})
  when ?SEC_ARB_OTH(SType) ->
  init_seeds(lists:flatten([Hosts, Arbiters, Passives]), Tab, Topts, Wopts),
  set_possible_primary(Tab, Primary),
  State#topology_state{type = checkIfHasPrimary(Tab), setName = SetName};
update_topology_state(#mc_server{type = SType, pid = Pid}, State = #topology_state{type = unknown}) when ?SEC_ARB_OTH(SType) ->
  exit(Pid, kill),
  State;
update_topology_state(#mc_server{type = SType, pid = Pid},
    State = #topology_state{type = replicaSetNoPrimary}) when ?STAL_MONGS(SType) ->  %% REPLICASETNOPRIMARY
  exit(Pid, kill),
  State;
update_topology_state(Server = #mc_server{type = SType, setName = SetName},
    State = #topology_state{type = replicaSetNoPrimary, setName = SetName}) when ?SEC_ARB_OTH(SType) ->
  update_topology_state(Server, State#topology_state{setName = undefined});
update_topology_state(
    #mc_server{type = SType, setName = SetName, hosts = Hosts, arbiters = Arbiters, passives = Passives, primary = Primary},
    State = #topology_state{type = replicaSetNoPrimary, setName = undefined, topology_opts = Topts, worker_opts = Wopts, servers = Tab})
  when ?SEC_ARB_OTH(SType) ->
  init_seeds(lists:flatten([Hosts, Arbiters, Passives]), Tab, Topts, Wopts),
  set_possible_primary(Tab, Primary),
  State#topology_state{setName = SetName};
update_topology_state(#mc_server{type = SType, pid = Pid}, State = #topology_state{type = replicaSetNoPrimary}) when ?SEC_ARB_OTH(SType) ->
  exit(Pid, kill),
  State;
update_topology_state(#mc_server{type = SType},
    State = #topology_state{type = replicaSetWithPrimary, servers = Tab}) when ?UNKN_GHST(SType) -> %% REPLICASETWITHPRIMARY
  State#topology_state{type = checkIfHasPrimary(Tab)};
update_topology_state(#mc_server{type = SType, pid = Pid},
    State = #topology_state{type = replicaSetWithPrimary, servers = Tab}) when ?STAL_MONGS(SType) ->
  exit(Pid, kill),
  State#topology_state{type = checkIfHasPrimary(Tab)};
update_topology_state(
    #mc_server{type = SType, setName = SetName, primary = Primary},
    State = #topology_state{type = replicaSetWithPrimary, setName = SetName, servers = Tab}) when ?SEC_ARB_OTH(SType) ->
  set_possible_primary(Tab, Primary),
  State#topology_state{type = checkIfHasPrimary(Tab)};
update_topology_state(
    #mc_server{type = SType, pid = Pid},
    State = #topology_state{type = replicaSetWithPrimary, servers = Tab}) when ?SEC_ARB_OTH(SType) ->
  exit(Pid, kill),
  State#topology_state{type = checkIfHasPrimary(Tab)};
update_topology_state(Server = #mc_server{type = rsPrimary, setName = SetName}, State = #topology_state{setName = SetName}) -> %% REPLICASETWITHPRIMARY
  update_topology_state(Server, State#topology_state{setName = undefined});
update_topology_state(#mc_server{type = rsPrimary, electionId = ElectionId, host = Host, pid = Pid},
    State = #topology_state{setName = undefined, maxElectionId = MaxElectionId, servers = Tab}) when ?NOT_MAX(ElectionId, MaxElectionId) ->
  ets:insert(Tab, #mc_server{pid = Pid, host = Host, type = unknown}),
  State#topology_state{type = checkIfHasPrimary(Tab)};
update_topology_state(
    #mc_server{type = rsPrimary, setName = SetName, electionId = ElectionId, hosts = Hosts, arbiters = Arbiters, passives = Passives},
    State = #topology_state{setName = undefined, topology_opts = Topts, worker_opts = Wopts, servers = Tab}) ->
  HostsList = lists:flatten([Hosts, Arbiters, Passives]),
  init_seeds(HostsList, Tab, Topts, Wopts),
  stop_servers_not_in_list(HostsList, Tab),
  State#topology_state{setName = SetName, maxElectionId = ElectionId, type = checkIfHasPrimary(Tab)};
update_topology_state(#mc_server{type = rsPrimary, pid = Pid, host = Host, setName = SSetName},
    State = #topology_state{setName = CSetName, servers = Tab}) when SSetName =/= CSetName ->
  ets:insert(Tab, #mc_server{pid = Pid, host = Host, type = deleted}),
  exit(Pid, kill),
  State#topology_state{type = checkIfHasPrimary(Tab)};
update_topology_state(_, State) ->
  State.

init_seeds(#topology_state{seeds = Seeds, topology_opts = Topts, worker_opts = Wopts, servers = Tab}) ->
  init_seeds(Seeds, Tab, Topts, Wopts).

init_seeds([], _, _, _) -> ok;
init_seeds([Addr | Seeds], Tab, Topts, Wopts) ->
  Host = mc_utils:to_binary(Addr),
  Saved = ets:select(Tab, [{#mc_server{host = Host, _ = '_'}, [], ['$_']}]),
  start_seed(Saved, Host, Tab, Topts, Wopts),
  init_seeds(Seeds, Tab, Topts, Wopts).


%% @private
start_seed([], Host, Tab, Topts, Wopts) ->
  {ok, Pid} = mc_server:start(self(), Host, Topts, Wopts),
  MRef = erlang:monitor(process, Pid),
  ets:insert(Tab, #mc_server{pid = Pid, mref = MRef, host = Host});
start_seed(_, _, _, _, _) ->
  ok.

%% @private
set_possible_primary(_, undefined) -> ok;
set_possible_primary(Tab, Addr) ->
  Saved = ets:select(Tab, [{#mc_server{host = Addr, type = unknown, _ = '_'}, [], ['$_']}]),
  update_possible_primary(Tab, Saved).

%% @private
update_possible_primary(_, []) -> ok;
update_possible_primary(Tab, [Saved]) ->
  ets:insert(Tab, Saved#mc_server{type = possiblePrimary}).

%% @private
checkIfHasPrimary(Tab) ->
  Res = ets:select(Tab, [{#mc_server{type = rsPrimary, _ = '_'}, [], ['$_']}]),
  checkIfHasPrimary_Res(Res).

%% @private
checkIfHasPrimary_Res([]) ->
  replicaSetNoPrimary;
checkIfHasPrimary_Res(_) ->
  replicaSetWithPrimary.

%% @private
stop_servers_not_in_list(HostsList, Tab) ->
  ets:foldl(
    fun(E, Acc) ->
      case lists:member(E#mc_server.host, HostsList) of
        false ->
          ets:insert(Tab, E#mc_server{type = deleted}),
          unlink(E#mc_server.pid),
          exit(E#mc_server.pid, kill),
          [E#mc_server.host | Acc];
        true -> Acc
      end
    end, [], Tab).