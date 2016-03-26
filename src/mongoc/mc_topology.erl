%%%-------------------------------------------------------------------
%%% @author Alexander Hudich (alttagil@gmail.com)
%%% @copyright (C) 2015, Alexander Hudich
%%% @doc
%%% mongoc internal module for monitoring a topology of one or more servers.
%%% @end
%%%-------------------------------------------------------------------
-module(mc_topology).
-author("alttagil@gmail.com").

-behaviour(gen_server).

-include("mongoc.hrl").

%% API
-export([start_link/3, drop_server/2, update_topology_state/2, update_topology/1, get_state/1, get_pool/2, select_server/3, get_pool/4, get_pool/1, disconnect/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {
  seeds = [],

  self,

  type = unknown,
  maxElectionId = undefined,
  setName = undefined,
  setVersion = undefined,

  rp_mode = primary,
  rp_tags = [],

  servers,

  serverSelectionTimeoutMS = 30000,
  localThresholdMS = 200,

  worker_opts = [],
  topology_opts = [],
  get_pool_timeout :: integer()
}).


-spec(start_link(any(), any(), any()) ->
  {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
-spec(init(Args :: term()) ->
  {ok, State :: #state{}} | {ok, State :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term()} | ignore).
-spec(terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: #state{}) -> term()).
-spec(code_change(OldVsn :: term() | {down, term()}, State :: #state{},
    Extra :: term()) ->
  {ok, NewState :: #state{}} | {error, Reason :: term()}).


%%%===================================================================
%%% API
%%%===================================================================

start_link(Seeds, TopologyOptions, WorkerOptions) ->
  gen_server:start_link(?MODULE, [Seeds, TopologyOptions, WorkerOptions], []).


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([SeedsList, TopologyOptions, WorkerOptions]) ->
  try_register(TopologyOptions),

  {Type, SetName, Seeds} = parse_seeds(SeedsList),

  ServerSelectionTimeoutMS = mc_utils:get_value(serverSelectionTimeoutMS, TopologyOptions, 30000),
  LocalThresholdMS = mc_utils:get_value(localThresholdMS, TopologyOptions, 200),

  RPmode = mc_utils:get_value(rp_mode, TopologyOptions, primary),
  RPTags = mc_utils:get_value(tags, TopologyOptions, []),
  GetPoolTimeout = mc_utils:get_value(get_pool_timeout, TopologyOptions, 5000),

  Servers = ets:new(mc_servers, [set, {keypos, 2}]),

  State = #state{
    self = self(),

    servers = Servers,
    type = Type,

    rp_mode = RPmode,
    rp_tags = RPTags,

    seeds = Seeds,
    setName = SetName,

    serverSelectionTimeoutMS = ServerSelectionTimeoutMS,
    localThresholdMS = LocalThresholdMS,

    topology_opts = TopologyOptions,
    worker_opts = WorkerOptions,
    get_pool_timeout = GetPoolTimeout
  },

  gen_server:cast(self(), init_seeds),

  {ok, State}.



terminate(_Reason, _State) ->
  ok.


code_change(_OldVsn, State, _Extra) ->
  {ok, State}.


%%%===================================================================
%%% External functions
%%%===================================================================


disconnect(Topology) ->
  gen_server:cast(Topology, halt).

update_topology(Topology) ->
  gen_server:cast(Topology, update_topology).


get_state(Topology) ->
  gen_server:call(Topology, get_state).


get_pool(Topology) ->
  get_pool(Topology, []).

get_pool(Topology, Options) ->
  State = mc_topology:get_state(Topology),

  RPMode = mc_utils:get_value(rp_mode, Options, State#state.rp_mode),
  RPTags = mc_utils:get_value(rp_tags, Options, State#state.rp_tags),

  get_pool(RPMode, RPTags, State).

get_pool(RPMode, RPTags, State) ->
  TO = State#state.topology_opts,
  ServerSelectionTimeoutMS = mc_utils:get_value(serverSelectionTimeoutMS, TO, 30000),

  Pid = spawn(?MODULE, get_pool, [self(), State, RPMode, RPTags]),
  receive
    {Pid, {error, Reason}, _} ->
      {error, Reason};
    {Pid, Pool, Type} ->
      {ok, #{pool => Pool, server_type => Type, read_preference => #{mode => RPMode, tags => RPTags}}}
  after
    ServerSelectionTimeoutMS ->
      exit(Pid, timeout),
      mc_topology:update_topology(State#state.self),
      {error, timeout}
  end.

get_pool(From, #state{self = Topology, get_pool_timeout = TM} = State, RPMode, Tags) ->
  case select_server(Topology, RPMode, Tags) of
    #mc_server{pid = Pid, type = Type} ->
      Pool = mc_server:get_pool(Pid, TM),
      From ! {self(), Pool, Type};

    undefined ->
      timer:sleep(100),
      get_pool(From, State, RPMode, Tags)
%			From ! { self(), { error, not_found }, unknown }
  end.


%%%===================================================================
%%% Handlers
%%%===================================================================

handle_call(get_state, _From, State) ->
  {reply, State, State};

handle_call(_Request, _From, State) ->
  {reply, ok, State}.



handle_cast(halt, State) ->
  {stop, normal, State};

handle_cast(init_seeds, State) ->
  init_seeds(State),
  {noreply, State};

handle_cast({monitor_ismaster, Server, IsMaster, RTT}, State) ->
  NState = parse_ismaster(Server, IsMaster, RTT, State),
  {noreply, NState};

handle_cast({server_to_unknown, Server}, State) ->
  NState = handle_server_to_unknown(Server, State),
  {noreply, NState};

handle_cast({drop_server, Pid}, #state{servers = Tab} = State) ->
  ets:delete(Tab, Pid),
  {noreply, State};

handle_cast(update_topology, #state{servers = Tab} = State) ->
  ets:foldl(fun(E, Acc) ->
    mc_monitor:next_loop(E#mc_server.pid),
    Acc
            end, [], Tab),
  {noreply, State};

handle_cast(_Request, State) ->
  {noreply, State}.



handle_info({'DOWN', MRef, _, _, _}, #state{topology_opts = Topts, worker_opts = Wopts, servers = Tab} = State) ->
  case ets:match(Tab, #mc_server{pid = '$1', host = '$2', mref = MRef, _ = '_'}) of
    [[Pid, Host]] ->
      true = ets:delete(Tab, Pid),
      init_seeds([Host], Tab, Topts, Wopts),
      {noreply, State};
    [] ->
      {noreply, State}
  end;


handle_info(_Info, State) ->
  {noreply, State}.


%%%===================================================================
%%% Internal functions
%%%===================================================================

init_seeds(#state{seeds = Seeds, topology_opts = Topts, worker_opts = Wopts, servers = Tab} = _State) ->
  init_seeds(Seeds, Tab, Topts, Wopts).

init_seeds([], _, _, _) ->
  ok;
init_seeds([Addr | Seeds], Tab, Topts, Wopts) ->
  Host = to_binary(Addr),
  Saved = ets:select(Tab, [{#mc_server{host = Host, _ = '_'}, [], ['$_']}]),
  start_seed(Saved, Host, Tab, Topts, Wopts),
  init_seeds(Seeds, Tab, Topts, Wopts).


start_seed([], Host, Tab, Topts, Wopts) ->
  {ok, Pid} = mc_server:start(self(), Host, Topts, Wopts),
  MRef = erlang:monitor(process, Pid),
  ets:insert(Tab, #mc_server{pid = Pid, mref = MRef, host = Host, type = unknown});

start_seed(_, _, _, _, _) ->
  ok.


%% Register this process if needed
try_register(Options) ->
  case lists:keyfind(register, 1, Options) of
    false -> ok;
    {_, Name} when is_atom(Name) -> register(Name, self());
    {_, RegFun} when is_function(RegFun) -> RegFun(self())
  end.



handle_server_to_unknown(Server, #state{servers = Tab} = State) ->
  [Saved] = ets:select(Tab, [{#mc_server{pid = Server, _ = '_'}, [], ['$_']}]),

  ToUpdate = #mc_server{
    pid = Saved#mc_server.pid,
    mref = Saved#mc_server.mref,
    host = Saved#mc_server.host,
    type = unknown
  },

  ets:insert(Tab, ToUpdate),
  mc_server:update_unknown(ToUpdate#mc_server.pid),

  update_topology_state(ToUpdate, State).



parse_ismaster(Server, IsMaster, RTT, #state{servers = Tab} = State) ->

  SType = server_type(IsMaster),

  [Saved] = ets:select(Tab, [{#mc_server{pid = Server, _ = '_'}, [], ['$_']}]),

  {OldRTT, NRTT} = parse_rtt(Saved#mc_server.old_rtt, Saved#mc_server.rtt, RTT),

  ToUpdate = Saved#mc_server{

    type = SType,

    me = maps:get(<<"me">>, IsMaster, undefined),

    old_rtt = OldRTT,
    rtt = NRTT,

    setName = maps:get(<<"setName">>, IsMaster, undefined),
    setVersion = maps:get(<<"setVersion">>, IsMaster, undefined),

    tags = maps:get(<<"tags">>, IsMaster, []),

    minWireVersion = maps:get(<<"minWireVersion">>, IsMaster, 0),
    maxWireVersion = maps:get(<<"maxWireVersion">>, IsMaster, 2),

    hosts = maps:get(<<"hosts">>, IsMaster, []),

    passives = maps:get(<<"passives">>, IsMaster, []),
    arbiters = maps:get(<<"arbiters">>, IsMaster, []),


    electionId = maps:get(<<"electionId">>, IsMaster, undefined),

    primary = maps:get(<<"primary">>, IsMaster, undefined),
    ismaster = maps:get(<<"ismaster">>, IsMaster, undefined),
    secondary = maps:get(<<"secondary">>, IsMaster, undefined)
  },

  ets:insert(Tab, ToUpdate),

  mc_server:update_ismaster(ToUpdate#mc_server.pid, {SType, ToUpdate}),

  update_topology_state(ToUpdate, State).



parse_rtt(_, undefined, RTT) ->
  {RTT, RTT};

parse_rtt(OldRTT, CurRTT, RTT) ->
  A = 0.2,
  {CurRTT, A * RTT + (1 - A) * OldRTT / 1000}.


%% SHARDED

update_topology_state(
    #mc_server{type = SType, pid = Pid},
    #state{type = sharded} = State)
  when SType =:= standalone; SType =:= rsPrimary; SType =:= rsSecondary; SType =:= rsArbiter; SType =:= rsOther; SType =:= rsGhost
  ->
  exit(Pid, kill),
  State;

update_topology_state(_, #state{type = sharded} = State) ->
  State;


%% UNKNOWN

update_topology_state(
    #mc_server{type = unknown},
    #state{type = unknown} = State) ->

  State;

update_topology_state(
    #mc_server{type = rsGhost},
    #state{type = unknown} = State) ->

  State;

update_topology_state(
    #mc_server{type = standalone, pid = Pid},
    #state{type = unknown, seeds = Seeds} = State)
  ->
  SeedsLen = length(Seeds),
  if
    SeedsLen =< 1 ->
      State#state{type = standalone};

    true ->
      exit(Pid, kill),
      State
  end;

update_topology_state(
    #mc_server{type = mongos},
    #state{type = unknown} = State)
  ->
  State#state{type = sharded};

update_topology_state(
    #mc_server{type = SType, setName = SetName} = Server,
    #state{type = unknown, setName = SetName} = State)
  when SType =:= rsSecondary; SType =:= rsArbiter; SType =:= rsOther
  ->
  update_topology_state(Server, State#state{setName = undefined});

update_topology_state(
    #mc_server{type = SType, setName = SetName, hosts = Hosts, arbiters = Arbiters, passives = Passives, primary = Primary},
    #state{type = unknown, setName = undefined, topology_opts = Topts, worker_opts = Wopts, servers = Tab} = State)
  when SType =:= rsSecondary; SType =:= rsArbiter; SType =:= rsOther
  ->
  init_seeds(lists:flatten([Hosts, Arbiters, Passives]), Tab, Topts, Wopts),
  set_possible_primary(Tab, Primary),
  State#state{type = checkIfHasPrimary(Tab), setName = SetName};

update_topology_state(
    #mc_server{type = SType, pid = Pid},
    #state{type = unknown} = State)
  when SType =:= rsSecondary; SType =:= rsArbiter; SType =:= rsOther
  ->
  exit(Pid, kill),
  State;


%% REPLICASETNOPRIMARY

update_topology_state(
    #mc_server{type = SType, pid = Pid},
    #state{type = replicaSetNoPrimary} = State)
  when SType =:= standalone; SType =:= mongos
  ->
  exit(Pid, kill),
  State;

update_topology_state(
    #mc_server{type = SType, setName = SetName} = Server,
    #state{type = replicaSetNoPrimary, setName = SetName} = State)
  when SType =:= rsSecondary; SType =:= rsArbiter; SType =:= rsOther
  ->
  update_topology_state(Server, State#state{setName = undefined});

update_topology_state(
    #mc_server{type = SType, setName = SetName, hosts = Hosts, arbiters = Arbiters, passives = Passives, primary = Primary},
    #state{type = replicaSetNoPrimary, setName = undefined, topology_opts = Topts, worker_opts = Wopts, servers = Tab} = State)
  when SType =:= rsSecondary; SType =:= rsArbiter; SType =:= rsOther
  ->
  init_seeds(lists:flatten([Hosts, Arbiters, Passives]), Tab, Topts, Wopts),
  set_possible_primary(Tab, Primary),
  State#state{setName = SetName};

update_topology_state(
    #mc_server{type = SType, pid = Pid},
    #state{type = replicaSetNoPrimary} = State)
  when SType =:= rsSecondary; SType =:= rsArbiter; SType =:= rsOther
  ->
  exit(Pid, kill),
  State;


%% REPLICASETWITHPRIMARY

update_topology_state(
    #mc_server{type = SType},
    #state{type = replicaSetWithPrimary, servers = Tab} = State)
  when SType =:= unknown; SType =:= rsGhost
  ->
  State#state{type = checkIfHasPrimary(Tab)};

update_topology_state(
    #mc_server{type = SType, pid = Pid},
    #state{type = replicaSetWithPrimary, servers = Tab} = State)
  when SType =:= standalone; SType =:= mongos
  ->
  exit(Pid, kill),
  State#state{type = checkIfHasPrimary(Tab)};

update_topology_state(
    #mc_server{type = SType, setName = SetName, primary = Primary},
    #state{type = replicaSetWithPrimary, setName = SetName, servers = Tab} = State)
  when SType =:= rsSecondary; SType =:= rsArbiter; SType =:= rsOther
  ->
  set_possible_primary(Tab, Primary),
  State#state{type = checkIfHasPrimary(Tab)};

update_topology_state(
    #mc_server{type = SType, pid = Pid},
    #state{type = replicaSetWithPrimary, servers = Tab} = State)
  when SType =:= rsSecondary; SType =:= rsArbiter; SType =:= rsOther
  ->
  exit(Pid, kill),
  State#state{type = checkIfHasPrimary(Tab)};


%% REPLICASETWITHPRIMARY


update_topology_state(
    #mc_server{type = rsPrimary, setName = SetName} = Server,
    #state{setName = SetName} = State)
  ->
  update_topology_state(Server, State#state{setName = undefined});


update_topology_state(
    #mc_server{type = rsPrimary, electionId = ElectionId, host = Host, pid = Pid},
    #state{setName = undefined, maxElectionId = MaxElectionId, servers = Tab} = State)
  when ElectionId =/= undefined, MaxElectionId =/= undefined, ElectionId < MaxElectionId
  ->
  ets:insert(Tab, #mc_server{pid = Pid, host = Host, type = unknown}),
  State#state{type = checkIfHasPrimary(Tab)};

update_topology_state(
    #mc_server{type = rsPrimary, setName = SetName, electionId = ElectionId, hosts = Hosts, arbiters = Arbiters, passives = Passives},
    #state{setName = undefined, topology_opts = Topts, worker_opts = Wopts, servers = Tab} = State)
  ->
  HostsList = lists:flatten([Hosts, Arbiters, Passives]),
  init_seeds(HostsList, Tab, Topts, Wopts),
  stop_servers_not_in_list(HostsList, Tab),
  State#state{setName = SetName, maxElectionId = ElectionId, type = checkIfHasPrimary(Tab)};

update_topology_state(
    #mc_server{type = rsPrimary, pid = Pid, host = Host, setName = SSetName},
    #state{setName = CSetName, servers = Tab} = State)
  when SSetName =/= CSetName
  ->
  ets:insert(Tab, #mc_server{pid = Pid, host = Host, type = deleted}),
  exit(Pid, kill),
  State#state{type = checkIfHasPrimary(Tab)};


update_topology_state(_, State) ->
  State.



server_type(#{<<"ismaster">> := true, <<"secondary">> := false, <<"setName">> := _}) ->
  rsPrimary;

server_type(#{<<"ismaster">> := false, <<"secondary">> := true, <<"setName">> := _}) ->
  rsSecondary;

server_type(#{<<"arbiterOnly">> := true, <<"setName">> := _}) ->
  rsArbiter;

server_type(#{<<"hidden">> := true, <<"setName">> := _}) ->
  rsOther;

server_type(#{<<"setName">> := _}) ->
  rsOther;

server_type(#{<<"msg">> := <<"isdbgrid">>}) ->
  mongos;

server_type(#{<<"isreplicaset">> := true}) ->
  rsGhost;

server_type(#{<<"ok">> := _}) ->
  unknown;

server_type(_) ->
  standalone.



drop_server(Topology, Server) ->
  gen_server:cast(Topology, {drop_server, Server}).


set_possible_primary(_, undefined) ->
  ok;
set_possible_primary(Tab, Addr) ->
  Saved = ets:select(Tab, [{#mc_server{host = Addr, type = unknown, _ = '_'}, [], ['$_']}]),
  update_possible_primary(Tab, Saved).


update_possible_primary(_, []) ->
  ok;
update_possible_primary(Tab, [Saved]) ->
  ets:insert(Tab, Saved#mc_server{type = possiblePrimary}).


to_binary(Str) when is_list(Str) ->
  list_to_binary(Str);
to_binary(Str) when is_binary(Str) ->
  Str.



checkIfHasPrimary(Tab) ->
  Res = ets:select(Tab, [{#mc_server{type = rsPrimary, _ = '_'}, [], ['$_']}]),
  checkIfHasPrimary_Res(Res).

checkIfHasPrimary_Res([]) ->
  replicaSetNoPrimary;
checkIfHasPrimary_Res(_) ->
  replicaSetWithPrimary.



stop_servers_not_in_list(HostsList, Tab) ->

  ets:foldl(fun(E, Acc) ->
    M = lists:member(E#mc_server.host, HostsList),
    if
      M =/= true ->
        ets:insert(Tab, E#mc_server{type = deleted}),
        unlink(E#mc_server.pid),
        exit(E#mc_server.pid, kill),
        [E#mc_server.host | Acc];

      true ->
        Acc
    end
            end, [], Tab).


parse_seeds({single, Addr}) ->
  {single, undefined, [Addr]};
parse_seeds({unknown, Seeds}) ->
  {unknown, undefined, Seeds};
parse_seeds({sharded, Seeds}) ->
  {sharded, undefined, Seeds};
parse_seeds({rs, SetName, Seeds}) ->
  {replicaSetNoPrimary, SetName, Seeds};
parse_seeds(Addr) when is_list(Addr) ->
  {single, undefined, [Addr]}.




select_server(Topology, primaryPreferred, Tags) ->
  case select_server(Topology, primary, Tags) of
    undefined ->
      select_server(Topology, secondary, Tags);
    Primary ->
      Primary
  end;

select_server(Topology, secondaryPreferred, Tags) ->
  case select_server(Topology, secondary, Tags) of
    undefined ->
      select_server(Topology, primary, Tags);
    Primary ->
      Primary
  end;

select_server(Topology, nearest, Tags) ->
  get_nearest(select_server(Topology, primary, Tags), select_server(Topology, secondary, Tags));

select_server(Topology, Mode, Tags) ->
  #state{servers = Tab, type = TType, localThresholdMS = Threshold} = get_state(Topology),

  LowestRTT = ets:foldl(
    fun
      (#mc_server{rtt = RTT, type = Type}, Acc) when Type =:= rsSecondary; Type =:= mongos; Type =:= standalone ->
        if
          Acc =:= 0 ->
            RTT;
          RTT < Acc ->
            RTT;
          true ->
            Acc
        end;
      (_, Acc) ->
        Acc
    end,
    0, Tab),

  MaxRTT = LowestRTT + Threshold,

  Candidates = ets:foldl(
    fun(E, Acc) ->
      case is_candidate(Mode, Tags, E, MaxRTT) of
        false ->
          Acc;
        Res ->
          [Res | Acc]
      end
    end,
    [], Tab),


  select_candidate(Mode, TType, Candidates).




get_nearest(undefined, A) ->
  A;

get_nearest(A, undefined) ->
  A;

get_nearest(#mc_server{rtt = RTT1} = A, #mc_server{rtt = RTT2} = B) ->
  if
    RTT1 < RTT2 ->
      A;
    true ->
      B
  end.



select_candidate(_, _, []) ->
  undefined;

select_candidate(primary, _, [Primary]) ->
  Primary;

select_candidate(primary, sharded, List) ->
  Len = length(List),
  pick_random(List, Len);

select_candidate(secondary, _, List) ->
  Len = length(List),
  pick_random(List, Len).



pick_random([Item], 1) ->
  Item;
pick_random(List, N) ->
  lists:nth(random:uniform(N), List).





is_candidate(_, _, #mc_server{type = Type}, _) when Type =/= rsPrimary, Type =/= rsSecondary, Type =/= mongos, Type =/= standalone ->
  false;

is_candidate(_, _, #mc_server{type = standalone} = Server, _) ->
  Server;


is_candidate(primary, _, #mc_server{type = rsPrimary} = Server, _) ->
  Server;

is_candidate(primary, _, #mc_server{type = mongos, rtt = RTT} = Server, MaxRTT) when RTT =< MaxRTT ->
  Server;

is_candidate(primary, _, #mc_server{type = _}, _) ->
  false;


is_candidate(secondary, Tags, #mc_server{type = rsSecondary, tags = STags, rtt = RTT} = Server, MaxRTT) when RTT =< MaxRTT ->
  check_tags(Server, Tags, STags);

is_candidate(secondary, _, #mc_server{type = mongos, rtt = RTT} = Server, MaxRTT) when RTT =< MaxRTT ->
  Server;


is_candidate(_, _, _, _) ->
  false.


check_tags(Server, Tags, STags) ->
  ResL = length(lists:subtract(STags, lists:subtract(STags, Tags))),
  L = length(Tags),
  if
    ResL =/= L ->
      false;
    true ->
      Server
  end.