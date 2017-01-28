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

-record(topology_state, {
  seeds = [],
  self :: pid(),
  type = unknown,
  maxElectionId = undefined,
  setName = undefined,
  setVersion = undefined,
  rp_mode = primary,
  rp_tags = [],
  servers = [] :: list(),
  serverSelectionTimeoutMS = 30000,
  localThresholdMS = 200,
  worker_opts = [],
  topology_opts = [],
  get_pool_timeout :: integer()
}).

%% API
-export([
  start_link/3,
  drop_server/2,
  update_topology/1,
  get_state/1,
  get_pool/2,
  get_pool/4,
  get_pool/1,
  disconnect/1,
  get_state_part/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%%%===================================================================
%%% API
%%%===================================================================

disconnect(Topology) ->
  gen_server:cast(Topology, halt).

update_topology(Topology) ->
  gen_server:cast(Topology, update_topology).

get_state(Topology) ->
  gen_server:call(Topology, get_state).

get_state_part(Topology) ->
  gen_server:call(Topology, get_state_part).

get_pool(Topology) ->
  get_pool(Topology, #{}).

-spec get_pool(pid() | atom(), map() | list()) -> {ok, map()} | {error, any()}.
get_pool(Topology, Options) when is_list(Options) ->
  get_pool(Topology, maps:from_list(Options));
get_pool(Topology, Options) ->
  State = mc_topology:get_state(Topology),
  RPMode = maps:get(rp_mode, Options, State#topology_state.rp_mode),
  RPTags = maps:get(rp_tags, Options, State#topology_state.rp_tags),
  get_pool(RPMode, RPTags, State).

get_pool(From, State = #topology_state{self = Topology}, RPMode, Tags) ->
  case mc_selecting_logics:select_server(Topology, RPMode, Tags) of
    #mc_server{pid = Pid, type = Type} ->
      From ! {self(), Pid, Type};
    undefined ->
      timer:sleep(100),
      get_pool(From, State, RPMode, Tags)
  end.

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
  State = #topology_state{
    self = self(),
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

handle_call(get_state_part, _From, State) ->
  #topology_state{servers = Servers, type = TType, localThresholdMS = Threshold} = State,
  {reply, {Servers, TType, Threshold}, State};
handle_call(get_state, _From, State) ->
  {reply, State, State};
handle_call(_Request, _From, State) ->
  {reply, ok, State}.

handle_cast(halt, State) ->
  {stop, normal, State};
handle_cast(init_seeds, State) ->
  #topology_state{seeds = Seeds, topology_opts = Topts, worker_opts = Wopts, servers = Servers} = State,
  UServers = mc_topology_logics:init_seeds(Seeds, Topts, Wopts, Servers),
  {noreply, State#topology_state{servers = UServers}};
handle_cast({monitor_ismaster, Server, IsMaster, RTT}, State) ->
  NState = parse_ismaster(Server, IsMaster, RTT, State),
  {noreply, NState};
handle_cast({server_to_unknown, Server}, State) ->
  NState = handle_server_to_unknown(Server, State),
  {noreply, NState};
handle_cast({drop_server, Pid}, State = #topology_state{servers = Servers}) ->
  UServers = lists:keydelete(Pid, #mc_server.pid, Servers),
  {noreply, State#topology_state{servers = UServers}};
handle_cast(update_topology, State = #topology_state{servers = Servers}) ->
  lists:foreach(fun(E) -> mc_monitor:next_loop(E#mc_server.pid) end, Servers),
  {noreply, State};
handle_cast(_Request, State) ->
  {noreply, State}.

handle_info({'DOWN', MRef, _, _, _}, State) ->
  #topology_state{topology_opts = Topts, worker_opts = Wopts, servers = Servers} = State,
  case lists:keyfind(MRef, #mc_server.mref, Servers) of
    #mc_server{host = Host, pid = Pid} ->
      Cleared = lists:keydelete(Pid, #mc_server.pid, Servers),
      UServers = mc_topology_logics:init_seeds([Host], Topts, Wopts, Cleared),
      {noreply, State#topology_state{servers = UServers}};
    false ->
      {noreply, State}
  end;
handle_info(_Info, State) ->
  {noreply, State}.

terminate(_Reason, _State) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.


%%%===================================================================
%%% External functions
%%%===================================================================
%% @private
get_pool(RPMode, RPTags, State) ->
  TO = State#topology_state.topology_opts,
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
      update_topology(State#topology_state.self), %TODO refactor
      {error, timeout}
  end.


%%%===================================================================
%%% Internal functions
%%%===================================================================
%% @private
%% Register this process if needed
try_register(Options) ->
  case lists:keyfind(register, 1, Options) of
    false -> ok;
    {_, Name} when is_atom(Name) -> register(Name, self());
    {_, RegFun} when is_function(RegFun) -> RegFun(self())
  end.

%% @private
handle_server_to_unknown(Server, State = #topology_state{servers = Servers}) ->
  Saved = lists:keyfind(Server, #mc_server.pid, Servers),
  ToUpdate = #mc_server{
    pid = Saved#mc_server.pid,
    mref = Saved#mc_server.mref,
    host = Saved#mc_server.host,
    type = unknown
  },
  UServers = lists:keyreplace(Server, #mc_server.pid, Servers, ToUpdate),
  mc_seed_logics:update_unknown(ToUpdate#mc_server.pid),
  mc_topology_logics:update_topology_state(ToUpdate, State#topology_state{servers = UServers}).

%% @private
parse_ismaster(Server, IsMaster, RTT, State = #topology_state{servers = Servers}) ->
  SType = server_type(IsMaster),
  [Saved] = ets:select(Servers, [{#mc_server{pid = Server, _ = '_'}, [], ['$_']}]),
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
  ets:insert(Servers, ToUpdate),
  mc_server:update_ismaster(ToUpdate#mc_server.pid, {SType, ToUpdate}),
  mc_topology_logics:update_topology_state(ToUpdate, State).

%% @private
parse_rtt(_, undefined, RTT) -> {RTT, RTT};
parse_rtt(OldRTT, CurRTT, RTT) ->
  A = 0.2,
  {CurRTT, A * RTT + (1 - A) * OldRTT / 1000}.

%% @private
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

%% @private
drop_server(Topology, Server) ->
  gen_server:cast(Topology, {drop_server, Server}).

%% @private
parse_seeds({single, Addr}) ->
  {single, undefined, [parse_seed(Addr)]};
parse_seeds({unknown, Seeds}) ->
  {unknown, undefined, parse_multiple(Seeds)};
parse_seeds({sharded, Seeds}) ->
  {sharded, undefined, parse_multiple(Seeds)};
parse_seeds({rs, SetName, Seeds}) ->
  {replicaSetNoPrimary, SetName, parse_multiple(Seeds)};
parse_seeds(Addr) when is_list(Addr) ->
  {single, undefined, [parse_seed(Addr)]}.

%% @private
parse_multiple(Seeds) ->
  lists:map(fun parse_seed/1, Seeds).

%% @private
parse_seed(Addr) when is_binary(Addr) ->
  parse_seed(binary_to_list(Addr));
parse_seed(Addr) when is_list(Addr) ->
  [Host, Port] = string:tokens(Addr, ":"),
  {Host, list_to_integer(Port)}.