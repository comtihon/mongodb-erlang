%%%-------------------------------------------------------------------
%%% @author Alexander Hudich (alttagil@gmail.com)
%%% @copyright (C) 2015, Alexander Hudich
%%% @doc
%%% mongoc internal module for monitoring one mongodb server and providing information to a topology module
%%% @end
%%%-------------------------------------------------------------------
-module(mc_monitor).
-author("alttagil@gmail.com").

-behaviour(gen_server).

%% API
-export([start_link/5, do_timeout/2, next_loop/1, update_type/2, stop/1]).

%% gen_server callbacks
-export([init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3]).


-define(SERVER, ?MODULE).

-record(state,
{
  type,
  host,
  port,
  topology,
  server,
  topology_opts,
  worker_opts,
  pool,
  connect_to = 20000,
  heartbeatF = 10000,
  minHeartbeatF = 1000,
  timer,
  counter = 0
}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Topology, Server, HostPort, Topts, Wopts) ->
  gen_server:start_link(?MODULE, [Topology, Server, HostPort, Topts, Wopts], []).

stop(Pid) ->
  gen_server:cast(Pid, halt).


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Topology, Server, {Host, Port}, Topts, Wopts]) ->
  process_flag(trap_exit, true),
  ConnectTimeoutMS = proplists:get_value(connectTimeoutMS, Topts, 20000),
  HeartbeatFrequencyMS = proplists:get_value(heartbeatFrequencyMS, Topts, 10000),
  MinHeartbeatFrequencyMS = proplists:get_value(minHeartbeatFrequencyMS, Topts, 1000),
  gen_server:cast(self(), loop),
  {ok, #state{host = Host, port = Port, topology = Topology, server = Server,
    topology_opts = Topts, worker_opts = Wopts, connect_to = ConnectTimeoutMS,
    heartbeatF = HeartbeatFrequencyMS, minHeartbeatF = MinHeartbeatFrequencyMS}}.

terminate(_Reason, _State) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.


update_type(undefined, _) -> ok;
update_type(Pid, Type) -> gen_server:cast(Pid, {update_type, Type}).


%%%===================================================================
%%% Handlers
%%%===================================================================
handle_call(_Request, _From, State) ->
  {reply, ok, State}.

handle_cast({update_type, Type}, State) ->
  {noreply, State#state{type = Type}};
handle_cast(loop, State) ->
  loop(State),
  {noreply, State};
handle_cast({loopn, Pid, TimeOut}, State = #state{timer = PausePid}) ->
  send_stop(PausePid),
  Timer = spawn_link(?MODULE, do_timeout, [Pid, TimeOut]),
  {noreply, State#state{timer = Timer}, hibernate};
handle_cast(loopn, State = #state{timer = PausePid}) ->
  send_stop(PausePid),
  NState = loop(State),
  {noreply, NState};
handle_cast(halt, State) ->
  {stop, normal, State};
handle_cast(_Request, State) ->
  {noreply, State}.

handle_info({'EXIT', Pid, _Reason}, State = #state{server = Pid}) ->
  exit(kill),
  {noreply, State};
handle_info(_Info, State) ->
  {noreply, State}.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
next_loop(Pid, Timestamp) ->
  gen_server:cast(Pid, {loopn, Pid, Timestamp}).

%% @private
next_loop(Pid) ->
  gen_server:cast(Pid, loopn).

%% @private
loop(State = #state{type = Type, host = Host, port = Port, topology = Topology, server = Server,
  connect_to = Timeout, heartbeatF = HB_MS, minHeartbeatF = MinHB_MS, counter = Counter, worker_opts = WOpts}) ->
  ConnectArgs = form_args(Host, Port, Timeout, WOpts),
  try check(ConnectArgs, Server) of
    Res ->
      gen_server:cast(Topology, Res),
      next_loop(self(), HB_MS)
  catch
    _:_ ->
      maybe_recheck(Type, Topology, Server, ConnectArgs, HB_MS, MinHB_MS)
  end,
  State#state{timer = undefined, counter = Counter + 1}.

%% @private
maybe_recheck(unknown, Topology, Server, _, _, _) ->
  gen_server:cast(Topology, {server_to_unknown, Server}),
  next_loop(self(), 1);
maybe_recheck(_, Topology, Server, ConnectArgs, HB_MS, MinHB_MS) ->
  timer:sleep(MinHB_MS),
  try check(ConnectArgs, Server) of
    Res ->
      gen_server:cast(Topology, Res),
      next_loop(self(), HB_MS)
  catch
    _:_ ->
      gen_server:cast(Topology, {server_to_unknown, Server}),
      next_loop(self(), MinHB_MS)
  end.

%% @private
check(ConnectArgs, Server) ->
  Start = os:timestamp(),
  {ok, Conn} = mc_worker_api:connect(ConnectArgs),
  {true, IsMaster} = mc_worker_api:command(Conn, {isMaster, 1}),
  Finish = os:timestamp(),
  mc_worker_api:disconnect(Conn),
  {monitor_ismaster, Server, IsMaster, timer:now_diff(Finish, Start)}.

%% @private
do_timeout(Pid, TO) when TO > 0 ->
  receive
    stop -> ok;
    run -> next_loop(Pid)
  after
    TO -> next_loop(Pid)
  end;
do_timeout(Pid, _TO) ->
  next_loop(Pid).

%% @private
send_stop(undefined) -> ok;
send_stop(PausePid) -> PausePid ! stop.

%% @private
form_args(Host, Port, Timeout, WorkerArgs) ->
  case mc_utils:get_value(ssl, WorkerArgs, false) of
    true -> [{host, Host}, {port, Port}, {timeout, Timeout}, {ssl, true}];
    false -> [{host, Host}, {port, Port}, {timeout, Timeout}]
  end.