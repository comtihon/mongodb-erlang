-module(mc_worker_pid_info).

%% This module is used to get information (currently only the protocol type)
%% ragaring mc_worker processes. This is useful so we can encode messages in
%% the right way befor sending them to an mc_worker process.

-behaviour(gen_server).

-include("mongo_protocol.hrl").

-export([start_link/0,
  init/1,
  terminate/2,
  handle_cast/2,
  handle_call/3,
  get_info/1,
  set_info/2,
  discard_info/1,
  get_protocol_type/1,
  handle_info/2,
  install_mc_worker_info/4]).

-dialyzer({no_fail_call, detect_protocol_type/4}).

-define(CLEAN_TABLE_PERIOD_MINS, 30).
-define(CLEAN_TABLE_MESSAGE, clean_table).
-define(MC_WORKER_PID_INFO_TAB_NAME, mc_worker_pid_info_tab).

-spec start_link() -> {ok, pid()}.
start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init(_) ->
  ets:new(?MC_WORKER_PID_INFO_TAB_NAME, [public, named_table, {read_concurrency, true}]),
  {ok, start_cleanup_timer_update_state(#{})}.

start_cleanup_timer_update_state(State) ->
  State#{
    cleanup_timer_ref =>
    erlang:start_timer(timer:minutes(?CLEAN_TABLE_PERIOD_MINS),
      self(),
      ?CLEAN_TABLE_MESSAGE,
      [])
  }.

terminate(_,_) ->
  ets:delete(?MC_WORKER_PID_INFO_TAB_NAME).

%% These functions does not do anything as this server is just a holder of an ETS table
handle_cast(_Request, State) -> {noreply, State}.
handle_call(_Request, _From, State) -> {reply, {error, ignore}, State}.

handle_info({timeout,
  TimerRef,
  ?CLEAN_TABLE_MESSAGE},
    #{cleanup_timer_ref := TimerRef} = State) ->
  PidInfos = ets:tab2list(?MC_WORKER_PID_INFO_TAB_NAME),
  [delete_pid_if_dead(Pid) || {Pid, _} <- PidInfos],
  {noreply, start_cleanup_timer_update_state(State)};
handle_info(_, State) ->
  {noreply, State}.

delete_pid_if_dead(Pid) ->
  case erlang:is_process_alive(Pid) of
    true -> ok;
    false -> ets:delete(?MC_WORKER_PID_INFO_TAB_NAME, Pid)
  end.

get_info(MCWorkerPID) ->
  try
    case ets:lookup(?MC_WORKER_PID_INFO_TAB_NAME, MCWorkerPID) of
      [{MCWorkerPID, InfoMap}] ->
        {ok, InfoMap};
      [] ->
        not_found
    end
  catch
    _:_:_ ->
      not_found
  end.


set_info(MCWorkerPID, InfoMap) ->
  ets:insert(?MC_WORKER_PID_INFO_TAB_NAME, {MCWorkerPID, InfoMap}).

discard_info(MCWorkerPID) ->
  ets:delete(?MC_WORKER_PID_INFO_TAB_NAME, MCWorkerPID).

get_protocol_type(MCWorkerPID) ->
  case get_info(MCWorkerPID) of
    {ok, #{protocol_type := ProtocolType}} ->
      ProtocolType;
    _ ->
      %% Not found means that this library has been hot upgraded and the
      %% mc_worker process was created before the hot_upgrade so we use
      %% the legacy protocol as this was what existed before
      legacy
  end.

%% This process should be called from mc_worker processes to install their info
%% in the ?MC_WORKER_PID_INFO_TAB_NAME ETS table
install_mc_worker_info(Options, NetModule, Database, Opts) ->
  UseLegacyProtocol = maps:get(use_legacy_protocol, Opts),
  try
    ProtocolType = detect_protocol_type(UseLegacyProtocol, Options, NetModule, Database),
    mc_worker_pid_info:set_info(self(), #{protocol_type => ProtocolType}),
    ok
  catch
    What:Reason ->
      {error, {What, Reason}}
  end.

%% true - use the legacy message protocol
%% false - use the modern message protocol based on the op_msg opcode
%% auto - detect which message protocol to use based on the database
detect_protocol_type(true, _ConnectionOpts, _NetModule, _Database)  -> legacy;
detect_protocol_type(false, _ConnectionOpts, _NetModule, _Database) -> op_msg;
detect_protocol_type(auto, ConnectionOpts, NetModule, Database) ->
  %% Automatically detect which protocol to use. We send a `hello' command using
  %% the new protocol. If we have our connection closed by the server, it means
  %% it doesn't support the new protocol.
  %% See also:
  %% * https://github.com/mongodb/mongo/blob/5e494138af456f42381ad08748cc7fbc4ace7a60/src/mongo/base/error_codes.yml
  %% * https://www.mongodb.com/docs/manual/reference/command/hello/#mongodb-dbcommand-dbcmd.hello
  %% * https://www.mongodb.com/docs/manual/reference/mongodb-wire-protocol/#std-label-wire-op-msg
  {ok, Socket} = mc_worker_logic:connect(ConnectionOpts),
  Command = bson:fields({hello, 1}),
  Request = #op_msg_command{command_doc = Command, database = Database},
  try mc_connection_man:request_raw_no_parse(Socket, Database, Request, NetModule) of
    [{_, #op_msg_response{response_doc = #{<<"ok">> := _}}} | _] -> op_msg;
    _ErrorResponse -> legacy
  catch
    _:_ -> legacy
  after
    NetModule:close(Socket)
  end.
