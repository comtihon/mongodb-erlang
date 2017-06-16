-module(mc_worker).
-behaviour(gen_server).

-include("mongo_protocol.hrl").

-define(WRITE(Req), is_record(Req, insert); is_record(Req, update); is_record(Req, delete)).
-define(READ(Req), is_record(Request, 'query'); is_record(Request, getmore)).

-export([start_link/1, disconnect/1, hibernate/1]).
-export([
  init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3]).

-record(state, {
  socket :: gen_tcp:socket() | ssl:sslsocket(),
  request_storage = #{} :: map(),
  buffer = <<>> :: binary(),
  conn_state :: conn_state(),
  hibernate_timer :: reference() | undefined,
  next_req_fun :: fun(),
  net_module :: ssl | gen_tcp
}).

-spec start_link(proplists:proplist()) -> {ok, pid()}.
start_link(Options) ->
  proc_lib:start_link(?MODULE, init, [Options]).

%% Make worker to go into hibernate. Any next call will wake it.
%% It should be done if you have problems with memory while fetching > 64B binaries from db.
-spec hibernate(pid()) -> ok.
hibernate(#{pool := Worker}) ->
  hibernate(Worker);
hibernate(Worker) ->
  gen_server:cast(Worker, hibernate).

%% halt worker, close tcp connection
disconnect(Worker) ->
  gen_server:cast(Worker, halt).

init(Options) ->
  case mc_worker_logic:connect_to_database(Options) of
    {ok, Socket} ->
      proc_lib:init_ack({ok, self()}),
      ConnState = form_state(Options),
      try_register(Options),
      NetModule = get_set_opts_module(Options),
      Login = mc_utils:get_value(login, Options),
      Password = mc_utils:get_value(password, Options),
      NextReqFun = mc_utils:get_value(next_req_fun, Options, fun() -> ok end),
      auth_if_credentials(Socket, ConnState, NetModule, Login, Password),
      gen_server:enter_loop(?MODULE, [],
        #state{socket = Socket,
          conn_state = ConnState,
          net_module = NetModule,
          next_req_fun = NextReqFun});
    Error ->
      proc_lib:init_ack(Error)
  end.

handle_call(NewState, _, State = #state{conn_state = OldState}) when is_record(NewState, conn_state) ->  % update state, return old
  {reply, {ok, OldState}, State#state{conn_state = NewState}};
handle_call(#ensure_index{collection = Coll, index_spec = IndexSpec}, _, State) -> % ensure index request with insert request
  #state{conn_state = ConnState, socket = Socket, net_module = NetModule} = State,
  Index = mc_worker_logic:ensure_index(IndexSpec, ConnState#conn_state.database, Coll),
  {ok, _, _} =
    mc_worker_logic:make_request(
      Socket,
      NetModule,
      ConnState#conn_state.database,
      #insert{collection = mc_worker_logic:update_dbcoll(Coll, <<"system.indexes">>), documents = [Index]}),
  {reply, ok, State};
handle_call(Request, From, State) when ?WRITE(Request) ->  % write requests (deprecated)
  process_write_request(Request, From, State);
handle_call(Request, From, State) when ?READ(Request) -> % read requests (and all through command)
  process_read_request(Request, From, State);
handle_call(Request, _, State = #state{socket = Socket, conn_state = ConnState, net_module = NetModule})
  when is_record(Request, killcursor) ->
  {ok, _, _} = mc_worker_logic:make_request(Socket, NetModule, ConnState#conn_state.database, Request),
  {reply, ok, State};
handle_call({stop, _}, _From, State) -> % stop request
  {stop, normal, ok, State}.

%% @hidden
handle_cast(halt, State) ->
  {stop, normal, State};
handle_cast(hibernate, State) ->
  {noreply, State, hibernate};
handle_cast(_, State) ->
  {noreply, State}.

%% @hidden
handle_info({Net, _Socket, Data}, State = #state{request_storage = RequestStorage}) when Net =:= tcp; Net =:= ssl ->
  Buffer = <<(State#state.buffer)/binary, Data/binary>>,
  {Responses, Pending} = mc_worker_logic:decode_responses(Buffer),
  UReqStor = mc_worker_logic:process_responses(Responses, RequestStorage),
  UState = need_hibernate(byte_size(Buffer), State),
  {noreply, UState#state{buffer = Pending, request_storage = UReqStor}};
handle_info({NetR, _Socket}, State) when NetR =:= tcp_closed; NetR =:= ssl_closed ->
  {stop, tcp_closed, State};
handle_info(hibernate, State) ->
  {noreply, State#state{hibernate_timer = undefined}, hibernate};
handle_info({NetR, _Socket, Reason}, State) when NetR =:= tcp_errror; NetR =:= ssl_error ->
  {stop, Reason, State}.

%% @hidden
terminate(_, State = #state{net_module = NetModule}) ->
  try NetModule:close(State#state.socket)
  catch
    _:_ -> ok
  end.

%% @hidden
code_change(_Old, State, _Extra) ->
  {ok, State}.

%% @private
process_read_request(Request, From, State) ->
  #state{socket = Socket,
    request_storage = RequestStorage,
    conn_state = CS,
    net_module = NetModule,
    next_req_fun = Next} = State,
  {UpdReq, Selector} = get_query_selector(Request, CS),
  {ok, PacketSize, Id} = mc_worker_logic:make_request(Socket, NetModule, CS#conn_state.database, UpdReq),
  UState = need_hibernate(PacketSize, State),
  case get_write_concern(Selector) of
    {<<"w">>, 0} -> %no concern request
      Next(),
      {reply, #reply{
        cursornotfound = false,
        queryerror = false,
        cursorid = 0,
        documents = [#{<<"ok">> => 1}]}, UState};
    _ ->  %ordinary request with response
      Next(),
      RespFun = mc_worker_logic:get_resp_fun(UpdReq, From),  % save function, which will be called on response
      URStorage = RequestStorage#{Id => RespFun},
      {noreply, UState#state{request_storage = URStorage}}
  end.

%% @deprecated
%% @private
process_write_request(Request, _, State = #state{conn_state = #conn_state{write_mode = unsafe, database = Db}}) ->
  #state{socket = Socket, net_module = NetModule} = State,
  {ok, PacketSize, _} = mc_worker_logic:make_request(Socket, NetModule, Db, Request),
  UState = need_hibernate(PacketSize, State),
  {reply, ok, UState};
process_write_request(Request, From, State = #state{conn_state = #conn_state{write_mode = Safe, database = Db}}) ->
  #state{socket = Socket, net_module = NetModule, request_storage = ReqStor} = State,
  Params = case Safe of safe -> {}; {safe, Param} -> Param end,
  ConfirmWrite =
    #'query'
    { % check-write read request
      batchsize = -1,
      collection = mc_worker_logic:update_dbcoll(mc_worker_logic:collection(Request), <<"$cmd">>),
      selector = bson:append({<<"getlasterror">>, 1}, Params)
    },
  {ok, PacketSize, Id} = mc_worker_logic:make_request(
    Socket, NetModule, Db, [Request, ConfirmWrite]), % ordinary write request
  RespFun = mc_worker_logic:get_resp_fun(Request, From),
  UReqStor = ReqStor#{Id => RespFun},  % save function, which will be called on response
  UState = need_hibernate(PacketSize, State#state{request_storage = UReqStor}),
  {noreply, UState}.

%% @private
need_hibernate(Pack, State) when Pack < 64 -> State;  %no need in hibernate
need_hibernate(_, State = #state{hibernate_timer = undefined}) ->
  TRef = erlang:send_after(1000, self(), hibernate),
  State#state{hibernate_timer = TRef};
need_hibernate(_, State) -> State.  %timer already started

%% @private
get_query_selector(Query = #query{selector = Selector, sok_overriden = true}, CS) ->
  {Query#'query'{slaveok = CS#conn_state.read_mode =:= slave_ok}, Selector};
get_query_selector(Query = #query{selector = Selector, sok_overriden = false}, _) ->
  {Query, Selector};
get_query_selector(GetMore, _) -> {GetMore, {}}.

%% @private
get_write_concern(#{<<"writeConcern">> := N}) -> N;
get_write_concern(Selector) when is_tuple(Selector) ->
  bson:lookup(<<"writeConcern">>, Selector);
get_write_concern(_) -> undefined.

%% @private
%% Parses proplist to record
form_state(Options) ->
  Database = mc_utils:get_value(database, Options, <<"admin">>),
  RMode = mc_utils:get_value(r_mode, Options, master),
  WMode = mc_utils:get_value(w_mode, Options, unsafe),
  #conn_state{database = Database, read_mode = RMode, write_mode = WMode}.

%% @private
%% Register this process if needed
try_register(Options) ->
  case lists:keyfind(register, 1, Options) of
    false -> ok;
    {_, Name} when is_atom(Name) -> register(Name, self());
    {_, RegFun} when is_function(RegFun) -> RegFun(self())
  end.

%% @private
get_set_opts_module(Options) ->
  case mc_utils:get_value(ssl, Options, false) of
    true -> ssl;
    false -> gen_tcp
  end.

%% @private
auth_if_credentials(_, _, _, Login, Password) when Login =:= undefined; Password =:= undefined ->
  ok;
auth_if_credentials(Socket, ConnState, NetModule, Login, Password) ->
  Version = mc_worker_logic:get_version(Socket, ConnState#conn_state.database, NetModule),
  mc_auth_logic:auth(Version, Socket, ConnState#conn_state.database, Login, Password, NetModule),
  ok.