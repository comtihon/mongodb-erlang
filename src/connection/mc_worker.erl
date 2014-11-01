-module(mc_worker).
-behaviour(gen_server).

-include("mongo_protocol.hrl").

-export([start_link/2, start_link/1]).
-export([
  init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3
]).

-export_type([connection/0, service/0, options/0]).

-record(state, {
  socket :: gen_tcp:socket(),
  ets,
  buffer :: binary(),
  conn_state
}).

-spec start_link(service(), options()) -> {ok, pid()}.
start_link([Service, Options]) -> start_link(Service, Options).
start_link(Service, Options) ->
  gen_server:start_link(?MODULE, [Service, Options], []).

%% @hidden
init([{Host, Port, State}, Options]) ->
  {ok, Socket} = connect_to_database(Host, Port, Options),
  process_flag(trap_exit, true),
  RequestStorage = ets:new(requests, [private]),  %TODO move to dict in case of ets number limits
  case proplists:get_value(auth, Options) of
    {User, Password} ->
      spawn(mongo, auth, [self(), User, Password]); %TODO remove this spike, make auth consequentially
    _ -> ok
  end,
  {ok, #state{socket = Socket, ets = RequestStorage, buffer = <<>>, conn_state = State}}.

%% @hidden
handle_call(NewState = #conn_state{}, _, State = #state{conn_state = OldState}) ->  % update state, return old
  {reply, {ok, OldState}, State#state{conn_state = NewState}};
handle_call(#ensure_index{collection = Coll, index_spec = IndexSpec}, _, State = #state{conn_state = ConnState, socket = Socket}) -> % ensure index request with insert request
  Key = bson:at(key, IndexSpec),
  Defaults = {name, mc_worker_logic:gen_index_name(Key), unique, false, dropDups, false},
  Index = bson:update(ns, mongo_protocol:dbcoll(ConnState#conn_state.database, Coll), bson:merge(IndexSpec, Defaults)),
  {ok, _} = mc_worker_logic:make_request(Socket, ConnState#conn_state.database, #insert{collection = 'system.indexes', documents = [Index]}),
  {reply, ok, State};
handle_call(Request, From, State = #state{socket = Socket, conn_state = ConnState = #conn_state{}, ets = Ets})
  when is_record(Request, insert); is_record(Request, update); is_record(Request, delete) -> % write requests
  case ConnState#conn_state.write_mode of
    unsafe ->   %unsafe (just write)
      {ok, _} = mc_worker_logic:make_request(Socket, ConnState#conn_state.database, Request),
      {reply, ok, State};
    SafeMode -> %safe (write and check)
      Params = case SafeMode of safe -> {}; {safe, Param} -> Param end,
      ConfirmWrite = #'query'{ % check-write read request
        batchsize = -1,
        collection = '$cmd',
        selector = bson:append({getlasterror, 1}, Params)
      },
      {ok, Id} = mc_worker_logic:make_request(Socket, ConnState#conn_state.database, [Request, ConfirmWrite]), % ordinary write request
      inet:setopts(Socket, [{active, once}]),
      RespFun = mc_worker_logic:process_write_response(From),
      true = ets:insert_new(Ets, {Id, RespFun}), % save function, which will be called on response
      {noreply, State}
  end;
handle_call(Request, From, State = #state{socket = Socket, ets = Ets, conn_state = CS}) % read requests
  when is_record(Request, 'query'); is_record(Request, getmore) ->
  UpdReq = case is_record(Request, 'query') of
             true -> Request#'query'{slaveok = CS#conn_state.read_mode =:= slave_ok};
             false -> Request
           end,
  {ok, Id} = mc_worker_logic:make_request(Socket, CS#conn_state.database, UpdReq),
  inet:setopts(Socket, [{active, once}]),
  RespFun = fun(Response) -> gen_server:reply(From, Response) end,  % save function, which will be called on response
  true = ets:insert_new(Ets, {Id, RespFun}),
  {noreply, State};
handle_call(Request = #killcursor{}, _, State = #state{socket = Socket, conn_state = ConnState}) ->
  {ok, _} = mc_worker_logic:make_request(Socket, ConnState#conn_state.database, Request),
  {reply, ok, State};
handle_call({stop, _}, _From, State) -> % stop request
  {stop, normal, ok, State}.

%% @hidden
handle_cast(_, State) ->
  {noreply, State}.

%% @hidden
handle_info({tcp, _Socket, Data}, State = #state{ets = Ets}) ->
  Buffer = <<(State#state.buffer)/binary, Data/binary>>,
  {Responses, Pending} = mc_worker_logic:decode_responses(Buffer),
  mc_worker_logic:process_responses(Responses, Ets),
  case proplists:get_value(size, ets:info(Ets)) of
    0 -> ok;
    _ -> inet:setopts(State#state.socket, [{active, once}]) %TODO why not always active true?
  end,
  {noreply, State#state{buffer = Pending}};
handle_info({tcp_closed, _Socket}, State) ->
  {stop, tcp_closed, State};
handle_info({tcp_error, _Socket, Reason}, State) ->
  {stop, Reason, State}.

%% @hidden
terminate(_, State) ->
  catch gen_tcp:close(State#state.socket).

%% @hidden
code_change(_Old, State, _Extra) ->
  {ok, State}.

%% @private
connect_to_database(Host, Port, Options) ->
  Timeout = proplists:get_value(timeout, Options, infinity),
  gen_tcp:connect(Host, Port, [binary, {active, false}, {packet, raw}], Timeout).