-module(mc_worker).
-behaviour(gen_server).

-include("mongo_protocol.hrl").

-export([start_link/2]).
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

-type connection() :: pid().
-type database() :: atom().
-type write_mode() :: unsafe | safe | {safe, bson:document()}.
-type read_mode() :: master | slave_ok.
-type action(A) :: fun (() -> A).
-type service() :: {Host :: inet:hostname() | inet:ip_address(), Post :: 0..65535}.
-type options() :: [option()].
-type option() :: {timeout, timeout()} | {ssl, boolean()} | ssl | {database, database()} | {read_mode, read_mode()} | {write_mode, write_mode()}.

-spec start_link(service(), options()) -> {ok, pid()}.
start_link(Service, Options) ->
	gen_server:start_link(?MODULE, [Service, Options], []).

%% @hidden
init([{Host, Port}, Options]) ->
	{ok, Socket} = connect_to_database(Host, Port, Options),
	State = proplists:get_value(state, Options),
	process_flag(trap_exit, true),
	RequestStorage = ets:new(requests, [private]),  %TODO heir me!
	{ok, #state{socket = Socket, ets = RequestStorage, buffer = <<>>, conn_state = State}}.

%% @hidden
handle_call(NewState = #conn_state, _, State = #state{conn_state = OldState}) ->  % update state, return old
	{reply, {ok, OldState}, State#state{conn_state = NewState}};
handle_call(#ensure_index{collection = Coll, index_spec = IndexSpec}, _, State = #state{conn_state = ConnState}) -> % ensure index request with insert request
	Key = bson:at(key, IndexSpec),
	Defaults = {name, gen_index_name(Key), unique, false, dropDups, false},
	Index = bson:update(ns, mongo_protocol:dbcoll(ConnState#conn_state.database, Coll), bson:merge(IndexSpec, Defaults)),
	mc_connection_man:request(self(), {'system.indexes', Index}), % TODO what request type?  %TODO deadlock?
	{reply, ok, State};
handle_call(Request, _, State = #state{socket = Socket, conn_state = ConnState#conn_state{write_mode = unsafe}})
	when is_record(Request, insert); is_record(Request, update); is_record(Request, delete) -> % write unsafe requests
	{ok, _} = make_request(Socket, ConnState#conn_state.database, Request),
	{reply, ok, State};
handle_call(Request, From, State = #state{socket = Socket, conn_state = ConnState#conn_state{write_mode = SafeMode}, ets = Ets})
	when is_record(Request, insert); is_record(Request, update); is_record(Request, delete) -> % write safe requests
	Params = case SafeMode of safe -> {}; {safe, Param} -> Param end,

	{ok, _} = make_request(Socket, ConnState#conn_state.database, Request), % ordinary write request

	RespFun = fun({0, [Ack | _]}) ->
		case bson:lookup(err, Ack, undefined) of
			undefined -> gen_server:reply(From, ok);
			String ->
				case bson:at(code, Ack) of
					10058 -> gen_server:reply(From, {error, {not_master, 10058}});
					Code -> gen_server:reply(From, {error, {write_failure, Code, String}})
				end
		end
	end,
	{ok, Id} = make_request(Socket, ConnState#conn_state.database, #'query'{ % check-write read request
		batchsize = -1,
		collection = '$cmd',
		selector = bson:append({getlasterror, 1}, Params)
	}),
	true = ets:insert_new(Ets, {Id, RespFun}),  % save function, which will be called on response
	{reply, ok, State};
handle_call(Request, From, State = #state{socket = Socket, conn_state = ConnState, ets = Ets, conn_state = CS}) % read requests
	when is_record(Request, 'query'); is_record(Request, getmore) ->
	UpdReq = case is_record(Request, 'query') of
		         true -> Request#'query'{slaveok = CS#conn_state.read_mode =:= slave_ok};
		         false -> Request
	         end,
	{ok, Id} = make_request(Socket, ConnState#conn_state.database, UpdReq), %TODO cursor?
	inet:setopts(Socket, [{active, once}]),
	RespFun = fun(Response) -> gen_server:reply(From, Response) end,  % save function, which will be called on response
	true = ets:insert_new(Ets, {Id, RespFun}),
	{noreply, State};
handle_call({request, Request}, _, State = #state{socket = Socket, conn_state = ConnState}) -> % ordinary requests %TODO what are they?
	{ok, _} = make_request(Socket, ConnState#conn_state.database, Request),
	{reply, ok, State};
handle_call({stop, _}, _From, State) -> % stop request
	{stop, normal, ok, State}.

%% @hidden
handle_cast(_, State) ->
	{noreply, State}.

%% @hidden
handle_info({tcp, _Socket, Data}, State = #state{ets = Ets}) ->
	Buffer = <<(State#state.buffer)/binary, Data/binary>>,
	{Responses, Pending} = decode_responses(Buffer),
	process_responses(Responses, Ets),
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
encode_request(Database, Request) ->
	RequestId = mongo_id_server:request_id(),
	Payload = mongo_protocol:put_message(Database, Request),
	{<<(byte_size(Payload) + 4):32/little, Payload/binary>>, RequestId}.

%% @private
decode_responses(Data) ->
	decode_responses(Data, []).

%% @private
decode_responses(<<Length:32/signed-little, Data/binary>>, Acc) when byte_size(Data) >= (Length - 4) ->
	PayloadLength = Length - 4,
	<<Payload:PayloadLength/binary, Rest/binary>> = Data,
	{Id, Response, <<>>} = mongo_protocol:get_reply(Payload),
	decode_responses(Rest, [{Id, Response} | Acc]);
decode_responses(Data, Acc) ->
	{lists:reverse(Acc), Data}.

%% @private
process_responses(Responses, Ets) ->
	lists:foreach(
		fun({Id, Response}) ->
			case ets:lookup(Ets, Id) of
				[] -> % TODO: close any cursor that might be linked to this request ?
					ok;
				[{Id, Fun}] ->
					ets:delete(Ets, Id),
					catch Fun(Response) % call on-response function
			end
		end, Responses).

%% @private
connect_to_database(Port, Host, Options) ->
	Timeout = proplists:get_value(timeout, Options, infinity),
	gen_tcp:connect(Host, Port, [binary, {active, false}, {packet, raw}], Timeout).

%% @private
gen_index_name(KeyOrder) ->
	bson:doc_foldl(fun(Label, Order, Acc) ->
		<<Acc/binary, $_, (value_to_binary(Label))/binary, $_, (value_to_binary(Order))/binary>>
	end, <<"i">>, KeyOrder).

%% @private
value_to_binary(Value) when is_integer(Value) ->
	bson:utf8(integer_to_list(Value));
value_to_binary(Value) when is_atom(Value) ->
	atom_to_binary(Value, utf8);
value_to_binary(Value) when is_binary(Value) ->
	Value;
value_to_binary(_Value) ->
	<<>>.

%% @private
make_request(Socket, Database, Request) ->
	{Packet, Id} = encode_request(Database, Request),
	{gen_tcp:send(Socket, Packet), Id}.