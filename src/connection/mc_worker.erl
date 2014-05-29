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
	requests :: orddict:orddict(),  %TODO ets me?
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
	{ok, #state{socket = Socket, requests = [], buffer = <<>>, conn_state = State}}.

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
handle_call(Request, _, State = #state{socket = Socket, conn_state = ConnState#conn_state{write_mode = SafeMode}})
	when is_record(Request, insert); is_record(Request, update); is_record(Request, delete) -> % write safe requests
	Params = case SafeMode of safe -> {}; {safe, Param} -> Param end,

	{ok, _} = make_request(Socket, ConnState#conn_state.database, Request),
	{0, [Ack | _]}, %TODO
	{ok, _} = make_request(Socket, ConnState#conn_state.database, #'query'{
		batchsize = -1,
		collection = '$cmd',
		selector = bson:append({getlasterror, 1}, Params)
	}),

	case bson:lookup(err, Ack, undefined) of  %TODO
		undefined -> ok;
		String ->
			case bson:at(code, Ack) of
				10058 -> erlang:exit(not_master);
				Code -> erlang:exit({write_failure, Code, String})
			end
	end,
	{Packet, _Id} = encode_request(ConnState#conn_state.database, Request),
	ok = gen_tcp:send(Socket, Packet),
	{reply, ok, State};
handle_call(Request, From, State = #state{socket = Socket, conn_state = ConnState})
	when is_record(Request, 'query'); is_record(Request, getmore) ->                           % read requests
	{ok, Id} = make_request(Socket, ConnState#conn_state.database, Request),  %TODO how to get data from mongo?
	inet:setopts(Socket, [{active, once}]),
	{noreply, State#state{requests = [{Id, From} | State#state.requests]}}; % TODO saving big data in record? %TODO why only read?
handle_call({request, Request}, _, State = #state{socket = Socket, conn_state = ConnState}) -> % ordinary requests
	{ok, _} = make_request(Socket, ConnState#conn_state.database, Request),
	{reply, ok, State};
handle_call({stop, _}, _From, State) -> % stop request
	{stop, normal, ok, State}.

%% @hidden
handle_cast(_, State) ->
	{noreply, State}.

%% @hidden
handle_info({tcp, _Socket, Data}, State) ->
	Buffer = <<(State#state.buffer)/binary, Data/binary>>,
	{Responses, Pending} = decode_responses(Buffer),
	Request = case process_responses(Responses, State#state.requests) of
		          [] ->
			          [];
		          Requests ->
			          inet:setopts(State#state.socket, [{active, once}]),
			          Requests
	          end,
	{noreply, State#state{requests = Request, buffer = Pending}};
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
decode_responses(Data) -> %TODO move me to mc_connection_man?
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
process_responses([], Requests) -> Requests;
process_responses([{Id, Response} | RemainingResponses], Requests) ->
	case lists:keytake(Id, 1, Requests) of
		false ->
			% TODO: close any cursor that might be linked to this request ?
			process_responses(RemainingResponses, Requests);
		{value, {_Id, From}, RemainingRequests} ->
			gen_server:reply(From, Response),
			process_responses(RemainingResponses, RemainingRequests)
	end.

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

make_request(Socket, Database, Request) ->
	{Packet, Id} = encode_request(Database, Request),
	{gen_tcp:send(Socket, Packet), Id}.