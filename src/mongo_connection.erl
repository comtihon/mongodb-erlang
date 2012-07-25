-module(mongo_connection).
-export([
	start_link/1,
	start_link/2,
	request/3,
	stop/1
]).

-behaviour(gen_server).
-export([
	init/1,
	handle_call/3,
	handle_cast/2,
	handle_info/2,
	terminate/2,
	code_change/3
]).

-record(state, {
	socket    :: gen_tcp:socket(),
	requests  :: orddict:orddict(),
	buffer    :: binary()
}).

-include("mongo_protocol.hrl").


start_link(Server) ->
	start_link(Server, []).

start_link(Server, Options) ->
	gen_server:start_link(?MODULE, [Server, Options], []).

request(Pid, Database, Request) ->
	gen_server:call(Pid, {request, Database, Request}, infinity).

stop(Pid) ->
	gen_server:call(Pid, stop).


%% @hidden
init([{Host, Port}, Options]) ->
	Timeout = proplists:get_value(timeout, Options, infinity),
	_SSL = proplists:get_bool(ssl, Options),
	case gen_tcp:connect(Host, Port, [binary, {active, true}, {packet, 0}, {nodelay, true}], Timeout) of
		{ok, Socket} ->
			{ok, #state{socket = Socket, requests = [], buffer = <<>>}};
		{error, Reason} ->
			{stop, Reason}
	end.

%% @hidden
handle_call(stop, _From, State) ->
	{stop, normal, State};

handle_call({request, Database, Request}, From, State) ->
	{Packet, Id} = encode_request(Database, Request),
	case gen_tcp:send(State#state.socket, Packet) of
		ok when is_record(Request, 'query'); is_record(Request, getmore) ->
			{noreply, State#state{requests = [{Id, From} | State#state.requests]}};
		ok ->
			{reply, ok, State};
		{error, Reason} ->
			{stop, Reason, State}
	end.

%% @hidden
handle_cast(_, State) ->
	{noreply, State}.

%% @hidden
handle_info({tcp, _Socket, Data}, State) ->
	Buffer = <<(State#state.buffer)/binary, Data/binary>>,
	{Responses, Pending} = decode_responses(Buffer),
	{noreply, State#state{
		requests = process_responses(Responses, State#state.requests),
		buffer = Pending
	}};

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
	RequestId = mongo_sup:next_requestid(),
	Payload = mongo_protocol:put_message(Database, Request, RequestId),
	{<<(byte_size(Payload) + 4):32/little, Payload/binary>>, RequestId}.

%% @private
decode_responses(Data) ->
	decode_responses(Data, []).

decode_responses(<<Length:32/signed-little, Payload:Length/binary, Rest/binary>>, Acc) ->
	{Id, Response, <<>>} = mongo_protocol:get_reply(Payload),
	decode_responses(Rest, [{Id, Response} | Acc]);
decode_responses(<<Rest/binary>>, Acc) ->
	{lists:reverse(Acc), Rest}.

%% @private
process_responses([], Requests) ->
	Requests;
process_responses([{Id, Response} | RemainingResponses], Requests) ->
	case lists:keytake(Id, 1, Requests) of
		false ->
			% TODO: close any cursor that might be linked to this request ?
			process_responses(RemainingResponses, Requests);
		{value, {_Id, From}, RemainingRequests} ->
			gen_server:reply(From, Response),
			process_responses(RemainingResponses, RemainingRequests)
	end.
