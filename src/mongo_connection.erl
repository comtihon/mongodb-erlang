-module(mongo_connection).
-export([
	start_link/1,
	start_link/2,
	request/3,
	stop/1
]).
-export_type([
	connection/0,
	service/0,
	options/0
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
-type connection() :: pid().
-type service()    :: {Host :: inet:hostname() | inet:ip_address(), Post :: 0..65535}.
-type options()    :: [option()].
-type option()     :: {timeout, timeout()} | {ssl, boolean()} | ssl.


-spec start_link(service()) -> {ok, pid()}.
start_link(Service) ->
	start_link(Service, []).

-spec start_link(service(), options()) -> {ok, pid()}.
start_link(Service, Options) ->
	gen_server:start_link(?MODULE, [Service, Options], []).

%-spec request(pid(), mongo:database(), mongo_protocol:request()) -> ok | {mongo_protocol:cursor(), [bson:document()]}.
-spec request(pid(), atom(), term()) -> ok | {non_neg_integer(), [bson:document()]}.
request(Pid, Database, Request) ->
	case gen_server:call(Pid, {request, Database, Request}, infinity) of
		ok ->
			ok;
		#reply{cursornotfound = false, queryerror = false} = Reply ->
			{Reply#reply.cursorid, Reply#reply.documents};
		#reply{cursornotfound = false, queryerror = true} = Reply ->
			[Doc | _] = Reply#reply.documents,
			process_error(bson:at(code, Doc), Doc);
		#reply{cursornotfound = true, queryerror = false} = Reply ->
			erlang:error({bad_cursor, Reply#reply.cursorid})
	end.

-spec stop(pid()) -> ok.
stop(Pid) ->
	gen_server:call(Pid, stop).


%% @private
process_error(13435, _) ->
	erlang:error(not_master);
process_error(10057, _) ->
	erlang:error(unauthorized);
process_error(_, Doc) ->
	erlang:error({bad_query, Doc}).

%% @hidden
init([{Host, Port}, Options]) ->
	Timeout = proplists:get_value(timeout, Options, infinity),
	_SSL = proplists:get_bool(ssl, Options),
	case gen_tcp:connect(Host, Port, [binary, {active, false}, {packet, raw}], Timeout) of
		{ok, Socket} ->
			{ok, #state{socket = Socket, requests = [], buffer = <<>>}};
		{error, Reason} ->
			{stop, Reason}
	end.

%% @hidden
handle_call(stop, _From, State) ->
	{stop, normal, ok, State};

handle_call({request, Database, Request}, From, State) ->
	{Packet, Id} = encode_request(Database, Request),
	case gen_tcp:send(State#state.socket, Packet) of
		ok when is_record(Request, 'query'); is_record(Request, getmore) ->
			inet:setopts(State#state.socket, [{active, once}]),
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
		requests = case process_responses(Responses, State#state.requests) of
			[] ->
				[];
			Requests ->
				inet:setopts(State#state.socket, [{active, once}]),
				Requests
		end,
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
	RequestId = mongo_id_server:request_id(),
	Payload = mongo_protocol:put_message(Database, Request, RequestId),
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
