% Thread-safe TCP connection to a MongoDB server with synchronous call and asynchronous send interface.
-module(mongo_connect).

-export_type ([host/0, connection/0, failure/0, dbconnection/0]).

-export([connect/1, close/1]). % API
-export ([call/3, send/2]). % for mongo_query and mongo_cursor

-behaviour (gen_server).
-export ([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-include_lib ("bson/include/bson_binary.hrl").

-type host() :: {inet:hostname() | inet:ip_address(), 0..65535}.
% Address and port where address may be ip address or its domain name

-type server(_) :: pid() | atom(). % Pid or register process name with parameterized state
-type reason() :: any().

-opaque connection() :: server (gen_tcp:socket()).
% Thread-safe, TCP connection to a MongoDB server.
% Passive raw binary socket.

-type failure() :: {connection_failure, connection(), reason()}.

-type dbconnection() :: {mongo_protocol:db(), connection()}.

-spec connect (host()) -> {ok, connection()} | {error, reason()}. % IO
% Create connection to given MongoDB server or return reason for connection failure.
connect (Host) -> gen_server:start_link (?MODULE, Host, []).

-spec close (connection()) -> ok. % IO
% Close connection.
close (Conn) -> gen_server:cast (Conn, stop).

-spec call (dbconnection(), [mongo_protocol:notice()], mongo_protocol:request()) -> mongo_protocol:reply(). % IO throws failure()
% Synchronous send and reply. Notices are sent right before request in single block. Exclusive access to connection during entire call.
call ({Db, Conn}, Notices, Request) ->
	{MessagesBin, RequestId} = messages_binary (Db, Notices ++ [Request]),
	case gen_server:call (Conn, {call, MessagesBin}) of
		{error, Reason} -> throw ({connection_failure, Conn, Reason});
		{ok, ReplyBin} ->
			{RequestId, Reply, <<>>} = mongo_protocol:get_reply (ReplyBin),
			Reply end. % ^ ResponseTo must match RequestId

-spec send (dbconnection(), [mongo_protocol:notice()]) -> ok. % IO
% Asynchronous send (no reply). Don't know if send succeeded. Exclusive access to the connection during send.
send ({Db, Conn}, Notices) ->
	{NoticesBin, _} = messages_binary (Db, Notices),
	gen_server:cast (Conn, {send, NoticesBin}).

-spec messages_binary (mongo_protocol:db(), [mongo_protocol:message()]) -> {binary(), mongo_protocol:requestid()}.
% Binary representation of messages
messages_binary (Db, Messages) ->
	Build = fun (Message, {Bin, _}) -> 
		RequestId = mongodb_app:next_requestid(),
		MBin = mongo_protocol:put_message (Db, Message, RequestId),
		{<<Bin /binary, ?put_int32 (byte_size (MBin) + 4), MBin /binary>>, RequestId} end,
	lists:foldl (Build, {<<>>, 0}, Messages).

% gen_server callbacks %

-spec init (host()) -> {ok, gen_tcp:socket()} | {stop, reason()}. % IO
% Connect to host or stop with connection failure
init ({Address, Port}) ->
	case gen_tcp:connect (Address, Port, [binary, {active, false}, {packet, 0}]) of
		{error, Reason} -> {stop, Reason};
		{ok, Socket} -> {ok, Socket} end.

-type tag() :: any(). % Unique tag

-spec handle_call ({call, binary()}, {pid(), tag()}, gen_tcp:socket()) -> {reply, {ok, binary()}, gen_tcp:socket()} | {stop, reason(), {err, reason()}, gen_tcp:socket()}. % IO
% Send request and wait and return reply
handle_call ({call, RequestBin}, _From, Socket) ->
	case gen_tcp:send (Socket, RequestBin) of
		{error, Reason} -> {stop, Reason, {error, Reason}, Socket};
		ok -> case gen_tcp:recv (Socket, 4) of
			{error, Reason} -> {stop, Reason, {error, Reason}, Socket};
			{ok, <<?get_int32 (N)>>} -> case gen_tcp:recv (Socket, N - 4) of
				{error, Reason} -> {stop, Reason, {error, Reason}, Socket};
				{ok, ReplyBin} -> {reply, {ok, ReplyBin}, Socket} end end end.

-spec handle_cast
	({send, binary()}, gen_tcp:socket()) -> {noreply, gen_tcp:socket()} | {stop, reason(), gen_tcp:socket()}; % IO
	(stop, gen_tcp:socket()) -> {stop, normal, gen_tcp:socket()}.
% Send notice
handle_cast ({send, NoticeBin}, Socket) ->
	case gen_tcp:send (Socket, NoticeBin) of
		{error, Reason} -> {stop, Reason, Socket};
		ok -> {noreply, Socket} end;
% Close connection
handle_cast (stop, Socket) -> {stop, normal, Socket}.

handle_info (_Info, Socket) -> {noreply, Socket}.

-spec terminate (reason(), gen_tcp:socket()) -> any(). % IO. Result ignored
terminate (_Reason, Socket) -> gen_tcp:close (Socket).

code_change (_OldVsn, State, _Extra) -> {ok, State}.
