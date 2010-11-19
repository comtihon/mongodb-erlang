% Connection to MongoDB server
-module (mongo_connect).

-export_type ([host/0, tcp_port/0]).
-export_type ([connection/0, connectionfailure/0, dbconnection/0]).

-export ([connect/2]). % API

-export ([call/3, send/2]). % for mongo_query and mongo_cursor

-include_lib ("bson/include/bson_binary.hrl").

% CIO means does IO and may throw connectionfailure()

-type connectionfailure() :: {connectionfailure, gen_tcp:socket() | {host(), tcp_port()}, any()}. % any reason

-opaque connection() :: var:mvar (gen_tcp:socket()).
% passive raw binary socket kept inside var for exclusive access

-type host() :: inet:hostname() | inet:ipaddress().
-type tcp_port() :: 0..65535.

-spec connect (host(), tcp_port()) -> connection(). % CIO
% Create connection to given mongodb server. Caller owns the connection, hence connection will close when caller terminates. Connection can also be closed by supplying it to var:terminate/1.
connect (Host, Port) -> case gen_tcp:connect (Host, Port, [binary, {active, false}, {packet, 0}]) of
	{error, Reason} -> throw ({connectionfailure, {Host, Port}, Reason});
	{ok, Socket} -> var:new (Socket, infinity, fun (Sock) -> gen_tcp:close (Sock) end) end.

-type dbconnection() :: {mongo_protocol:db(), connection()}.

-spec call (dbconnection(), [mongo_protocol:notice()], mongo_protocol:request()) -> mongo_protocol:reply(). % CIO
% Synchronous send and reply. Notices are sent write before request in single block. Exclusive access to connection during entire call.
call ({Db, Conn}, Notices, Request) ->
	Build = fun (Notice, Bin) ->
		RequestId = mongodb_app:next_requestid(),
		mongo_protocol:append_message (Bin, Db, Notice, RequestId) end,
	Bin0 = lists:foldl (Build, <<>>, Notices),
	RequestId = mongodb_app:next_requestid(),
	Bin1 = mongo_protocol:append_message (Bin0, Db, Request, RequestId),
	var:with (Conn, fun (Socket) ->
		tcp_send (Socket, Bin1),
		<<?get_int32 (N)>> = tcp_recv (Socket, 4),
		ReplyBin = tcp_recv (Socket, N - 4),
		{RequestId, Reply, <<>>} = mongo_protocol:get_reply (ReplyBin), % ResponseTo == RequestId
		Reply end).

-spec send (dbconnection(), [mongo_protocol:notice()]) -> ok. % CIO
% Asynchronous send (no reply). Exclusive access to the connection during send, ie. no other messages can get in between these notices.
send ({Db, Conn}, Notices) ->
	NotBin = << <<?put_int32 (byte_size (MBin) + 4), MBin /binary>> ||
		Mess <- Notices,
		MBin = mongo_protocol:put_message (Db, Mess, mongodb_app:next_requestid()) >>,
	var:with (Conn, fun (Socket) -> tcp_send (Socket, NotBin) end).

-spec tcp_send (gen_tcp:socket(), binary()) -> ok. % CIO
tcp_send (Socket, Binary) -> case gen_tcp:send (Socket, Binary) of
	{error, Reason} -> gen_tcp:close (Socket), throw ({connectionfailure, Socket, Reason});
	ok -> ok end.

-spec tcp_recv (gen_tcp:socket(), integer()) -> binary(). % CIO
tcp_recv (Socket, Size) -> case gen_tcp:recv (Socket, Size) of
	{error, Reason} -> gen_tcp:close (Socket), throw ({connectionfailure, Socket, Reason});
	{ok, Binary} -> Binary end.
