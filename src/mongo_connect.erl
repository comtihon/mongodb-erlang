%@doc Thread-safe TCP connection to a MongoDB server with synchronous call and asynchronous send interface.
-module (mongo_connect).

-export_type ([host/0, connection/0, dbconnection/0, failure/0]).

-export ([host_port/1, read_host/1, show_host/1]).
-export ([connect/1, connect/2, conn_host/1, close/1, is_closed/1]).

-export ([call/3, send/2]). % for mongo_query and mongo_cursor

-include_lib ("bson/include/bson_binary.hrl").

-type host() :: {inet:hostname(), 0..65535} | inet:hostname().
% Hostname and port. Port defaults to 27017 when missing

-spec host_port (host()) -> host().
%@doc Port explicitly filled in with defaut if missing
host_port ({Hostname, Port}) -> {hostname_string (Hostname), Port};
host_port (Hostname) -> {hostname_string (Hostname), 27017}.

-spec hostname_string (inet:hostname()) -> string().
%@doc Convert possible hostname atom to string
hostname_string (Name) when is_atom (Name) -> atom_to_list (Name);
hostname_string (Name) -> Name.

-spec show_host (host()) -> bson:utf8().
%@doc UString representation of host, ie. "Hostname:Port"
show_host (Host) ->
	{Hostname, Port} = host_port (Host),
	bson:utf8 (Hostname ++ ":" ++ integer_to_list (Port)).

-spec read_host (bson:utf8()) -> host().
%@doc Interpret ustring as host, ie. "Hostname:Port" -> {Hostname, Port}
read_host (UString) -> case string:tokens (bson:str (UString), ":") of
	[Hostname] -> host_port (Hostname);
	[Hostname, Port] -> {Hostname, list_to_integer (Port)} end.

-type reason() :: any().

-opaque connection() :: {connection, host(), mvar:mvar (gen_tcp:socket()), timeout()}.
% Thread-safe, TCP connection to a MongoDB server.
% Passive raw binary socket.
% Type not opaque to mongo:connection_mode/2

-spec connect (host()) -> {ok, connection()} | {error, reason()}. % IO
%@doc Create connection to given MongoDB server or return reason for connection failure.
connect (Host) -> connect (Host, infinity).

-spec connect (host(), timeout()) -> {ok, connection()} | {error, reason()}. % IO
%@doc Create connection to given MongoDB server or return reason for connection failure. Timeout is used for initial connection and every call.
connect (Host, TimeoutMS) -> try mvar:create (fun () -> tcp_connect (host_port (Host), TimeoutMS) end, fun gen_tcp:close/1)
	of VSocket -> {ok, {connection, host_port (Host), VSocket, TimeoutMS}}
	catch Reason -> {error, Reason} end.

-spec conn_host (connection()) -> host().
%@doc Host this is connected to
conn_host ({connection, Host, _VSocket, _}) -> Host.

-spec close (connection()) -> ok. % IO
%@doc Close connection.
close ({connection, _Host, VSocket, _}) -> mvar:terminate (VSocket).

-spec is_closed (connection()) -> boolean(). % IO
%@doc Has connection been closed?
is_closed ({connection, _, VSocket, _}) -> mvar:is_terminated (VSocket).

-type dbconnection() :: {mongo_protocol:db(), connection()}.

-type failure() :: {connection_failure, connection(), reason()}.

-spec call (dbconnection(), [mongo_protocol:notice()], mongo_protocol:request()) -> mongo_protocol:reply(). % IO throws failure()
%@doc Synchronous send and reply. Notices are sent right before request in single block. Exclusive access to connection during entire call.
call ({Db, Conn = {connection, _Host, VSocket, TimeoutMS}}, Notices, Request) ->
	{MessagesBin, RequestId} = messages_binary (Db, Notices ++ [Request]),
	Call = fun (Socket) ->
		tcp_send (Socket, MessagesBin),
		<<?get_int32 (N)>> = tcp_recv (Socket, 4, TimeoutMS),
		tcp_recv (Socket, N-4, TimeoutMS) end,
	try mvar:with (VSocket, Call) of
		ReplyBin ->
			{RequestId, Reply, <<>>} = mongo_protocol:get_reply (ReplyBin),
			Reply  % ^ ResponseTo must match RequestId
	catch
		throw: Reason -> close (Conn), throw ({connection_failure, Conn, Reason});
		exit: {noproc, _} -> throw ({connection_failure, Conn, closed}) end.

-spec send (dbconnection(), [mongo_protocol:notice()]) -> ok. % IO throws failure()
%@doc Asynchronous send (no reply). Don't know if send succeeded. Exclusive access to the connection during send.
send ({Db, Conn = {connection, _Host, VSocket, _}}, Notices) ->
	{NoticesBin, _} = messages_binary (Db, Notices),
	Send = fun (Socket) -> tcp_send (Socket, NoticesBin) end,
	try mvar:with (VSocket, Send)
	catch
		throw: Reason -> close (Conn), throw ({connection_failure, Conn, Reason});
		exit: {noproc, _} -> throw ({connection_failure, Conn, closed}) end.

-spec messages_binary (mongo_protocol:db(), [mongo_protocol:message()]) -> {binary(), mongo_protocol:requestid()}.
%@doc Binary representation of messages
messages_binary (Db, Messages) ->
	Build = fun (Message, {Bin, _}) -> 
		RequestId = mongodb_app:next_requestid(),
		MBin = mongo_protocol:put_message (Db, Message, RequestId),
		{<<Bin /binary, ?put_int32 (byte_size (MBin) + 4), MBin /binary>>, RequestId} end,
	lists:foldl (Build, {<<>>, 0}, Messages).

% Util %

tcp_connect ({Hostname, Port}, TimeoutMS) -> case gen_tcp:connect (Hostname, Port, [binary, {active, false}, {packet, 0}], TimeoutMS) of
	{ok, Socket} -> Socket;
	{error, Reason} -> throw (Reason) end.

tcp_send (Socket, Binary) -> case gen_tcp:send (Socket, Binary) of
	ok -> ok;
	{error, Reason} -> throw (Reason) end.

tcp_recv (Socket, N, TimeoutMS) -> case gen_tcp:recv (Socket, N, TimeoutMS) of
	{ok, Binary} -> Binary;
	{error, Reason} -> throw (Reason) end.
