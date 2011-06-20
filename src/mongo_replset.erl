%@doc Get connection to appropriate server in a replica set
-module (mongo_replset).

-export_type ([replset/0, rs_connection/0]).
-export ([connect/1, connect/2, primary/1, secondary_ok/1, close/1, is_closed/1]). % API

-type maybe(A) :: {A} | {}.
-type err_or(A) :: {ok, A} | {error, reason()}.
-type reason() :: any().

% -spec find (fun ((A) -> boolean()), [A]) -> maybe(A).
% return first element in list that satisfies predicate
% find (Pred, []) -> {};
% find (Pred, [A | Tail]) -> case Pred (A) of true -> {A}; false -> find (Pred, Tail) end.

-spec until_success ([A], fun ((A) -> B)) -> B. % EIO, fun EIO
%@doc Apply fun on each element until one doesn't fail. Fail if all fail or list is empty
until_success ([], _Fun) -> throw ([]);
until_success ([A | Tail], Fun) -> try Fun (A)
	catch Reason -> try until_success (Tail, Fun)
		catch Reasons -> throw ([Reason | Reasons]) end end.

-spec rotate (integer(), [A]) -> [A].
%@doc Move first N element of list to back of list
rotate (N, List) ->
	{Front, Back} = lists:split (N, List),
	Back ++ Front.

-type host() :: mongo_connect:host().
-type connection() :: mongo_connect:connection().

-type replset() :: {rs_name(), [host()]}.
% Identify replset. Hosts is just seed list, not necessarily all hosts in replica set
-type rs_name() :: bson:utf8().

-spec connect (replset()) -> rs_connection(). % IO
%@doc Create new cache of connections to replica set members starting with seed members. No connection attempted until primary or secondary_ok called.
connect (ReplSet) -> connect (ReplSet, infinity).

-spec connect (replset(), timeout()) -> rs_connection(). % IO
%@doc Create new cache of connections to replica set members starting with seed members. No connection attempted until primary or secondary_ok called. Timeout used for initial connection and every call.
connect ({ReplName, Hosts}, TimeoutMS) ->
	Dict = dict:from_list (lists:map (fun (Host) -> {mongo_connect:host_port (Host), {}} end, Hosts)),
	{rs_connection, ReplName, mvar:new (Dict), TimeoutMS}.

-opaque rs_connection() :: {rs_connection, rs_name(), mvar:mvar(connections()), timeout()}.
% Maintains set of connections to some if not all of the replica set members. Opaque except to mongo:connect_mode
% Type not opaque to mongo:connection_mode/2
-type connections() :: dict:dictionary (host(), maybe(connection())).
% All hosts listed in last member_info fetched are keys in dict. Value is {} if no attempt to connect to that host yet

-spec primary (rs_connection()) -> err_or(connection()). % IO
%@doc Return connection to current primary in replica set
primary (ReplConn) -> try
		MemberInfo = fetch_member_info (ReplConn),
		primary_conn (2, ReplConn, MemberInfo)
	of Conn -> {ok, Conn}
	catch Reason -> {error, Reason} end.

-spec secondary_ok (rs_connection()) -> err_or(connection()). % IO
%@doc Return connection to a current secondary in replica set or primary if none
secondary_ok (ReplConn) -> try
		{_Conn, Info} = fetch_member_info (ReplConn),
		Hosts = lists:map (fun mongo_connect:read_host/1, bson:at (hosts, Info)),
		R = random:uniform (length (Hosts)) - 1,
		secondary_ok_conn (ReplConn, rotate (R, Hosts))
	of Conn -> {ok, Conn}
	catch Reason -> {error, Reason} end.

-spec close (rs_connection()) -> ok. % IO
%@doc Close replset connection
close ({rs_connection, _, VConns, _}) ->
	CloseConn = fun (_, MCon, _) -> case MCon of {Con} -> mongo_connect:close (Con); {} -> ok end end,
	mvar:with (VConns, fun (Dict) -> dict:fold (CloseConn, ok, Dict) end),
	mvar:terminate (VConns).

-spec is_closed (rs_connection()) -> boolean(). % IO
%@doc Has replset connection been closed?
is_closed ({rs_connection, _, VConns, _}) -> mvar:is_terminated (VConns).

% EIO = IO that may throw error of any type

-type member_info() :: {connection(), bson:document()}.
% Result of isMaster query on a server connnection. Returned fields are: setName, ismaster, secondary, hosts, [primary]. primary only present when ismaster = false

-spec primary_conn (integer(), rs_connection(), member_info()) -> connection(). % EIO
%@doc Return connection to primary designated in member_info. Only chase primary pointer N times.
primary_conn (0, _ReplConn, MemberInfo) -> throw ({false_primary, MemberInfo});
primary_conn (Tries, ReplConn, {Conn, Info}) -> case bson:at (ismaster, Info) of
	true -> Conn;
	false -> case bson:lookup (primary, Info) of
		{HostString} ->
			MemberInfo = connect_member (ReplConn, mongo_connect:read_host (HostString)),
			primary_conn (Tries - 1, ReplConn, MemberInfo);
		{} -> throw ({no_primary, {Conn, Info}}) end end.

-spec secondary_ok_conn (rs_connection(), [host()]) -> connection(). % EIO
%@doc Return connection to a live secondaries in replica set, or primary if none
secondary_ok_conn (ReplConn, Hosts) -> try
		until_success (Hosts, fun (Host) ->
			{Conn, Info} = connect_member (ReplConn, Host),
			case bson:at (secondary, Info) of true -> Conn; false -> throw (not_secondary) end end)
	catch _ -> primary_conn (2, ReplConn, fetch_member_info (ReplConn)) end.

-spec fetch_member_info (rs_connection()) -> member_info(). % EIO
%@doc Retrieve isMaster info from a current known member in replica set. Update known list of members from fetched info.
fetch_member_info (ReplConn = {rs_connection, _ReplName, VConns, _}) ->
	OldHosts_ = dict:fetch_keys (mvar:read (VConns)),
	{Conn, Info} = until_success (OldHosts_, fun (Host) -> connect_member (ReplConn, Host) end),
	OldHosts = sets:from_list (OldHosts_),
	NewHosts = sets:from_list (lists:map (fun mongo_connect:read_host/1, bson:at (hosts, Info))),
	RemovedHosts = sets:subtract (OldHosts, NewHosts),
	AddedHosts = sets:subtract (NewHosts, OldHosts),
	mvar:modify_ (VConns, fun (Dict) ->
		Dict1 = sets:fold (fun remove_host/2, Dict, RemovedHosts),
		Dict2 = sets:fold (fun add_host/2, Dict1, AddedHosts),
		Dict2 end),
	case sets:is_element (mongo_connect:conn_host (Conn), RemovedHosts) of
		false -> {Conn, Info};
		true -> % Conn connected to member but under wrong name (eg. localhost instead of 127.0.0.1) so it was closed and removed because it did not match a host in isMaster info. Reconnect using correct name.
			Hosts = dict:fetch_keys (mvar:read (VConns)),
			until_success (Hosts, fun (Host) -> connect_member (ReplConn, Host) end) end.

add_host (Host, Dict) -> dict:store (Host, {}, Dict).

remove_host (Host, Dict) ->
	MConn = dict:fetch (Host, Dict),
	Dict1 = dict:erase (Host, Dict),
	case MConn of {Conn} -> mongo_connect:close (Conn); {} -> ok end,
	Dict1.

-spec connect_member (rs_connection(), host()) -> member_info(). % EIO
%@doc Connect to host and verify membership. Cache connection in rs_connection
connect_member ({rs_connection, ReplName, VConns, TimeoutMS}, Host) ->
	Conn = get_connection (VConns, Host, TimeoutMS),
	Info = try get_member_info (Conn) catch _ ->
		mongo_connect:close (Conn),
		Conn1 = get_connection (VConns, Host, TimeoutMS),
		get_member_info (Conn1) end,
	case bson:at (setName, Info) of
		ReplName -> {Conn, Info};
		_ ->
			mongo_connect:close (Conn),
			throw ({not_member, ReplName, Host, Info}) end.

get_connection (VConns, Host, TimeoutMS) -> mvar:modify (VConns, fun (Dict) ->
	case dict:find (Host, Dict) of
		{ok, {Conn}} -> case mongo_connect:is_closed (Conn) of
			false -> {Dict, Conn};
			true -> new_connection (Dict, Host, TimeoutMS) end;
		_ -> new_connection (Dict, Host, TimeoutMS) end end).

new_connection (Dict, Host, TimeoutMS) -> case mongo_connect:connect (Host, TimeoutMS) of
	{ok, Conn} -> {dict:store (Host, {Conn}, Dict), Conn};
	{error, Reason} -> throw ({cant_connect, Reason}) end.

get_member_info (Conn) -> mongo_query:command ({admin, Conn}, {isMaster, 1}, true).
