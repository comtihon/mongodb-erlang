-module (mongo_sup).
-export ([
	start_link/0,
	gen_objectid/0,
	next_requestid/0
]).

-behaviour (supervisor).
-export ([
	init/1
]).


start_link() ->
	supervisor:start_link({local, ?MODULE}, ?MODULE, app).


%% @doc Fresh request id
-spec next_requestid () -> mongo_protocol:requestid().
next_requestid() ->
	ets:update_counter(?MODULE, requestid_counter, 1).

%% @doc Fresh object id
-spec gen_objectid () -> bson:objectid().
gen_objectid() ->
	Now = bson:unixtime_to_secs(bson:timenow()),
	MPid = ets:lookup_element(?MODULE, oid_machineprocid, 2),
	N = ets:update_counter(?MODULE, oid_counter, 1),
	bson:objectid(Now, MPid, N).

%% @hidden
init (app) ->
	ets:new(?MODULE, [named_table, public]),
	ets:insert(?MODULE, [
		{oid_counter, 0},
		{oid_machineprocid, oid_machineprocid()},
		{requestid_counter, 0}
	]),
	{ok, {
		{one_for_one, 5, 10}, [
		]
	}}.


%% @private
-spec oid_machineprocid () -> <<_:40>>.
oid_machineprocid() ->
	OSPid = list_to_integer(os:getpid()),
	{ok, Hostname} = inet:gethostname(),
	<<MachineId:3/binary, _/binary>> = erlang:md5(Hostname),
	<<MachineId:3/binary, OSPid:16/big>>.
