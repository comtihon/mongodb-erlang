-module(mongo_id_server).

-behaviour(gen_server).

-export([
	request_id/0,
	object_id/0
]).

-export([
	start_link/0
]).

-export([
	init/1,
	handle_call/3,
	handle_cast/2,
	handle_info/2,
	terminate/2,
	code_change/3
]).

-define(MAX_INT32, 2147483647).

%% @doc Fresh request id
-spec request_id() -> pos_integer().
request_id() ->
	ets:update_counter(?MODULE, requestid_counter, {2, 1, ?MAX_INT32, 0}).

%% @doc Fresh object id
-spec object_id() -> bson:objectid().
object_id() ->
	Now = bson:unixtime_to_secs(bson:timenow()),
	MPid = ets:lookup_element(?MODULE, oid_machineprocid, 2),
	N = ets:update_counter(?MODULE, oid_counter, 1),
	bson:objectid(Now, MPid, N).


-spec start_link() -> {ok, pid()}.
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


%% @hidden
init([]) ->
	?MODULE = ets:new(?MODULE, [named_table, public, {write_concurrency, true}, {read_concurrency, true}]),
	ets:insert(?MODULE, [
		{oid_counter, 0},
		{oid_machineprocid, oid_machineprocid()},
		{requestid_counter, 0}
	]),
	{ok, undefined}.

handle_call(_, _From, State) ->
	{stop, 'bad-request', State}.

%% @hidden
handle_cast(_, State) ->
	{noreply, State}.

%% @hidden
handle_info(_, State) ->
	{noreply, State}.

%% @hidden
terminate(_, _State) ->
	ok.

%% @hidden
code_change(_Old, State, _Extra) ->
	{ok, State}.

%% @private
-spec oid_machineprocid() -> <<_:40>>.
oid_machineprocid() ->
	OSPid = list_to_integer(os:getpid()),
	{ok, Hostname} = inet:gethostname(),
	<<MachineId:3/binary, _/binary>> = erlang:md5(Hostname),
	<<MachineId:3/binary, OSPid:16/big>>.
