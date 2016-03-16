%%%-------------------------------------------------------------------
%%% @author Alexander Hudich (alttagil@gmail.com)
%%% @doc
%%% Client for a MongoDB instance, a replica set, or a set of mongoses.
%%% @end
%%%-------------------------------------------------------------------
-module(mongoc).
-author("alttagil@gmail.com").

-include("mongo_protocol.hrl").

-export([
  connect/3,
  disconnect/1,
  command/4,
  find_one/5,
  find/6,
  find/3,
  count/5,
  transaction_query/2,
  transaction_query/3,
  transaction_query/4,
  transaction/2,
  transaction/3,
  transaction/4,
  status/1]).

-define(TRANSACTION_TIMEOUT, 5000).


-type readmode() :: primary | secondary | primaryPreferred | secondaryPreferred | nearest.
-type host() :: list().
-type seed() :: host()
| {rs, binary(), [host()]}
| {single, host()}
| {unknown, [host()]}
| {sharded, [host()]}.
-type connectoptions() :: [coption()].
-type coption() :: {name, atom()}
|{register, atom()}
|{pool_size, integer()}
|{max_overflow, integer()}
|{localThresholdMS, integer()}
|{connectTimeoutMS, integer()}
|{socketTimeoutMS, integer()}
|{serverSelectionTimeoutMS, integer()}
|{waitQueueTimeoutMS, integer()}
|{heartbeatFrequencyMS, integer()}
|{minHeartbeatFrequencyMS, integer()}
|{rp_mode, readmode()}
|{rp_tags, list()}.
-type workeroptions() :: [woption()].
-type woption() :: {database, database()}
| {login, binary()}
| {password, binary()}
| {w_mode, mc_worker_api:write_mode()}.
-type readprefs() :: [readpref()].
-type readpref() :: {rp_mode, readmode()}
|{rp_tags, [tuple()]}.
-type reason() :: atom().


%% @doc Creates new topology discoverer, return its pid
-spec connect(seed(), connectoptions(), workeroptions()) -> {ok, Pid :: pid()} | ignore | {error, Reason :: term()}.
connect(Seeds, Options, WorkerOptions) ->
  application:ensure_started(poolboy),
  mc_pool_sup:start_link(),
  mc_topology:start_link(Seeds, Options, WorkerOptions).

-spec disconnect(pid()) -> ok.
disconnect(Topology) ->
  mc_topology:disconnect(Topology).

%% @doc Get worker from pool and run transaction with it. Suitable for all write transactions
-spec transaction(pid() | atom(), fun()) -> any().
transaction(Topology, Transaction) ->
  transaction(Topology, Transaction, ?TRANSACTION_TIMEOUT).

-spec transaction(pid() | atom(), fun(), integer() | infinity | proplists:proplist()) -> any().
transaction(Topology, Transaction, Timeout) when is_integer(Timeout); Timeout =:= infinity ->
  case mc_topology:get_pool(Topology, [{rp_mode, primary}]) of
    {ok, #{pool := C}} ->
      try poolboy:transaction(C, Transaction, Timeout)
      catch
        error:not_master ->
          mc_topology:update_topology(Topology),
          {error, not_master};
        error:{bad_query, {not_master, _}} ->
          mc_topology:update_topology(Topology),
          {error, not_master};
        _:R ->
          mc_topology:update_topology(Topology),
          {error, R}
      end;
    Error ->
      Error
  end;
transaction(Topology, Transaction, Options) ->
  transaction(Topology, Transaction, Options, ?TRANSACTION_TIMEOUT).

-spec status(pid() | atom()) -> {atom(), integer(), integer(), integer()}.
status(Topology) ->
  Res = mc_topology:get_pool(Topology, []),
  {ok, #{pool := Pid}} = Res,
  poolboy:status(Pid).


%% @doc Get worker from pool and run transaction with it. Suitable for command transactions
-spec transaction(pid() | atom(), fun(), proplists:proplist(), integer() | infinity) -> any().
transaction(Topology, Transaction, Options, Timeout) ->
  case mc_topology:get_pool(Topology, Options) of
    {ok, Pool = #{pool := C}} ->
      try poolboy:transaction(C, fun(Worker) -> Transaction(Pool#{pool => Worker}) end, Timeout)
      catch
        error:not_master ->
          mc_topology:update_topology(Topology),
          {error, not_master};
        error:{bad_query, {not_master, _}} ->
          mc_topology:update_topology(Topology),
          {error, not_master}
      end;
    Error ->
      Error
  end.

%% @doc Get worker from pool and run transaction with additioanl query options on it. Suitable for read transactions
transaction_query(Topology, Transaction) ->
  transaction_query(Topology, Transaction, []).

transaction_query(Topology, Transaction, Options) ->
  transaction_query(Topology, Transaction, Options, ?TRANSACTION_TIMEOUT).

-spec transaction_query(pid() | atom(), fun(), proplists:proplist(), integer() | infinity) -> any().
transaction_query(Topology, Transaction, Options, Timeout) ->
  case mc_topology:get_pool(Topology, Options) of
    {ok, Pool = #{pool := C}} ->
      poolboy:transaction(C, fun(Worker) -> Transaction(Pool#{pool => Worker}) end, Timeout);
    Error ->
      Error
  end.

-spec find_one(map(), colldb(), mc_worker_api:selector(), mc_worker_api:projector(), integer()) -> map().
find_one(#{pool := Pool, server_type := ServerType, read_preference := RPrefs},
    Coll, Selector, Projector, Skip) ->
  Q = #'query'{
    collection = Coll,
    selector = Selector,
    projector = Projector,
    skip = Skip
  },
  mc_action_man:read_one(Pool, mongos_query_transform(ServerType, Q, RPrefs)).

%% @doc Returns projection of selected documents.
%%      Empty projection [] means full projection.
-spec find(map(), colldb(), mc_worker_api:selector()) -> mc_worker_api:cursor().
find(Pool, Coll, Selector) ->
  find(Pool, Coll, Selector, [], 0, 0).

-spec find(map(), colldb(), mc_worker_api:selector(), mc_worker_api:projector(), integer(), integer()) ->
  mc_worker_api:cursor().
find(#{pool := Pool, server_type := ServerType, read_preference := RPrefs},
    Coll, Selector, Projector, Skip, BatchSize) ->
  Q = #'query'{
    collection = Coll,
    selector = Selector,
    projector = Projector,
    skip = Skip,
    batchsize = BatchSize
  },
  mc_action_man:read(Pool, mongos_query_transform(ServerType, Q, RPrefs)).

%% @doc Count selected documents up to given max number; 0 means no max.
%%     Ie. stops counting when max is reached to save processing time.
-spec count(map(), colldb(), mc_worker_api:selector(), readprefs(), integer()) -> integer().
count(Pool, {Db, Coll}, Selector, Options, Limit) when Limit =< 0 ->
  {true, #{<<"n">> := N}} = command(Pool,
    {<<"count">>, mc_utils:value_to_binary(Coll), <<"query">>, Selector}, Options, Db),
  trunc(N);
count(Pool, {Db, Coll}, Selector, Options, Limit) ->
  {true, #{<<"n">> := N}} = command(Pool,
    {<<"count">>, mc_utils:value_to_binary(Coll), <<"query">>, Selector, <<"limit">>, Limit}, Options, Db),
  trunc(N); % Server returns count as float
count(Pool, Coll, Selector, Options, Limit) when Limit =< 0 ->
  {true, #{<<"n">> := N}} = command(Pool,
    {<<"count">>, mc_utils:value_to_binary(Coll), <<"query">>, Selector}, Options, undefined),
  trunc(N);
count(Pool, Coll, Selector, Options, Limit) ->
  {true, #{<<"n">> := N}} = command(Pool,
    {<<"count">>, mc_utils:value_to_binary(Coll), <<"query">>, Selector, <<"limit">>, Limit}, Options, undefined),
  trunc(N). % Server returns count as float

-spec command(map(), bson:document(), readprefs(), undefined | colldb()) ->
  {boolean(), bson:document()} | {error, reason()}. % Action
command(Pid, Command, Options, Db) when is_pid(Pid) ->
  case mc_topology:get_pool(Pid, Options) of
    {ok, Pool} -> command(Pool, Command, Options, Db);
    Error -> Error
  end;
command(#{pool := Pool, server_type := ServerType, read_preference := RPrefs}, Command, _, Db) ->
  Q = #'query'{
    collection = {Db, <<"$cmd">>},
    selector = Command
  },
  exec_command(Pool, mongos_query_transform(ServerType, Q, RPrefs)).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
exec_command(C, Command) ->
  Doc = mc_action_man:read_one(C, Command),
  mc_connection_man:process_reply(Doc, Command).

%% @private
mongos_query_transform(mongos, #'query'{selector = S} = Q, #{mode := primary}) ->
  Q#'query'{selector = S, slaveok = false, sok_overriden = true};
mongos_query_transform(mongos, #'query'{selector = S} = Q, #{mode := primaryPreferred, tags := []}) ->
  Q#'query'{selector = S, slaveok = true, sok_overriden = true};
mongos_query_transform(mongos, #'query'{selector = S} = Q, #{mode := primaryPreferred, tags := Tags}) ->
  Q#'query'{
    selector = append_read_preference(S, #{mode => <<"primaryPreferred">>, <<"tags">> => bson:document(Tags)}),
    slaveok = true,
    sok_overriden = true};
mongos_query_transform(mongos, #'query'{selector = S} = Q, #{mode := secondary, tags := []}) ->
  Q#'query'{
    selector = append_read_preference(S, #{mode => <<"secondary">>}),
    slaveok = true,
    sok_overriden = true};
mongos_query_transform(mongos, #'query'{selector = S} = Q, #{mode := secondary, tags := Tags}) ->
  Q#'query'{
    selector = append_read_preference(S, #{mode => <<"secondary">>, <<"tags">> => bson:document(Tags)}),
    slaveok = true,
    sok_overriden = true};
mongos_query_transform(mongos, #'query'{selector = S} = Q, #{mode := secondaryPreferred, tags := []}) ->
  Q#'query'{
    selector = append_read_preference(S, #{mode => <<"secondaryPreferred">>}),
    slaveok = true,
    sok_overriden = true};
mongos_query_transform(mongos, #'query'{selector = S} = Q, #{mode := secondaryPreferred, tags := Tags}) ->
  Q#'query'{
    selector = append_read_preference(S, #{mode => <<"secondaryPreferred">>, <<"tags">> => bson:document(Tags)}),
    slaveok = true,
    sok_overriden = true};
mongos_query_transform(mongos, #'query'{selector = S} = Q, #{mode := nearest, tags := []}) ->
  Q#'query'{
    selector = append_read_preference(S, #{mode => <<"nearest">>}),
    slaveok = true,
    sok_overriden = true};
mongos_query_transform(mongos, #'query'{selector = S} = Q, #{mode := nearest, tags := Tags}) ->
  Q#'query'{
    selector = append_read_preference(S, #{mode => <<"nearest">>, tags => bson:document(Tags)}),
    slaveok = true,
    sok_overriden = true};
mongos_query_transform(_, Q, #{mode := primary}) ->
  Q#'query'{slaveok = false, sok_overriden = true};
mongos_query_transform(_, Q, _) ->
  Q#'query'{slaveok = true, sok_overriden = true}.

%% @private
append_read_preference(Selector = #{<<"$query">> := _}, RP) ->
  Selector#{<<"$readPreference">> => RP};
append_read_preference(Selector, RP) ->
  #{<<"$query">> => Selector, <<"$readPreference">> => RP}.