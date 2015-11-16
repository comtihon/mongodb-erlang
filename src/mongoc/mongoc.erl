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
  command/2,
  command/3,
  find_one/4,
  find/4,
  find/3,
  count/3,
  count/4,
  transaction_query/2,
  transaction_query/3,
  transaction/2, transaction/3]).


-type readmode() :: primary | secondary | primaryPreferred | secondaryPreferred | nearest.
-type host() :: list().
-type seed() :: host()
| {rs, binary(), [host()]}
| {single, host()}
| {unknown, [host()]}
| {sharded, [host()]}.
-type connectoptions() :: [coption()].
-type coption() :: {name, atom()}
|{minPoolSize, integer()}
|{maxPoolSize, integer()}
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
| {w_mode, mongo:write_mode()}.
-type readprefs() :: [readpref()].
-type readpref() :: {rp_mode, readmode()}
|{rp_tags, [tuple()]}.
-type reason() :: atom().


%% @doc Creates new topology discoverer, return its pid
-spec connect(seed(), connectoptions(), workeroptions()) -> {ok, Pid :: pid()} | ignore | {error, Reason :: term()}.
connect(Seeds, Options, WorkerOptions) ->
  mc_topology:start_link(Seeds, Options, WorkerOptions).

-spec disconnect(pid()) -> ok.
disconnect(Topology) ->
  mc_topology:disconnect(Topology).

%% @doc Get worker from pool and run transaction with it. Suitable for all write transactions
-spec transaction(pid(), fun()) -> any().
transaction(Topology, Transaction) ->
  case mc_topology:get_pool(Topology, [{rp_mode, primary}]) of  %TODO transaction in pool!
    {ok, #{pool := C}} ->
      try Transaction(C)
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
  end.

%% @doc Get worker from pool and run transaction with it. Suitable for command transacctions
transaction(Topology, Transaction, Options) ->
  case mc_topology:get_pool(Topology, Options) of
    {ok, Pool} ->
      try Transaction(Pool)
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
  case mc_topology:get_pool(Topology, Options) of   %TODO transaction in pool!
    {ok, Pool} ->
      Transaction(Pool);
    Error ->
      Error
  end.

-spec find_one(map(), colldb(), mongo:selector(), readprefs()) -> map().
find_one(#{pool := Pool, server_type := ServerType, readPreference := RPrefs}, Coll, Selector, Options) ->
  Projector = mc_utils:get_value(projector, Options, []),
  Skip = mc_utils:get_value(skip, Options, 0),
  Q = #'query'{
    collection = Coll,
    selector = Selector,
    projector = Projector,
    skip = Skip
  },
  mc_action_man:read_one(Pool, mongos_query_transform(ServerType, Q, RPrefs)).

%% @doc Returns projection of selected documents.
%%      Empty projection [] means full projection.
-spec find(map(), colldb(), mongo:selector()) -> mongo:cursor().
find(Pool, Coll, Selector) ->
  find(Pool, Coll, Selector, []).

-spec find(map(), colldb(), mongo:selector(), readprefs()) -> mongo:cursor().
find(#{pool := Pool, server_type := ServerType, readPreference := RPrefs}, Coll, Selector, Options) ->
  Projector = mc_utils:get_value(projector, Options, []),
  Skip = mc_utils:get_value(skip, Options, 0),
  BatchSize = mc_utils:get_value(batchsize, Options, 0),
  Q = #'query'{
    collection = Coll,
    selector = Selector,
    projector = Projector,
    skip = Skip,
    batchsize = BatchSize
  },
  mc_action_man:read(Pool, mongos_query_transform(ServerType, Q, RPrefs)).

%% @doc Counts selected documents
-spec count(pid(), colldb(), mongo:selector()) -> integer().
count(Pool, Coll, Selector) ->
  count(Pool, Coll, Selector, 0).


%% @doc Count selected documents up to given max number; 0 means no max.
%%     Ie. stops counting when max is reached to save processing time.
-spec count(pid(), colldb(), mongo:selector(), integer()) -> integer().
count(Pool, {Db, Coll}, Selector, Limit) when Limit =< 0 ->
  {true, #{<<"n">> := N}} = command(Pool,
    {<<"count">>, mc_utils:value_to_binary(Coll), <<"query">>, Selector}, Db),
  trunc(N);
count(Pool, {Db, Coll}, Selector, Limit) ->
  {true, #{<<"n">> := N}} = command(Pool,
    {<<"count">>, mc_utils:value_to_binary(Coll), <<"query">>, Selector, <<"limit">>, Limit}, Db),
  trunc(N); % Server returns count as float
count(Pool, Coll, Selector, Limit) when Limit =< 0 ->
  {true, #{<<"n">> := N}} = command(Pool,
    {<<"count">>, mc_utils:value_to_binary(Coll), <<"query">>, Selector}),
  trunc(N);
count(Pool, Coll, Selector, Limit) ->
  {true, #{<<"n">> := N}} = command(Pool,
    {<<"count">>, mc_utils:value_to_binary(Coll), <<"query">>, Selector, <<"limit">>, Limit}),
  trunc(N). % Server returns count as float

-spec command(pid(), bson:document()) -> {boolean(), bson:document()} | {error, reason()}. % Action
command(Pool, Command) ->
  command(Pool, Command, undefined).

-spec command(map(), bson:document(), readprefs()) -> {boolean(), bson:document()} | {error, reason()}. % Action
command(#{pool := Pool, server_type := ServerType, readPreference := RPrefs}, Command, Db) ->
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
mongos_query_transform(mongos, #'query'{} = Q, #{mode := primary}) ->
  Q#'query'{slaveok = false, sok_overriden = true};
mongos_query_transform(mongos, #'query'{selector = S} = Q, #{mode := secondary, tags := Tags}) ->
  Q#'query'{
    selector = bson:document([{'$query', S}, {'$readPreference', [{mode, secondary}, {tags, Tags}]}]),
    slaveok = true, sok_overriden = true};
mongos_query_transform(mongos, #'query'{selector = S} = Q, #{mode := primaryPreferred, tags := []}) ->
  Q#'query'{selector = S, slaveok = true, sok_overriden = true};
mongos_query_transform(mongos, #'query'{selector = S} = Q, #{mode := primaryPreferred, tags := Tags}) ->
  Q#'query'{
    selector = bson:document([{'$query', S}, {'$readPreference', [{mode, secondary}, {tags, Tags}]}]),
    slaveok = true, sok_overriden = true};
mongos_query_transform(mongos, #'query'{selector = S} = Q, #{mode := primaryPreffered, tags := Tags}) ->
  Q#'query'{
    selector = bson:document([{'$query', S}, {'$readPreference', [{mode, primaryPreffered}, {tags, Tags}]}]),
    slaveok = true, sok_overriden = true};
mongos_query_transform(mongos, #'query'{selector = S} = Q, #{mode := nearest, tags := Tags}) ->
  Q#'query'{
    selector = bson:document([{'$query', S}, {'$readPreference', [{mode, nearest}, {tags, Tags}]}]),
    slaveok = true, sok_overriden = true};
mongos_query_transform(_, Q, #{mode := primary}) ->
  Q#'query'{slaveok = false, sok_overriden = true};
mongos_query_transform(_, Q, _) ->
  Q#'query'{slaveok = true, sok_overriden = true}.
