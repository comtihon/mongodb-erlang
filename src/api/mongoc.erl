%%%-------------------------------------------------------------------
%%% @author Alexander Hudich (alttagil@gmail.com)
%%% @doc
%%% Client for a MongoDB instance, a replica set, or a set of mongoses.
%%% @end
%%%-------------------------------------------------------------------
-module(mongoc).
-author("alttagil@gmail.com").

-include("mongoc.hrl").
-include("mongo_protocol.hrl").

-export([
  connect/3,
  disconnect/1,
  transaction_query/2,
  transaction_query/3,
  transaction_query/4,
  transaction/2,
  transaction/3,
  transaction/4,
  status/1,
  append_read_preference/2,
  find_query/6,
  count_query/4,
  find_one_query/5]).


%% @doc Creates new topology discoverer, return its pid
-spec connect(seed(), connectoptions(), workeroptions()) ->
  {ok, Pid :: pid()} | ignore | {error, Reason :: term()}.
connect(Seeds, Options, WorkerOptions) ->
  ok = mc_pool_sup:ensure_started(),
  mc_topology:start_link(Seeds, Options, WorkerOptions).

-spec disconnect(pid()) -> ok.
disconnect(Topology) ->
  mc_topology:disconnect(Topology).

-spec status(pid() | atom()) -> {atom(), integer(), integer(), integer()}.
status(Topology) ->
  Res = mc_topology:get_pool(Topology, []),
  {ok, #{pool := Pid}} = Res,
  poolboy:status(Pid).

-spec transaction(pid() | atom(), fun()) -> any().
transaction(Topology, Transaction) ->
  transaction(Topology, Transaction, #{}, ?TRANSACTION_TIMEOUT).

-spec transaction(pid() | atom(), fun(), map()) -> any().
transaction(Topology, Transaction, Options) ->
  transaction(Topology, Transaction, Options, ?TRANSACTION_TIMEOUT).

%% @doc Get worker from pool and run transaction with it. Suitable for command transactions
-spec transaction(pid() | atom(), fun(), map(), integer() | infinity) -> any().
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
          {error, not_master};
        _:R ->
          mc_topology:update_topology(Topology),
          {error, R}
      end;
    Error ->
      Error
  end.

%% @doc Get worker from pool and run transaction with additioanl query options on it. Suitable for read transactions
-spec transaction_query(pid() | atom(), fun()) -> any().
transaction_query(Topology, Transaction) ->
  transaction_query(Topology, Transaction, #{}).

-spec transaction_query(pid() | atom(), fun(), map()) -> any().
transaction_query(Topology, Transaction, Options) ->
  transaction_query(Topology, Transaction, Options, ?TRANSACTION_TIMEOUT).

-spec transaction_query(pid() | atom(), fun(), map(), integer() | infinity) -> any().
transaction_query(Topology, Transaction, Options, Timeout) ->
  case mc_topology:get_pool(Topology, Options) of
    {ok, Pool = #{pool := C}} ->
      poolboy:transaction(C, fun(Worker) -> Transaction(Pool#{pool => Worker}) end, Timeout);
    Error ->
      Error
  end.

-spec find_one_query(map(), collection(), selector(), projector(), integer()) -> query().
find_one_query(#{server_type := ServerType, read_preference := RPrefs}, Coll, Selector, Projector, Skip) ->
  Q = #'query'{
    collection = Coll,
    selector = Selector,
    projector = Projector,
    skip = Skip
  },
  mongos_query_transform(ServerType, Q, RPrefs).

-spec find_query(map(), collection(), selector(), projector(), integer(), integer()) -> query().
find_query(#{server_type := ServerType, read_preference := RPrefs},
    Coll, Selector, Projector, Skip, BatchSize) ->
  Q = #'query'{
    collection = Coll,
    selector = Selector,
    projector = Projector,
    skip = Skip,
    batchsize = BatchSize
  },
  mongos_query_transform(ServerType, Q, RPrefs).

-spec count_query(map(), collection(), selector(), integer()) -> query().
count_query(#{server_type := ServerType, read_preference := RPrefs}, Coll, Selector, Limit) when Limit =< 0 ->
  Command = {<<"count">>, mc_utils:value_to_binary(Coll), <<"query">>, Selector},
  Q = #'query'{
    collection = <<"$cmd">>,
    selector = Command
  },
  mongos_query_transform(ServerType, Q, RPrefs);
count_query(#{server_type := ServerType, read_preference := RPrefs}, Coll, Selector, Limit) ->
  Command =
    {<<"count">>, mc_utils:value_to_binary(Coll), <<"query">>, Selector, <<"limit">>, Limit},
  Q = #'query'{
    collection = <<"$cmd">>,
    selector = Command
  },
  mongos_query_transform(ServerType, Q, RPrefs).

-spec append_read_preference(selector(), map()) -> selector().
append_read_preference(Selector = #{<<"$query">> := _}, RP) ->
  Selector#{<<"$readPreference">> => RP};
append_read_preference(Selector, RP) when is_tuple(Selector) andalso element(1, Selector) =:= <<"count">> ->
  bson:append(Selector, {<<"$readPreference">>, RP});
append_read_preference(Selector, RP) when is_tuple(Selector) andalso element(1, Selector) =:= <<"$query">> ->
  bson:append(Selector, {<<"$readPreference">>, RP});
append_read_preference(Selector, RP) ->
  #{<<"$query">> => Selector, <<"$readPreference">> => RP}.


%%%===================================================================
%%% Internal functions
%%%===================================================================

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