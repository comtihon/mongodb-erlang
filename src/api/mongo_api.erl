%%%-------------------------------------------------------------------
%%% @author tihon
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%% Api helper module. You can use it as an example for your own api
%%% to mongoc, as not all parameters are passed.
%%% @end
%%% Created : 19. Jan 2016 16:04
%%%-------------------------------------------------------------------
-module(mongo_api).
-author("tihon").

-include("mongoc.hrl").
-include("mongo_protocol.hrl").

%% API
-export([
  connect/4,
  insert/3,
  find/4, find/6,
  find_one/5, find_one/4,
  update/5,
  delete/3,
  count/4,
  ensure_index/3,
  disconnect/1]).

-spec connect(atom(), list(), proplists:proplist(), proplists:proplist()) -> {ok, pid()}.
connect(Type, Hosts, TopologyOptions, WorkerOptions) ->
  mongoc:connect({Type, Hosts}, TopologyOptions, WorkerOptions).

-spec insert(atom() | pid(), collection(), list() | map() | bson:document()) ->
  {{boolean(), map()}, list()}.
insert(Topology, Collection, Document) ->
  mongoc:transaction(Topology,
    fun(#{pool := Worker}) ->
      mc_worker_api:insert(Worker, Collection, Document)
    end,
    #{}).

-spec update(atom() | pid(), collection(), selector(), map(), map()) ->
  {boolean(), map()}.
update(Topology, Collection, Selector, Doc, Opts) ->
  Upsert = maps:get(upsert, Opts, false),
  MultiUpdate = maps:get(multi, Opts, false),
  mongoc:transaction(Topology,
    fun(#{pool := Worker}) ->
      mc_worker_api:update(Worker, Collection, Selector, Doc, Upsert, MultiUpdate)
    end, Opts).

-spec delete(atom() | pid(), collection(), selector()) ->
  {boolean(), map()}.
delete(Topology, Collection, Selector) ->
  mongoc:transaction(Topology,
    fun(#{pool := Worker}) ->
      mc_worker_api:delete(Worker, Collection, Selector)
    end,
    #{}).

-spec find(atom() | pid(), collection(), selector(), projector()) ->
  {ok, cursor()} | [].
find(Topology, Collection, Selector, Projector) ->
  find(Topology, Collection, Selector, Projector, 0, 0).

-spec find(atom() | pid(), collection(), selector(), projector(), integer(), integer()) ->
  {ok, cursor()} | [].
find(Topology, Collection, Selector, Projector, Skip, Batchsize) ->
  mongoc:transaction_query(Topology,
    fun(Conf = #{pool := Worker}) ->
      Query = mongoc:find_query(Conf, Collection, Selector, Projector, Skip, Batchsize),
      mc_worker_api:find(Worker, Query)
    end, #{}).

-spec find_one(atom() | pid(), collection(), selector(), projector()) ->
  map() | undefined.
find_one(Topology, Collection, Selector, Projector) ->
  find_one(Topology, Collection, Selector, Projector, 0).

-spec find_one(atom() | pid(), collection(), selector(), projector(), integer()) ->
  map() | undefined.
find_one(Topology, Collection, Selector, Projector, Skip) ->
  mongoc:transaction_query(Topology,
    fun(Conf = #{pool := Worker}) ->
      Query = mongoc:find_one_query(Conf, Collection, Selector, Projector, Skip),
      mc_worker_api:find_one(Worker, Query)
    end, #{}).

-spec count(atom() | pid(), collection(), selector(), integer()) -> integer().
count(Topology, Collection, Selector, Limit) ->
  mongoc:transaction_query(Topology,
    fun(Conf = #{pool := Worker}) ->
      Query = mongoc:count_query(Conf, Collection, Selector, Limit),
      mc_worker_api:count(Worker, Query)
    end,
    #{}).

%% @doc Creates index on collection according to given spec.
%%      The key specification is a bson documents with the following fields:
%%      key      :: bson document, for e.g. {field, 1, other, -1, location, 2d}, <strong>required</strong>
-spec ensure_index(pid() | atom(), collection(), bson:document()) -> ok | {error, any()}.
ensure_index(Topology, Coll, IndexSpec) ->
  mongoc:transaction(Topology,
    fun(#{pool := Worker}) ->
      mc_worker_api:ensure_index(Worker, Coll, IndexSpec)
    end, #{}).

-spec disconnect(atom() | pid()) -> ok.
disconnect(Topology) ->
  mongoc:disconnect(Topology).