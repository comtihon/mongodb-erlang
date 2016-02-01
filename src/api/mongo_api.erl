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

%% API
-export([insert/4, find/5, update/5, delete/4, count/5, find_one/5, connect/4]).

-spec connect(atom(), list(), proplists:proplist(), proplists:proplist()) -> {ok, pid()}.
connect(Type, Hosts, TopologyOptions,  WorkerOptions) ->
  mongoc:connect({Type, Hosts}, TopologyOptions, WorkerOptions).

-spec insert(atom() | pid(), binary(), list() | map() | bson:document(), integer() | infinity) ->
{{boolean(), map()}, list()}.
insert(Topology, Collection, Document, TTL) ->
  mongoc:transaction(Topology, fun(Worker) -> mc_worker_api:insert(Worker, Collection, Document) end, TTL).

-spec update(atom() | pid(), binary(), mc_worker_api:selector(), map(), map()) ->
  {boolean(), map()}.
update(Topology, Collection, Selector, Doc, Opts) ->
  TTL = maps:get(ttl, Opts, infinity),
  Upsert = maps:get(upsert, Opts, false),
  MultiUpdate = maps:get(multi, Opts, false),
  mongoc:transaction(Topology,
    fun(Worker) ->
      mc_worker_api:update(Worker, Collection, Selector, Doc, Upsert, MultiUpdate)
    end, TTL).

-spec delete(atom() | pid(), binary(), mc_worker_api:selector(), integer() | infinity) ->
  {boolean(), map()}.
delete(Topology, Collection, Selector, TTL) ->
  mongoc:transaction(Topology, fun(Worker) -> mc_worker_api:delete(Worker, Collection, Selector) end, TTL).

-spec find(atom() | pid(), binary(), mc_worker_api:selector(), mc_worker_api:projector(), integer() | infinity) ->
  mc_worker_api:cursor().
find(Topology, Collection, Selector, Projector, TTL) ->
  mongoc:transaction_query(Topology,
    fun(Conf) -> mongoc:find(Conf, Collection, Selector, Projector, 0, 0) end, [], TTL).

-spec find_one(atom() | pid(), binary(), mc_worker_api:selector(), mc_worker_api:projector(), integer() | infinity) ->
  mc_worker_api:cursor().
find_one(Topology, Collection, Selector, Projector, TTL) ->
  mongoc:transaction_query(Topology,
    fun(Conf) -> mongoc:find_one(Conf, Collection, Selector, Projector, 0) end, [], TTL).

-spec count(atom() | pid(), binary(), mc_worker_api:selector(), map() | list(), integer() | infinity) -> integer().
count(Topology, Collection, Selector, Limit, TTL) ->
  mongoc:transaction(Topology, fun(Conf) -> mongoc:count(Conf, Collection, Selector, [], Limit) end, [], TTL).