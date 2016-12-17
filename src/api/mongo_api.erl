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

-include("mongo_protocol.hrl").

%% API
-export([
  connect/4,
  insert/3,
  find/5,
  update/5,
  delete/3,
  count/4,
  find_one/5,
  ensure_index/3]).

-spec connect(atom(), list(), proplists:proplist(), proplists:proplist()) -> {ok, pid()}.
connect(Type, Hosts, TopologyOptions, WorkerOptions) ->
  mongoc:connect({Type, Hosts}, TopologyOptions, WorkerOptions).

-spec insert(atom() | pid(), binary(), list() | map() | bson:document()) ->
  {{boolean(), map()}, list()}.
insert(Topology, Collection, Document) ->
  mongoc:transaction(Topology,
    fun(#{pool := Worker}) ->
      mc_worker_api:insert(Worker, Collection, Document)
    end,
    #{rp_mode => primary}).

-spec update(atom() | pid(), binary(), mc_worker_api:selector(), map(), map()) ->
  {boolean(), map()}.
update(Topology, Collection, Selector, Doc, Opts) ->
  Upsert = maps:get(upsert, Opts, false),
  MultiUpdate = maps:get(multi, Opts, false),
  mongoc:transaction(Topology,
    fun(#{pool := Worker}) ->
      mc_worker_api:update(Worker, Collection, Selector, Doc, Upsert, MultiUpdate)
    end, Opts).

-spec delete(atom() | pid(), binary(), mc_worker_api:selector()) ->
  {boolean(), map()}.
delete(Topology, Collection, Selector) ->
  mongoc:transaction(Topology,
    fun(#{pool := Worker}) ->
      mc_worker_api:delete(Worker, Collection, Selector)
    end,
    #{rp_mode => primary}).

-spec find(atom() | pid(), binary(), mc_worker_api:selector(), mc_worker_api:projector(), integer() | infinity) ->
  mc_worker_api:cursor().
find(Topology, Collection, Selector, Projector, _) ->
  mongoc:transaction_query(Topology,
    fun(Conf) -> mongoc:find(Conf, Collection, Selector, Projector, 0, 0) end, []).

-spec find_one(atom() | pid(), binary(), mc_worker_api:selector(), mc_worker_api:projector(), integer() | infinity) ->
  mc_worker_api:cursor().
find_one(Topology, Collection, Selector, Projector, _) ->
  mongoc:transaction_query(Topology,
    fun(Conf) -> mongoc:find_one(Conf, Collection, Selector, Projector, 0) end, []).

-spec count(atom() | pid(), binary(), mc_worker_api:selector(), integer()) -> integer().
count(Topology, Collection, Selector, Limit) ->
  mongoc:transaction_query(Topology,
    fun(#{pool := Worker}) ->
      mc_worker_api:count(Worker, Collection, Selector, #{limit => Limit})
    end,
    #{rp_mode => primary}).

%% @doc Creates index on collection according to given spec.
%%      The key specification is a bson documents with the following fields:
%%      key      :: bson document, for e.g. {field, 1, other, -1, location, 2d}, <strong>required</strong>
%%      name     :: bson:utf8()
%%      unique   :: boolean()
%%      dropDups :: boolean()
ensure_index(Topology, Coll, IndexSpec) ->
  mongoc:transaction(Topology,
    fun(#{pool := Worker}) ->
      mc_connection_man:request_worker(Worker, #ensure_index{collection = Coll, index_spec = IndexSpec})
    end, #{rp_mode => primary}).