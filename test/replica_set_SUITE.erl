-module(replica_set_SUITE).

%% API
-export([]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-include("mongo_protocol.hrl").

-compile(export_all).

-define(HOSTS, [<<"localhost:27018">>, <<"localhost:27019">>, <<"localhost:27020">>]).
-define(USER, <<"rs_user">>).
-define(PASSWORD, <<"rs_test">>).
-define(DB, <<"test">>).

all() ->
  [todo_test].

init_per_suite(Config) ->
  application:ensure_all_started(mongodb),
  case mongoc_connect() of
    {ok, Topology} ->
      mongoc:disconnect(Topology),
      Config;
    {error, _} ->
      {skipped, cannot_connect_rs_cluster}
  end.

end_per_suite(_Config) ->
  ok.

init_per_testcase(Case, Config) ->
  {ok, Connection} =
    mc_worker_api:connect([{database, ?config(database, Config)},
                           {login, <<"user">>},
                           {password, <<"test">>},
                           {w_mode, safe}]),
  [{connection, Connection}, {collection, mc_test_utils:collection(Case)} | Config].

end_per_testcase(_Case, Config) ->
  Connection = ?config(connection, Config),
  Collection = ?config(collection, Config),
  {true, _} = mc_worker_api:delete(Connection, Collection, #{}).

%% Tests
todo_test(Config) ->
  Config.

%% Private
mongoc_connect() ->
  mongoc:connect({rs, <<"rs0">>, ?HOSTS},
                 [{rp_mode, secondaryPreferred}],
                 [{database, ?DB}, {login, ?USER}, {password, ?PASSWORD}]).
