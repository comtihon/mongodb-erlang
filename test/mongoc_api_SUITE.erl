-module(mongoc_api_SUITE).
-author("tihon").

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

all() ->
  [
    ensure_index_test
  ].

init_per_suite(Config) ->
  application:ensure_all_started(mongodb),
  [{database, test} | Config].

end_per_suite(_Config) ->
  ok.

init_per_testcase(_, Config) ->
  Config.

end_per_testcase(_Case, Config) ->
  Connection = ?config(connection, Config),
  Collection = ?config(collection, Config),
  mc_worker_api:delete(Connection, Collection, {}).

%% Tests
ensure_index_test(Config) ->
  Collection = ?config(collection, Config),

  {ok, Pid} = mongoc:connect({single, "localhost:27017"}, [], [{database, <<"test">>}]),

  ok = mongo_api:ensure_index(Pid, Collection, #{<<"key">> => {<<"cid">>, 1, <<"ts">>, 1}}),
  Config.