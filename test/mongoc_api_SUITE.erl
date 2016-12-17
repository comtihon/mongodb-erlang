-module(mongoc_api_SUITE).
-author("tihon").

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

all() ->
  [
    ensure_index_test,
    count_test
  ].

init_per_suite(Config) ->
  application:ensure_all_started(mongodb),
  [{database, test} | Config].

end_per_suite(_Config) ->
  ok.

init_per_testcase(Case, Config) ->
  [{collection, mc_test_utils:collection(Case)} | Config].

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

count_test(Config) ->
  Collection = ?config(collection, Config),

  {ok, Pid} = mongo_api:connect(single, ["localhost:27017"], [], [{database, <<"test">>}]),

  {{true, #{<<"n">> := 4}}, _} = mongo_api:insert(Pid, Collection, [
    {<<"name">>, <<"Yankees">>, <<"home">>, {<<"city">>, <<"New York">>, <<"state">>, <<"NY">>}, <<"league">>, <<"American">>},
    {<<"name">>, <<"Mets">>, <<"home">>, {<<"city">>, <<"New York">>, <<"state">>, <<"NY">>}, <<"league">>, <<"National">>},
    {<<"name">>, <<"Phillies">>, <<"home">>, {<<"city">>, <<"Philadelphia">>, <<"state">>, <<"PA">>}, <<"league">>, <<"National">>},
    {<<"name">>, <<"Red Sox">>, <<"home">>, {<<"city">>, <<"Boston">>, <<"state">>, <<"MA">>}, <<"league">>, <<"American">>}
  ]),

  N = mongo_api:count(Pid, Collection, #{}, 17),
  ?assertEqual(4, N),
  ok.