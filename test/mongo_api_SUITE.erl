-module(mongo_api_SUITE).
-author("tihon").

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

all() ->
  [
    ensure_index_test,
    count_test,
    find_one_test,
    find_test,
    upsert_and_update_test
  ].

init_per_suite(Config) ->
  application:ensure_all_started(mongodb),
  [{database, test} | Config].

end_per_suite(_Config) ->
  ok.

init_per_testcase(Case, Config) ->
  {ok, Pid} = mongo_api:connect(single, ["localhost:27017"],
    [{pool_size, 1}, {max_overflow, 0}], [{database, ?config(database, Config)}]),
  [{connection, Pid}, {collection, mc_test_utils:collection(Case)} | Config].

end_per_testcase(_Case, Config) ->
  Connection = ?config(connection, Config),
  Collection = ?config(collection, Config),
  mongo_api:delete(Connection, Collection, #{}),
  mongo_api:disconnect(Connection).

%% Tests
ensure_index_test(Config) ->
  Pid = ?config(connection, Config),
  Collection = ?config(collection, Config),
  ok = mongo_api:ensure_index(Pid, Collection, #{<<"key">> => {<<"cid">>, 1, <<"ts">>, 1}}),
  ok = mongo_api:ensure_index(Pid, Collection, {<<"key">>, {<<"z_first">>, 1, <<"a_last">>, 1}}),
  Config.

count_test(Config) ->
  Collection = ?config(collection, Config),
  Pid = ?config(connection, Config),
  {{true, #{<<"n">> := 4}}, _} = mongo_api:insert(Pid, Collection, [
    #{<<"name">> => <<"Yankees">>,
      <<"home">> => #{<<"city">> => <<"New York">>, <<"state">> => <<"NY">>},
      <<"league">> => <<"American">>},
    #{<<"name">> => <<"Mets">>,
      <<"home">> => #{<<"city">> => <<"New York">>, <<"state">> => <<"NY">>},
      <<"league">> => <<"National">>},
    #{<<"name">> => <<"Phillies">>,
      <<"home">> => #{<<"city">> => <<"Philadelphia">>, <<"state">> => <<"PA">>},
      <<"league">> => <<"National">>},
    #{<<"name">> => <<"Red Sox">>,
      <<"home">>=> #{<<"city">> => <<"Boston">>, <<"state">> => <<"MA">>},
      <<"league">> => <<"American">>}
  ]),
  N = mongo_api:count(Pid, Collection, #{}, 17),
  ?assertEqual(4, N),
  Config.

find_one_test(Config) ->
  Collection = ?config(collection, Config),
  Pid = ?config(connection, Config),
  undefined = mongo_api:find_one(Pid, Collection, #{}, #{<<"name">> => true}),
  {{true, #{<<"n">> := 4}}, _} = mongo_api:insert(Pid, Collection, [
    #{<<"name">> => <<"Yankees">>,
      <<"home">> => #{<<"city">> => <<"New York">>, <<"state">> => <<"NY">>},
      <<"league">> => <<"American">>},
    #{<<"name">> => <<"Mets">>,
      <<"home">> => #{<<"city">> => <<"New York">>, <<"state">> => <<"NY">>},
      <<"league">> => <<"National">>},
    #{<<"name">> => <<"Phillies">>,
      <<"home">> => #{<<"city">> => <<"Philadelphia">>, <<"state">> => <<"PA">>},
      <<"league">> => <<"National">>},
    #{<<"name">> => <<"Red Sox">>,
      <<"home">>=> #{<<"city">> => <<"Boston">>, <<"state">> => <<"MA">>},
      <<"league">> => <<"American">>}
  ]),
  #{<<"name">> := <<"Yankees">>} = mongo_api:find_one(Pid, Collection, #{}, #{<<"name">> => true}),
  undefined = mongo_api:find_one(Pid, Collection, #{<<"name">> => <<"Batman">>}, #{<<"name">> => true}),
  Config.

find_test(Config) ->
  Collection = ?config(collection, Config),
  Pid = ?config(connection, Config),
  [] = mongo_api:find(Pid, Collection, #{}, #{<<"name">> => true}),
  {{true, #{<<"n">> := 4}}, _} = mongo_api:insert(Pid, Collection, [
    #{<<"name">> => <<"Yankees">>,
      <<"home">> => #{<<"city">> => <<"New York">>, <<"state">> => <<"NY">>},
      <<"league">> => <<"American">>},
    #{<<"name">> => <<"Mets">>,
      <<"home">> => #{<<"city">> => <<"New York">>, <<"state">> => <<"NY">>},
      <<"league">> => <<"National">>},
    #{<<"name">> => <<"Phillies">>,
      <<"home">> => #{<<"city">> => <<"Philadelphia">>, <<"state">> => <<"PA">>},
      <<"league">> => <<"National">>},
    #{<<"name">> => <<"Red Sox">>,
      <<"home">>=> #{<<"city">> => <<"Boston">>, <<"state">> => <<"MA">>},
      <<"league">> => <<"American">>}
  ]),
  [] = mongo_api:find(Pid, Collection, #{<<"name">> => <<"Batman">>}, #{<<"name">> => true}),
  {ok, Cursor} =
    mongo_api:find(Pid, Collection, #{<<"home.city">> => <<"New York">>}, #{<<"name">> => true}),
  [
    #{<<"name">> := <<"Yankees">>},
    #{<<"name">> := <<"Mets">>}
  ] = mc_cursor:rest(Cursor),
  Config.

upsert_and_update_test(Config) ->
  Collection = ?config(collection, Config),
  Pid = ?config(connection, Config),
  {true, #{<<"n">> := 1}} = mongo_api:update(Pid, Collection, #{},
    #{<<"_id">> => 100,
      <<"sku">> => <<"abc123">>,
      <<"quantity">> => 250,
      <<"instock">> => true,
      <<"reorder">> => false,
      <<"details">> => #{<<"model">> => "14Q2", <<"make">> => "xyz"},
      <<"tags">> => ["apparel", "clothing"],
      <<"ratings">> => [#{<<"by">> => "ijk", <<"rating">> => 4}]},
    #{upsert => true}),
  {ok, Cursor} =
    mongo_api:find(Pid, Collection, #{<<"_id">> => 100}, #{}),
  [#{<<"_id">> := 100,
    <<"sku">> := <<"abc123">>,
    <<"quantity">> := 250,
    <<"instock">> := true,
    <<"reorder">> := false,
    <<"details">> := #{<<"model">> := "14Q2", <<"make">> := "xyz"},
    <<"tags">> := ["apparel", "clothing"],
    <<"ratings">> := [#{<<"by">> := "ijk", <<"rating">> := 4}]}] = mc_cursor:rest(Cursor),

  %update existent fields
  Command = #{
    <<"quantity">> => 500,
    <<"details">> => #{<<"model">> => "14Q3"},  %with flatten_map there is no need to specify non-changeble data
    <<"tags">> => ["coats", "outerwear", "clothing"]
  },
  {true, #{<<"n">> := 1}} = mongo_api:update(Pid, Collection,
    #{<<"_id">> => 100}, #{<<"$set">> => bson:flatten_map(Command)}, #{}),

  {ok, Cursor1} = mongo_api:find(Pid, Collection, #{<<"_id">> => 100}, #{}),

  [#{<<"_id">> := 100,
    <<"sku">> := <<"abc123">>,
    <<"quantity">> := 500,
    <<"instock">> := true,
    <<"reorder">> := false,
    <<"details">> := #{<<"model">> := "14Q3", <<"make">> := "xyz"},
    <<"tags">> := ["coats", "outerwear", "clothing"],
    <<"ratings">> := [#{<<"by">> := "ijk", <<"rating">> := 4}]}] = mc_cursor:rest(Cursor1),
  Config.