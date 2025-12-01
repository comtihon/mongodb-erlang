-module(batchsize_behavior_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%% CT callbacks
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).

%% Test cases
-export([
  test_positive_batchsize_returns_multiple_batches/1,
  test_negative_batchsize_acts_as_limit/1,
  test_zero_batchsize_uses_default/1,
  test_negative_batchsize_closes_cursor/1,
  test_batchsize_with_skip/1,
  test_negative_batchsize_with_projection/1,
  test_negative_batchsize_smaller_than_results/1,
  test_negative_batchsize_larger_than_results/1
]).

all() ->
  [
    test_positive_batchsize_returns_multiple_batches,
    test_negative_batchsize_acts_as_limit,
    test_zero_batchsize_uses_default,
    test_negative_batchsize_closes_cursor,
    test_batchsize_with_skip,
    test_negative_batchsize_with_projection,
    test_negative_batchsize_smaller_than_results,
    test_negative_batchsize_larger_than_results
  ].

init_per_suite(Config) ->
  application:ensure_all_started(mongodb),
  Config.

end_per_suite(_Config) ->
  ok.

init_per_testcase(TestCase, Config) ->
  %% Try to connect with and without auth
  Connection = case (catch mc_worker_api:connect([{database, <<"test">>}, 
                                                    {login, <<"user">>}, 
                                                    {password, <<"test">>}])) of
    {ok, Conn} ->
      ct:log("Connected with authentication"),
      Conn;
    _ ->
      ct:log("Connecting without authentication"),
      {ok, Conn} = mc_worker_api:connect([{database, <<"test">>}]),
      Conn
  end,
  
  Collection = test_collection_name(TestCase),
  
  %% Clean up any existing test data
  mc_worker_api:delete(Connection, Collection, #{}),
  
  %% Insert test documents
  Docs = [#{<<"_id">> => N, <<"value">> => N * 10} || N <- lists:seq(1, 20)],
  mc_worker_api:insert(Connection, Collection, Docs),
  
  [{connection, Connection}, {collection, Collection} | Config].

end_per_testcase(_TestCase, Config) ->
  Connection = ?config(connection, Config),
  Collection = ?config(collection, Config),
  mc_worker_api:delete(Connection, Collection, #{}),
  mc_worker_api:disconnect(Connection),
  ok.

test_collection_name(TestCase) ->
  list_to_binary("test_" ++ atom_to_list(TestCase)).

%% Test Cases

test_positive_batchsize_returns_multiple_batches(Config) ->
  Connection = ?config(connection, Config),
  Collection = ?config(collection, Config),
  
  %% Positive batchsize should allow cursor to fetch multiple batches
  {ok, Cursor} = mc_worker_api:find(Connection, Collection, #{}, #{batchsize => 5}),
  
  %% Should be able to get all 20 documents by iterating the cursor
  Docs = mc_cursor:rest(Cursor),
  ?assertEqual(20, length(Docs)),
  
  %% Cursor should be exhausted
  ?assertEqual({}, mc_cursor:next(Cursor)).

test_negative_batchsize_acts_as_limit(Config) ->
  Connection = ?config(connection, Config),
  Collection = ?config(collection, Config),
  
  %% Negative batchsize should act as a limit
  {ok, Cursor} = mc_worker_api:find(Connection, Collection, #{}, #{batchsize => -5}),
  
  %% Should only get 5 documents
  Docs = mc_cursor:rest(Cursor),
  ?assertEqual(5, length(Docs)),
  
  %% Verify we got the first 5 documents
  Values = [maps:get(<<"value">>, Doc) || Doc <- Docs],
  ?assertEqual([10, 20, 30, 40, 50], Values).

test_zero_batchsize_uses_default(Config) ->
  Connection = ?config(connection, Config),
  Collection = ?config(collection, Config),
  
  %% Zero batchsize should use default (101 for OP_MSG)
  {ok, Cursor} = mc_worker_api:find(Connection, Collection, #{}, #{batchsize => 0}),
  
  %% Should get all documents (20 < default 101)
  Docs = mc_cursor:rest(Cursor),
  ?assertEqual(20, length(Docs)).

test_negative_batchsize_closes_cursor(Config) ->
  Connection = ?config(connection, Config),
  Collection = ?config(collection, Config),
  
  %% Negative batchsize should close cursor after first batch
  {ok, Cursor} = mc_worker_api:find(Connection, Collection, #{}, #{batchsize => -10}),
  
  %% Get first batch
  Docs = mc_cursor:rest(Cursor),
  ?assertEqual(10, length(Docs)),
  
  %% Cursor should be closed (no more documents available)
  ?assertEqual({}, mc_cursor:next(Cursor)).

test_batchsize_with_skip(Config) ->
  Connection = ?config(connection, Config),
  Collection = ?config(collection, Config),
  
  %% Negative batchsize with skip should work correctly
  {ok, Cursor} = mc_worker_api:find(Connection, Collection, #{}, #{batchsize => -5, skip => 10}),
  
  %% Should get 5 documents starting from position 11
  Docs = mc_cursor:rest(Cursor),
  ?assertEqual(5, length(Docs)),
  
  %% Verify we got documents 11-15
  Values = [maps:get(<<"value">>, Doc) || Doc <- Docs],
  ?assertEqual([110, 120, 130, 140, 150], Values).

test_negative_batchsize_with_projection(Config) ->
  Connection = ?config(connection, Config),
  Collection = ?config(collection, Config),
  
  %% Negative batchsize with projection should work
  {ok, Cursor} = mc_worker_api:find(Connection, Collection, #{}, 
                                     #{batchsize => -3, projector => #{<<"value">> => 1, <<"_id">> => 0}}),
  
  Docs = mc_cursor:rest(Cursor),
  ?assertEqual(3, length(Docs)),
  
  %% Should only have 'value' field, not '_id'
  [FirstDoc | _] = Docs,
  ?assertEqual(false, maps:is_key(<<"_id">>, FirstDoc)),
  ?assertEqual(true, maps:is_key(<<"value">>, FirstDoc)).

test_negative_batchsize_smaller_than_results(Config) ->
  Connection = ?config(connection, Config),
  Collection = ?config(collection, Config),
  
  %% When negative batchsize is smaller than total results
  {ok, Cursor} = mc_worker_api:find(Connection, Collection, #{}, #{batchsize => -7}),
  
  Docs = mc_cursor:rest(Cursor),
  ?assertEqual(7, length(Docs)),
  
  %% Should get first 7 documents
  Values = [maps:get(<<"value">>, Doc) || Doc <- Docs],
  ?assertEqual([10, 20, 30, 40, 50, 60, 70], Values).

test_negative_batchsize_larger_than_results(Config) ->
  Connection = ?config(connection, Config),
  Collection = ?config(collection, Config),
  
  %% When negative batchsize is larger than total results
  {ok, Cursor} = mc_worker_api:find(Connection, Collection, #{}, #{batchsize => -100}),
  
  %% Should get all 20 documents (not 100)
  Docs = mc_cursor:rest(Cursor),
  ?assertEqual(20, length(Docs)).
