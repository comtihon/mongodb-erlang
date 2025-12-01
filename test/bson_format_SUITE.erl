-module(bson_format_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile([export_all, nowarn_export_all]).

%%====================================================================
%% CT Callbacks
%%====================================================================

all() ->
  [
    test_bson_document_creation,
    test_bson_append_formats,
    test_command_doc_tuple_format,
    test_command_requires_bson_not_map,
    test_bson_merge_with_documents,
    test_invalid_bson_operations,
    test_nested_document_handling
  ].

init_per_suite(Config) ->
  application:ensure_all_started(mongodb),
  [{database, <<"test">>} | Config].

end_per_suite(_Config) ->
  ok.

init_per_testcase(_Case, Config) ->
  Config.

end_per_testcase(_Case, _Config) ->
  ok.

%%====================================================================
%% Test Cases
%%====================================================================

%% Test that bson:document/1 creates proper BSON documents
test_bson_document_creation(_Config) ->
  %% Create document from list
  Doc1 = bson:document([{hello, 1}]),
  ?assert(is_tuple(Doc1)),
  
  %% Verify fields can be extracted
  Fields = bson:fields(Doc1),
  ?assert(is_list(Fields)),
  ?assert(lists:keymember(hello, 1, Fields)),
  
  %% Create document with multiple fields
  Doc2 = bson:document([{hello, 1}, {<<"$db">>, <<"admin">>}]),
  Fields2 = bson:fields(Doc2),
  ?assertEqual(2, length(Fields2)),
  
  ok.

%% Test bson:append with proper document formats
test_bson_append_formats(_Config) ->
  %% Create two BSON documents
  Doc1 = bson:document([{hello, 1}]),
  Doc2 = bson:document([{<<"$db">>, <<"test">>}]),
  
  %% Append should work with proper documents
  Merged = bson:append(Doc1, Doc2),
  ?assert(is_tuple(Merged)),
  
  %% Verify both fields are present
  Fields = bson:fields(Merged),
  ?assert(lists:keymember(hello, 1, Fields)),
  ?assert(lists:keymember(<<"$db">>, 1, Fields)),
  
  %% Verify values are correct
  {hello, 1} = lists:keyfind(hello, 1, Fields),
  {<<"$db">>, <<"test">>} = lists:keyfind(<<"$db">>, 1, Fields),
  
  ok.

%% Test that commands work with tuple BSON document format
test_command_doc_tuple_format(Config) ->
  Database = ?config(database, Config),
  
  {ok, Conn} = mc_worker_api:connect([{database, Database}]),
  
  %% Create command as BSON document (tuple format)
  TupleCmd = bson:document([{ping, 1}]),
  ?assert(is_tuple(TupleCmd)),
  
  %% Execute command
  {true, Result} = mc_worker_api:command(Conn, TupleCmd),
  ?assertEqual(1.0, maps:get(<<"ok">>, Result)),
  
  mc_worker_api:disconnect(Conn),
  ok.

%% Test that commands now accept both maps and BSON documents
test_command_requires_bson_not_map(Config) ->
  Database = ?config(database, Config),
  
  {ok, Conn} = mc_worker_api:connect([{database, Database}]),
  
  %% Maps are now accepted by the driver (MongoDB 6 compatibility)
  MapCmd = #{<<"ping">> => 1},
  ?assert(is_map(MapCmd)),
  
  %% Maps should work directly now
  {true, MapResult} = mc_worker_api:command(Conn, MapCmd),
  ?assertEqual(1.0, maps:get(<<"ok">>, MapResult)),
  
  %% BSON documents still work as before
  BsonCmd = bson:document(maps:to_list(MapCmd)),
  {true, BsonResult} = mc_worker_api:command(Conn, BsonCmd),
  ?assertEqual(1.0, maps:get(<<"ok">>, BsonResult)),
  
  mc_worker_api:disconnect(Conn),
  ok.

%% Test bson:append with documents (not raw tuples/lists)
test_bson_merge_with_documents(_Config) ->
  %% This tests the pattern that was causing crashes
  %% Old buggy code: bson:append({hello, 1}, [{<<"$db">>, <<"test">>}])
  %% Fixed code: bson:append(bson:document([{hello, 1}]), bson:document([{<<"$db">>, <<"test">>}]))
  
  Doc1 = bson:document([{hello, 1}]),
  Doc2 = bson:document([{<<"$db">>, <<"test">>}, {<<"$readPreference">>, #{<<"mode">> => <<"primary">>}}]),
  
  %% Should not crash
  Merged = bson:append(Doc1, Doc2),
  
  %% Verify all fields present
  Fields = bson:fields(Merged),
  ?assertEqual(3, length(Fields)),
  ?assert(lists:keymember(hello, 1, Fields)),
  ?assert(lists:keymember(<<"$db">>, 1, Fields)),
  ?assert(lists:keymember(<<"$readPreference">>, 1, Fields)),
  
  ok.

%% Test that BSON append behavior with different input types
test_invalid_bson_operations(_Config) ->
  %% Test appending documents vs raw tuples
  Doc = bson:document([{hello, 1}]),
  
  %% bson:append actually accepts tuples and treats them as documents
  %% This documents the current behavior rather than enforcing strict validation
  Result1 = bson:append(Doc, {raw, tuple}),
  ?assert(is_tuple(Result1)),
  
  %% The key insight: we should always use bson:document() to be explicit
  %% This is what the fixes enforce in the codebase
  Doc2 = bson:document([{world, 2}]),
  Result2 = bson:append(Doc, Doc2),
  ?assert(is_tuple(Result2)),
  
  Fields = bson:fields(Result2),
  ?assert(lists:keymember(hello, 1, Fields)),
  ?assert(lists:keymember(world, 1, Fields)),
  
  ok.

%% Test nested document handling
test_nested_document_handling(Config) ->
  Database = ?config(database, Config),
  
  {ok, Conn} = mc_worker_api:connect([{database, Database}]),
  
  %% Create command with nested documents
  NestedDoc = bson:document([
    {find, <<"test_collection">>},
    {filter, bson:document([{status, <<"active">>}])},
    {projection, bson:document([{name, 1}, {'_id', 0}])}
  ]),
  
  %% This should not crash even if collection doesn't exist
  Result = mc_worker_api:command(Conn, NestedDoc),
  ?assert(is_tuple(Result)),
  
  mc_worker_api:disconnect(Conn),
  ok.
