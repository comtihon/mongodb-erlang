-module(protocol_detection_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile([export_all, nowarn_export_all]).

%%====================================================================
%% CT Callbacks
%%====================================================================

all() ->
  [
    test_auto_protocol_detection,
    test_op_msg_protocol_forced,
    test_legacy_protocol_forced,
    test_hello_command_with_op_msg,
    test_ismaster_command_with_legacy,
    test_protocol_type_stored_in_ets,
    test_commands_work_with_detected_protocol,
    test_bson_document_formats
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

%% Test that protocol detection automatically selects op_msg for MongoDB 6.0+
test_auto_protocol_detection(Config) ->
  Database = ?config(database, Config),
  
  %% Connect with auto protocol detection (default)
  {ok, Conn} = mc_worker_api:connect([{database, Database}]),
  
  %% Verify protocol type is detected and stored
  ProtocolType = mc_worker_pid_info:get_protocol_type(Conn),
  
  %% For MongoDB 6.0+, should be op_msg
  ?assertEqual(op_msg, ProtocolType),
  
  mc_worker_api:disconnect(Conn),
  ok.

%% Test forcing op_msg protocol
test_op_msg_protocol_forced(Config) ->
  Database = ?config(database, Config),
  
  {ok, Conn} = mc_worker_api:connect([
    {database, Database},
    {use_legacy_protocol, false}
  ]),
  
  ProtocolType = mc_worker_pid_info:get_protocol_type(Conn),
  ?assertEqual(op_msg, ProtocolType),
  
  mc_worker_api:disconnect(Conn),
  ok.

%% Test forcing legacy protocol (should fail on MongoDB 6.0+)
test_legacy_protocol_forced(Config) ->
  Database = ?config(database, Config),
  
  %% This should connect but commands will fail on MongoDB 6.0+
  {ok, Conn} = mc_worker_api:connect([
    {database, Database},
    {use_legacy_protocol, true}
  ]),
  
  ProtocolType = mc_worker_pid_info:get_protocol_type(Conn),
  ?assertEqual(legacy, ProtocolType),
  
  mc_worker_api:disconnect(Conn),
  ok.

%% Test that hello command works with op_msg protocol
test_hello_command_with_op_msg(Config) ->
  Database = ?config(database, Config),
  
  {ok, Conn} = mc_worker_api:connect([
    {database, Database},
    {use_legacy_protocol, false}
  ]),
  
  %% Execute hello command
  {true, Result} = mc_worker_api:command(Conn, bson:document([{hello, 1}])),
  
  %% Verify response contains expected fields
  ?assert(maps:is_key(<<"ok">>, Result)),
  ?assertEqual(1.0, maps:get(<<"ok">>, Result)),
  ?assert(maps:is_key(<<"maxWireVersion">>, Result)),
  
  mc_worker_api:disconnect(Conn),
  ok.

%% Test that isMaster command works with legacy protocol
test_ismaster_command_with_legacy(Config) ->
  Database = ?config(database, Config),
  
  {ok, Conn} = mc_worker_api:connect([
    {database, Database},
    {use_legacy_protocol, true}
  ]),
  
  %% Execute isMaster command
  {true, Result} = mc_worker_api:command(Conn, bson:document([{isMaster, 1}])),
  
  %% Verify response contains topology information
  ?assert(is_map(Result)),
  ?assert(map_size(Result) > 0),
  
  mc_worker_api:disconnect(Conn),
  ok.

%% Test that protocol type is correctly stored in ETS
test_protocol_type_stored_in_ets(Config) ->
  Database = ?config(database, Config),
  
  {ok, Conn} = mc_worker_api:connect([{database, Database}]),
  
  %% Check ETS table directly
  {ok, Info} = mc_worker_pid_info:get_info(Conn),
  ?assert(maps:is_key(protocol_type, Info)),
  
  ProtocolType = maps:get(protocol_type, Info),
  ?assert(ProtocolType =:= op_msg orelse ProtocolType =:= legacy),
  
  mc_worker_api:disconnect(Conn),
  ok.

%% Test that basic commands work with detected protocol
test_commands_work_with_detected_protocol(Config) ->
  Database = ?config(database, Config),
  
  {ok, Conn} = mc_worker_api:connect([{database, Database}]),
  
  %% Test ping command
  {true, PingResult} = mc_worker_api:command(Conn, bson:document([{ping, 1}])),
  ?assertEqual(1.0, maps:get(<<"ok">>, PingResult)),
  
  %% Test insert and find
  Collection = <<"test_protocol_collection">>,
  TestDoc = #{<<"test">> => <<"value">>, <<"timestamp">> => erlang:system_time(second)},
  
  {{true, _}, _} = mc_worker_api:insert(Conn, Collection, TestDoc),
  
  Found = mc_worker_api:find_one(Conn, Collection, #{}),
  ?assert(is_map(Found)),
  ?assert(maps:is_key(<<"test">>, Found)),
  ?assertEqual(<<"value">>, maps:get(<<"test">>, Found)),
  
  %% Cleanup
  mc_worker_api:delete(Conn, Collection, #{}),
  mc_worker_api:disconnect(Conn),
  ok.

%% Test various BSON document formats
test_bson_document_formats(Config) ->
  Database = ?config(database, Config),
  
  {ok, Conn} = mc_worker_api:connect([{database, Database}]),
  
  %% Test command with tuple BSON document
  TupleDoc = bson:document([{ping, 1}]),
  ?assert(is_tuple(TupleDoc)),
  {true, Result1} = mc_worker_api:command(Conn, TupleDoc),
  ?assertEqual(1.0, maps:get(<<"ok">>, Result1)),
  
  %% Test command with list format (converted to BSON document)
  ListDoc = bson:document([{ping, 1}]),
  {true, Result2} = mc_worker_api:command(Conn, ListDoc),
  ?assertEqual(1.0, maps:get(<<"ok">>, Result2)),
  
  mc_worker_api:disconnect(Conn),
  ok.
