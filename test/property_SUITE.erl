-module(property_SUITE).

-include_lib("proper/include/proper.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile([export_all, nowarn_export_all]).

%%====================================================================
%% CT Callbacks
%%====================================================================

all() ->
  [
    prop_bson_document_roundtrip,
    prop_bson_append_associative,
    prop_ping_command_idempotent,
    prop_insert_find_roundtrip,
    prop_protocol_detection_consistent
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
%% Property Tests
%%====================================================================

%% Test that BSON documents can be encoded and decoded without loss
prop_bson_document_roundtrip(_Config) ->
  Property = ?FORALL(Fields, list({binary_field(), simple_bson_value()}),
    begin
      try
        Doc = bson:document(Fields),
        Encoded = bson_binary:put_document(Doc),
        {Decoded, <<>>} = bson_binary:get_document(Encoded),
        % Sort fields for comparison since order might vary
        lists:sort(bson:fields(Doc)) =:= lists:sort(bson:fields(Decoded))
      catch
        _:_ -> false
      end
    end),
  
  true = proper:quickcheck(Property, [{numtests, 100}, {to_file, user}]),
  ok.

%% Test that bson:append is associative
prop_bson_append_associative(_Config) ->
  Property = ?FORALL({Fields1, Fields2, Fields3}, 
                     {list({binary_field(), simple_bson_value()}),
                      list({binary_field(), simple_bson_value()}),
                      list({binary_field(), simple_bson_value()})},
    begin
      try
        Doc1 = bson:document(Fields1),
        Doc2 = bson:document(Fields2),
        Doc3 = bson:document(Fields3),
        
        % (Doc1 + Doc2) + Doc3
        Left = bson:append(bson:append(Doc1, Doc2), Doc3),
        % Doc1 + (Doc2 + Doc3)
        Right = bson:append(Doc1, bson:append(Doc2, Doc3)),
        
        bson:fields(Left) =:= bson:fields(Right)
      catch
        _:_ -> false
      end
    end),
  
  true = proper:quickcheck(Property, [{numtests, 100}, {to_file, user}]),
  ok.

%% Test that ping command is idempotent
prop_ping_command_idempotent(Config) ->
  Database = ?config(database, Config),
  
  Property = ?FORALL(_, integer(),
    begin
      try
        {ok, Conn} = mc_worker_api:connect([{database, Database}]),
        
        {true, Result1} = mc_worker_api:command(Conn, bson:document([{ping, 1}])),
        {true, Result2} = mc_worker_api:command(Conn, bson:document([{ping, 1}])),
        
        mc_worker_api:disconnect(Conn),
        
        % Both results should have ok: 1.0
        maps:get(<<"ok">>, Result1) =:= 1.0 andalso
        maps:get(<<"ok">>, Result2) =:= 1.0
      catch
        _:_ -> false
      end
    end),
  
  true = proper:quickcheck(Property, [{numtests, 10}, {to_file, user}]),
  ok.

%% Test that documents can be inserted and retrieved
prop_insert_find_roundtrip(Config) ->
  Database = ?config(database, Config),
  
  Property = ?FORALL(DocFields, non_empty(list({binary_field(), simple_bson_value()})),
    begin
      Result = try
        {ok, Conn} = mc_worker_api:connect([{database, Database}]),
        
        % Create unique collection name
        Timestamp = erlang:system_time(microsecond),
        Collection = <<"prop_test_", (integer_to_binary(Timestamp))/binary>>,
        
        % Create document
        Doc = maps:from_list(DocFields),
        
        % Insert - returns {{true, Result}, InsertedDoc} not a list
        {{true, _}, InsertedDoc} = mc_worker_api:insert(Conn, Collection, Doc),
        
        % Find
        FoundDoc = mc_worker_api:find_one(Conn, Collection, #{}),
        
        % Cleanup
        mc_worker_api:delete(Conn, Collection, #{}),
        mc_worker_api:disconnect(Conn),
        
        % Compare without _id field (MongoDB adds it)
        DocWithoutId = maps:without([<<"_id">>], InsertedDoc),
        FoundWithoutId = maps:without([<<"_id">>], FoundDoc),
        
        Match = DocWithoutId =:= FoundWithoutId,
        case Match of
          false ->
            ct:log("Mismatch:~nInserted: ~p~nFound: ~p", [DocWithoutId, FoundWithoutId]);
          _ -> ok
        end,
        Match
      catch
        Class:Error:Stack ->
          ct:log("Insert/Find roundtrip failed: ~p:~p~n~p", [Class, Error, Stack]),
          false
      end,
      Result
    end),
  
  true = proper:quickcheck(Property, [{numtests, 20}, {to_file, user}]),
  ok.

%% Test that protocol detection is consistent
prop_protocol_detection_consistent(Config) ->
  Database = ?config(database, Config),
  
  Property = ?FORALL(_, integer(),
    begin
      try
        {ok, Conn1} = mc_worker_api:connect([{database, Database}]),
        Protocol1 = mc_worker_pid_info:get_protocol_type(Conn1),
        mc_worker_api:disconnect(Conn1),
        
        {ok, Conn2} = mc_worker_api:connect([{database, Database}]),
        Protocol2 = mc_worker_pid_info:get_protocol_type(Conn2),
        mc_worker_api:disconnect(Conn2),
        
        % Protocol should be the same for the same MongoDB instance
        Protocol1 =:= Protocol2 andalso
        (Protocol1 =:= op_msg orelse Protocol1 =:= legacy)
      catch
        _:_ -> false
      end
    end),
  
  true = proper:quickcheck(Property, [{numtests, 10}, {to_file, user}]),
  ok.

%%====================================================================
%% Generators
%%====================================================================

%% Generate binary field names (non-empty, valid UTF-8)
binary_field() ->
  ?LET(Str, non_empty(list(choose($a, $z))),
    list_to_binary(Str)).

%% Generate simple BSON values (all basic types MongoDB supports)
simple_bson_value() ->
  oneof([
    binary_value(),
    integer(),
    boolean(),
    float()
  ]).

%% Generate binary values (non-empty to avoid MongoDB edge cases)
binary_value() ->
  ?LET(Str, non_empty(list(choose($a, $z))),
    list_to_binary(Str)).
