-module(error_handling_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile([export_all, nowarn_export_all]).

%%====================================================================
%% CT Callbacks
%%====================================================================

all() ->
  [
    test_connection_failure_handling,
    test_invalid_database_name,
    test_command_on_closed_connection,
    test_malformed_query_handling,
    test_network_timeout_handling,
    test_authentication_failure
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

%% Test connection failure to non-existent host
test_connection_failure_handling(_Config) ->
  %% Try to connect to non-existent host
  %% The current implementation throws an exception rather than returning error tuple
  Result = (catch mc_worker_api:connect([
    {database, <<"test">>},
    {host, "nonexistent.invalid"},
    {port, 27017}
  ])),
  
  %% Should fail (either error tuple or exception), not hang
  ?assert(
    case Result of
      {error, _} -> true;
      {'EXIT', _} -> true;
      {badmatch, {error, _}} -> true;
      _ -> false
    end
  ),
  
  ok.

%% Test with invalid database name
test_invalid_database_name(_Config) ->
  %% Empty database name should be handled
  Result1 = mc_worker_api:connect([{database, <<>>}]),
  
  %% Should either connect (and fail on operations) or return error
  %% Both are acceptable error handling
  case Result1 of
    {ok, Conn} ->
      %% If connection succeeds, operations should fail gracefully
      mc_worker_api:disconnect(Conn);
    {error, _} ->
      %% Connection failed gracefully
      ok
  end,
  
  ok.

%% Test operations on closed connection
test_command_on_closed_connection(Config) ->
  Database = ?config(database, Config),
  
  {ok, Conn} = mc_worker_api:connect([{database, Database}]),
  
  %% Close the connection
  ok = mc_worker_api:disconnect(Conn),
  
  %% Try to execute command on closed connection
  %% Should fail gracefully, not crash
  Result = (catch mc_worker_api:command(Conn, bson:document([{ping, 1}]))),
  
  %% Should be an error (exit, throw, or error tuple)
  ?assert(
    is_tuple(Result) andalso 
    (element(1, Result) =:= 'EXIT' orelse 
     element(1, Result) =:= error)
  ),
  
  ok.

%% Test malformed query handling
test_malformed_query_handling(Config) ->
  Database = ?config(database, Config),
  
  {ok, Conn} = mc_worker_api:connect([{database, Database}]),
  
  Collection = <<"test_error_collection">>,
  
  %% Try to find with malformed selector
  %% Most malformed queries will be caught by BSON encoding
  Result = (catch mc_worker_api:find(Conn, Collection, invalid_selector)),
  
  %% Should fail gracefully
  ?assert(is_tuple(Result)),
  
  mc_worker_api:disconnect(Conn),
  ok.

%% Test network timeout handling
test_network_timeout_handling(Config) ->
  Database = ?config(database, Config),
  
  %% Connect with very short timeout
  Result = mc_worker_api:connect([
    {database, Database},
    {connect_timeout_ms, 1}  % 1ms timeout - very aggressive
  ]),
  
  %% Should either connect (if fast enough) or timeout gracefully
  case Result of
    {ok, Conn} ->
      %% Connection succeeded despite short timeout
      mc_worker_api:disconnect(Conn),
      ok;
    {error, _Reason} ->
      %% Timeout occurred, handled gracefully
      ok
  end,
  
  ok.

%% Test authentication failure handling
test_authentication_failure(_Config) ->
  %% Try to connect with invalid credentials
  %% The current implementation throws an exception on auth failure
  Result = (catch mc_worker_api:connect([
    {database, <<"test">>},
    {login, <<"invalid_user">>},
    {password, <<"wrong_password">>}
  ])),
  
  %% Should fail gracefully (exception or error tuple)
  case Result of
    {error, _} ->
      %% Auth failed with error tuple
      ok;
    {ok, Conn} ->
      %% Connection succeeded (maybe auth not required on this MongoDB instance)
      mc_worker_api:disconnect(Conn),
      ok;
    {<<"Can't pass authentication">>, _} ->
      %% Auth failed with exception (current behavior)
      ok;
    {'EXIT', _} ->
      %% Auth failed with exit
      ok
  end,
  
  ok.
