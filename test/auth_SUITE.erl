-module(auth_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile([export_all, nowarn_export_all]).

%%====================================================================
%% CT Callbacks
%%====================================================================

all() ->
  [
    test_successful_authentication,
    test_failed_authentication_wrong_password,
    test_failed_authentication_wrong_user,
    test_operations_with_auth,
    test_connection_without_auth_fails
  ].

init_per_suite(Config) ->
  application:ensure_all_started(mongodb),
  %% Check if auth MongoDB is running on port 27021
  %% Note: Port 27017 is used by single node, 27018-27020 by replica set cluster
  case gen_tcp:connect("localhost", 27021, [], 1000) of
    {ok, Socket} ->
      gen_tcp:close(Socket),
      [{database, <<"test">>}, {auth_port, 27021} | Config];
    {error, _} ->
      ct:pal("MongoDB with auth not running on port 27021. Start with: ./scripts/start_mongo_auth.sh"),
      {skip, "MongoDB with authentication not available"}
  end.

end_per_suite(_Config) ->
  ok.

init_per_testcase(_Case, Config) ->
  Config.

end_per_testcase(_Case, _Config) ->
  ok.

%%====================================================================
%% Test Cases
%%====================================================================

%% Test that we can successfully connect with correct credentials
test_successful_authentication(Config) ->
  Database = ?config(database, Config),
  Port = ?config(auth_port, Config),
  
  %% Connect with correct credentials
  {ok, Conn} = mc_worker_api:connect([
    {database, Database},
    {auth_source, Database},
    {login, <<"testuser">>},
    {password, <<"testpass">>},
    {host, "localhost"},
    {port, Port}
  ]),
  
  %% Verify we can perform operations
  {true, Result} = mc_worker_api:command(Conn, {<<"ping">>, 1}),
  ?assertEqual(1.0, maps:get(<<"ok">>, Result)),
  
  mc_worker_api:disconnect(Conn),
  ok.

%% Test that authentication fails with wrong password
test_failed_authentication_wrong_password(Config) ->
  Database = ?config(database, Config),
  Port = ?config(auth_port, Config),
  
  %% Try to connect with wrong password - should throw an exception
  Result = (catch mc_worker_api:connect([
    {database, Database},
    {auth_source, Database},
    {login, <<"testuser">>},
    {password, <<"wrongpassword">>},
    {host, "localhost"},
    {port, Port}
  ])),
  
  %% Should fail with authentication error
  ?assertMatch({'EXIT', {<<"Can't pass authentication">>, _}}, Result),
  ok.

%% Test that authentication fails with wrong username
test_failed_authentication_wrong_user(Config) ->
  Database = ?config(database, Config),
  Port = ?config(auth_port, Config),
  
  %% Try to connect with non-existent user - should throw an exception
  Result = (catch mc_worker_api:connect([
    {database, Database},
    {auth_source, Database},
    {login, <<"wronguser">>},
    {password, <<"testpass">>},
    {host, "localhost"},
    {port, Port}
  ])),
  
  %% Should fail with authentication error
  ?assertMatch({'EXIT', {<<"Can't pass authentication">>, _}}, Result),
  ok.

%% Test that we can perform database operations with authentication
test_operations_with_auth(Config) ->
  Database = ?config(database, Config),
  Port = ?config(auth_port, Config),
  
  %% Connect with correct credentials
  {ok, Conn} = mc_worker_api:connect([
    {database, Database},
    {auth_source, Database},
    {login, <<"testuser">>},
    {password, <<"testpass">>},
    {host, "localhost"},
    {port, Port}
  ]),
  
  Collection = <<"auth_test_collection">>,
  
  %% Create an index on the name field
  ok = mc_worker_api:ensure_index(Conn, Collection, #{
    <<"key">> => {<<"name">>, 1},
    <<"name">> => <<"name_1">>
  }),
  
  %% Insert a document
  Doc = #{<<"name">> => <<"test">>, <<"value">> => 42},
  {{true, #{<<"n">> := 1}}, _InsertedDoc} = mc_worker_api:insert(Conn, Collection, Doc),
  
  %% Find the document using find_one
  FoundDoc = mc_worker_api:find_one(Conn, Collection, #{<<"name">> => <<"test">>}),
  ?assertEqual(<<"test">>, maps:get(<<"name">>, FoundDoc)),
  ?assertEqual(42, maps:get(<<"value">>, FoundDoc)),
  
  %% Clean up
  mc_worker_api:delete(Conn, Collection, #{}),
  mc_worker_api:disconnect(Conn),
  ok.

%% Test that connection without auth fails when auth is required
test_connection_without_auth_fails(Config) ->
  Database = ?config(database, Config),
  Port = ?config(auth_port, Config),
  
  %% Try to connect without credentials
  {ok, Conn} = mc_worker_api:connect([
    {database, Database},
    {host, "localhost"},
    {port, Port}
  ]),
  
  %% Connection succeeds but operations should fail with unauthorized error
  %% Try to insert a document which requires authentication
  Collection = <<"auth_test_no_auth">>,
  Doc = #{<<"test">> => <<"value">>},
  Result = (catch mc_worker_api:insert(Conn, Collection, Doc)),
  
  %% Should fail with unauthorized error
  ?assertMatch({'EXIT', {unauthorized, _}}, Result),
  
  mc_worker_api:disconnect(Conn),
  ok.
