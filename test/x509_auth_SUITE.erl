-module(x509_auth_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile([export_all, nowarn_export_all]).

-define(CERT_DIR, "/tmp/mongodb-x509-certs").
-define(X509_PORT, 27022).

%%====================================================================
%% CT Callbacks
%%====================================================================

all() ->
  [
    test_successful_x509_authentication,
    test_successful_x509_auth_with_explicit_subject,
    test_failed_x509_authentication_invalid_cert,
    test_operations_with_x509_auth,
    test_x509_auth_without_ssl_fails
  ].

init_per_suite(Config) ->
  application:ensure_all_started(mongodb),
  %% Check if x509 MongoDB is running on port 27022
  case gen_tcp:connect("localhost", ?X509_PORT, [], 1000) of
    {ok, Socket} ->
      gen_tcp:close(Socket),
      %% Read client subject from file
      {ok, SubjectBin} = file:read_file(?CERT_DIR ++ "/client-valid-subject.txt"),
      Subject = string:trim(binary_to_list(SubjectBin)),
      [
        {database, <<"test">>},
        {x509_port, ?X509_PORT},
        {cert_dir, ?CERT_DIR},
        {client_subject, list_to_binary(Subject)}
        | Config
      ];
    {error, _} ->
      ct:pal("MongoDB with X509 auth not running on port ~p. "
             "Start with: ./scripts/setup_mongodb8_x509.sh", [?X509_PORT]),
      {skip, "MongoDB with X509 authentication not available"}
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

%% Test that we can successfully connect with X509 client certificate
test_successful_x509_authentication(Config) ->
  Port = ?config(x509_port, Config),
  CertDir = ?config(cert_dir, Config),
  Database = ?config(database, Config),

  %% Connect with valid X509 certificate
  {ok, Conn} = mc_worker_api:connect([
    {database, Database},
    {host, "localhost"},
    {port, Port},
    {ssl, true},
    {ssl_opts, [
      {certfile, CertDir ++ "/client-valid.pem"},
      {keyfile, CertDir ++ "/client-valid.key"},
      {cacertfile, CertDir ++ "/server-ca.pem"},
      {verify, verify_peer},
      {server_name_indication, "localhost"}
    ]},
    {auth_mechanism, 'MONGODB-X509'}
  ]),

  %% Verify we can perform operations
  {true, Result} = mc_worker_api:command(Conn, {<<"ping">>, 1}),
  ?assertEqual(1.0, maps:get(<<"ok">>, Result)),

  mc_worker_api:disconnect(Conn),
  ok.

%% Test X509 auth with explicit subject
test_successful_x509_auth_with_explicit_subject(Config) ->
  Port = ?config(x509_port, Config),
  CertDir = ?config(cert_dir, Config),
  Database = ?config(database, Config),
  Subject = ?config(client_subject, Config),

  %% Connect with valid X509 certificate and explicit subject
  {ok, Conn} = mc_worker_api:connect([
    {database, Database},
    {host, "localhost"},
    {port, Port},
    {ssl, true},
    {ssl_opts, [
      {certfile, CertDir ++ "/client-valid.pem"},
      {keyfile, CertDir ++ "/client-valid.key"},
      {cacertfile, CertDir ++ "/server-ca.pem"},
      {verify, verify_peer},
      {server_name_indication, "localhost"}
    ]},
    {auth_mechanism, 'MONGODB-X509'},
    {x509_subject, Subject}
  ]),

  %% Verify we can perform operations
  {true, Result} = mc_worker_api:command(Conn, {<<"ping">>, 1}),
  ?assertEqual(1.0, maps:get(<<"ok">>, Result)),

  mc_worker_api:disconnect(Conn),
  ok.

%% Test that X509 authentication fails with invalid certificate
test_failed_x509_authentication_invalid_cert(Config) ->
  Port = ?config(x509_port, Config),
  CertDir = ?config(cert_dir, Config),
  Database = ?config(database, Config),

  %% Try to connect with invalid certificate (signed by different CA)
  %% This should fail during SSL handshake or authentication
  %% We need to catch the exception since the SSL handshake may close the connection
  process_flag(trap_exit, true),
  Result = try
    mc_worker_api:connect([
      {database, Database},
      {host, "localhost"},
      {port, Port},
      {ssl, true},
      {ssl_opts, [
        {certfile, CertDir ++ "/client-invalid.pem"},
        {keyfile, CertDir ++ "/client-invalid.key"},
        {cacertfile, CertDir ++ "/server-ca.pem"},
        {verify, verify_peer},
        {server_name_indication, "localhost"}
      ]},
      {auth_mechanism, 'MONGODB-X509'}
    ])
  catch
    error:_ -> {error, auth_failed};
    exit:_ -> {error, connection_closed};
    _:_ -> {error, unknown}
  end,
  process_flag(trap_exit, false),

  %% Should fail with either SSL error or authentication error
  case Result of
    {error, _} ->
      %% Error - expected
      ok;
    {ok, Conn} ->
      %% Connection succeeded but shouldn't have - disconnect and fail
      mc_worker_api:disconnect(Conn),
      ?assert(false)
  end.

%% Test that we can perform database operations with X509 auth
test_operations_with_x509_auth(Config) ->
  Port = ?config(x509_port, Config),
  CertDir = ?config(cert_dir, Config),
  Database = ?config(database, Config),

  %% Connect with valid X509 certificate
  {ok, Conn} = mc_worker_api:connect([
    {database, Database},
    {host, "localhost"},
    {port, Port},
    {ssl, true},
    {ssl_opts, [
      {certfile, CertDir ++ "/client-valid.pem"},
      {keyfile, CertDir ++ "/client-valid.key"},
      {cacertfile, CertDir ++ "/server-ca.pem"},
      {verify, verify_peer},
      {server_name_indication, "localhost"}
    ]},
    {auth_mechanism, 'MONGODB-X509'}
  ]),

  Collection = <<"x509_test_collection">>,

  %% Insert a document
  Doc = #{<<"name">> => <<"x509_test">>, <<"value">> => 123},
  {{true, #{<<"n">> := 1}}, _InsertedDoc} = mc_worker_api:insert(Conn, Collection, Doc),

  %% Find the document
  FoundDoc = mc_worker_api:find_one(Conn, Collection, #{<<"name">> => <<"x509_test">>}),
  ?assertEqual(<<"x509_test">>, maps:get(<<"name">>, FoundDoc)),
  ?assertEqual(123, maps:get(<<"value">>, FoundDoc)),

  %% Update the document
  {true, #{<<"n">> := 1}} = mc_worker_api:update(Conn, Collection,
    #{<<"name">> => <<"x509_test">>},
    #{<<"$set">> => #{<<"value">> => 456}}),

  %% Verify update
  UpdatedDoc = mc_worker_api:find_one(Conn, Collection, #{<<"name">> => <<"x509_test">>}),
  ?assertEqual(456, maps:get(<<"value">>, UpdatedDoc)),

  %% Clean up
  mc_worker_api:delete(Conn, Collection, #{}),
  mc_worker_api:disconnect(Conn),
  ok.

%% Test that X509 auth without SSL fails properly
test_x509_auth_without_ssl_fails(Config) ->
  Port = ?config(x509_port, Config),
  Database = ?config(database, Config),

  %% Try to connect without SSL - should fail because server requires TLS
  process_flag(trap_exit, true),
  Result = try
    mc_worker_api:connect([
      {database, Database},
      {host, "localhost"},
      {port, Port},
      {ssl, false},  %% No SSL
      {auth_mechanism, 'MONGODB-X509'}
    ])
  catch
    error:_ -> {error, auth_failed};
    exit:_ -> {error, connection_closed};
    _:_ -> {error, unknown}
  end,
  process_flag(trap_exit, false),

  %% Should fail - either connection error or auth error
  case Result of
    {error, _} ->
      %% Error - expected
      ok;
    {ok, Conn} ->
      %% Connection succeeded but shouldn't work without SSL
      mc_worker_api:disconnect(Conn),
      ?assert(false)
  end.
