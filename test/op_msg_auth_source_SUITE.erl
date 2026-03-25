-module(op_msg_auth_source_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile([export_all, nowarn_export_all]).

all() ->
  [
    auth_source_same_as_database_test,
    auth_source_differs_from_database_test
  ].

init_per_suite(Config) ->
  application:ensure_all_started(mongodb),
  Host = os:getenv("MONGO_AUTH_HOST", "localhost"),
  Port = list_to_integer(os:getenv("MONGO_AUTH_PORT", "27021")),
  case gen_tcp:connect(Host, Port, [], 1000) of
    {ok, Socket} ->
      gen_tcp:close(Socket),
      [
        {mongo_host, Host},
        {mongo_port, Port},
        {mongo_user, bin_env("MONGO_AUTH_USER", "admin")},
        {mongo_password, bin_env("MONGO_AUTH_PASSWORD", "admin123")}
        | Config
      ];
    {error, _} ->
      {skip, "Auth-enabled MongoDB not available"}
  end.

end_per_suite(_Config) ->
  ok.

auth_source_same_as_database_test(Config) ->
  Connection = connect(Config, <<"admin">>, <<"admin">>),
  ?assertEqual(op_msg, mc_worker_pid_info:get_protocol_type(Connection)),
  {true, PingResult} = mc_worker_api:command(Connection, {<<"ping">>, 1}),
  ?assertEqual(1.0, maps:get(<<"ok">>, PingResult)),
  mc_worker_api:disconnect(Connection),
  ok.

auth_source_differs_from_database_test(Config) ->
  Connection = connect(Config, <<"test">>, <<"admin">>),
  ?assertEqual(op_msg, mc_worker_pid_info:get_protocol_type(Connection)),
  {true, PingResult} = mc_worker_api:command(Connection, {<<"ping">>, 1}),
  ?assertEqual(1.0, maps:get(<<"ok">>, PingResult)),
  mc_worker_api:disconnect(Connection),
  ok.

connect(Config, Database, AuthSource) ->
  Host = ?config(mongo_host, Config),
  Port = ?config(mongo_port, Config),
  User = ?config(mongo_user, Config),
  Password = ?config(mongo_password, Config),
  {ok, Connection} = mc_worker_api:connect([
    {database, Database},
    {auth_source, AuthSource},
    {login, User},
    {password, Password},
    {host, Host},
    {port, Port},
    {use_legacy_protocol, false}
  ]),
  Connection.

bin_env(Name, Default) ->
  list_to_binary(os:getenv(Name, Default)).
