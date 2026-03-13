-module(buildinfo_auth_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile([export_all, nowarn_export_all]).

%% Regression test for MongoDB 8.x auth flow:
%% some servers reject pre-auth buildInfo, but connect/1 must still reach SCRAM.

all() ->
  [buildinfo_auth_regression_test].

init_per_suite(Config) ->
  application:ensure_all_started(mongodb),
  {Host, Ports} = target(),
  case find_target(Host, Ports) of
    {ok, Port} ->
      [{database, <<"admin">>}, {mongo_host, Host}, {mongo_port, Port} | Config];
    not_found ->
      {skip, "No auth-enabled MongoDB with pre-auth buildInfo rejection found"}
  end.

end_per_suite(_Config) ->
  ok.

buildinfo_auth_regression_test(Config) ->
  Database = ?config(database, Config),
  Host = ?config(mongo_host, Config),
  Port = ?config(mongo_port, Config),
  Result = catch mc_worker_api:connect([
    {database, Database},
    {login, <<"definitely_invalid_user">>},
    {password, <<"wrong_password">>},
    {host, Host},
    {port, Port}
  ]),
  ?assertMatch({'EXIT', {<<"Can't pass authentication">>, _}}, Result),
  ?assertNot(is_buildinfo_auth_error(Result)),
  ok.

target() ->
  case os:getenv("MONGO_AUTH_HOST") of
    false ->
      {"localhost", [27021, 27017]};
    Host ->
      {Host, [env_port()]}
  end.

env_port() ->
  list_to_integer(os:getenv("MONGO_AUTH_PORT", "27017")).

find_target(_, []) ->
  not_found;
find_target(Host, [Port | Rest]) ->
  case buildinfo_requires_authentication(Host, Port) of
    true -> {ok, Port};
    false -> find_target(Host, Rest)
  end.

buildinfo_requires_authentication(Host, Port) ->
  case catch mc_worker_api:connect([
    {database, <<"admin">>},
    {host, Host},
    {port, Port}
  ]) of
    {ok, Connection} ->
      Result = catch mc_worker_api:command(Connection, bson:document([{buildinfo, 1}])),
      mc_worker_api:disconnect(Connection),
      is_buildinfo_auth_error(Result);
    _ ->
      false
  end.

is_buildinfo_auth_error({'EXIT', {{error, {op_msg_response, ResponseDoc}}, _}}) ->
  is_buildinfo_auth_response(ResponseDoc);
is_buildinfo_auth_error({'EXIT', {{error, ResponseDoc}, _}}) when is_map(ResponseDoc) ->
  is_buildinfo_auth_response(ResponseDoc);
is_buildinfo_auth_error(_) ->
  false.

is_buildinfo_auth_response(#{<<"code">> := 13, <<"errmsg">> := Message}) when is_binary(Message) ->
  binary:match(Message, <<"buildInfo requires authentication">>) =/= nomatch;
is_buildinfo_auth_response(_) ->
  false.
