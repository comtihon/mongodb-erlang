%%%-------------------------------------------------------------------
%%% @author tihon
%%% @copyright (C) 2014, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 29. Дек. 2014 15:40
%%%-------------------------------------------------------------------
-module(mc_auth).
-author("tihon").

-include("mongo_protocol.hrl").

%% API
-export([auth/3, connect_to_database/1, get_version/2]).

%% Make connection to database and return socket
-spec connect_to_database(proplists:proplist()) -> {ok, port()} | {error, inet:posix()}.
connect_to_database(Conf) ->  %TODO scram server-first auth case
  Timeout = mc_utils:get_value(timeout, Conf, infinity),
  Host = mc_utils:get_value(host, Conf, "127.0.0.1"),
  Port = mc_utils:get_value(port, Conf, 27017),
  gen_tcp:connect(Host, Port, [binary, {active, false}, {packet, raw}], Timeout).

%% Get server version. This is need to choose default authentification method.
-spec get_version(port(), binary()) -> float().
get_version(Socket, Database) ->
  {true, #{<<"version">> := Version}} = mongo:sync_command(Socket, Database, {<<"buildinfo">>, 1}),
  {VFloat, _} = string:to_float(binary_to_list(Version)),
  VFloat.

%% Authorize on database synchronously
-spec auth(port(), proplists:proplist(), binary()) -> {boolean(), bson:document()}.
auth(Socket, Conf, Database) ->
  Version = get_version(Socket, Database),
  Login = mc_utils:get_value(login, Conf),
  Password = mc_utils:get_value(password, Conf),
  do_auth(Version, Socket, Database, Login, Password).


%% @private
-spec do_auth(float(), port(), binary(), binary() | undefined, binary() | undefined) -> boolean().
do_auth(_, _, _, Login, Pass) when Login == undefined; Pass == undefined -> true; %do nothing
do_auth(Version, Socket, Database, Login, Password) when Version > 2.7 ->  %new authorisation
  mc_auth_logic:scram_sha_1_auth(Socket, Database, Login, Password);
do_auth(_, Socket, Database, Login, Password) ->   %old authorisation
  mc_auth_logic:mongodb_cr_auth(Socket, Database, Login, Password).