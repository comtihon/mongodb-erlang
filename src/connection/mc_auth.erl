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
-export([auth/3, connect_to_database/1]).

%% Make connection to database and return socket
-spec connect_to_database(proplists:proplist()) -> {ok, port()} | {error, inet:posix()}.
connect_to_database(Conf) ->
  Timeout = mc_utils:get_value(timeout, Conf, infinity),
  Host = mc_utils:get_value(host, Conf, "127.0.0.1"),
  Port = mc_utils:get_value(port, Conf, 27017),
  gen_tcp:connect(Host, Port, [binary, {active, false}, {packet, raw}], Timeout).

%% Authorize on database synchronously
-spec auth(port(), proplists:proplist(), binary()) -> {boolean(), bson:document()}.
auth(Socket, Conf, Database) ->
  Login = mc_utils:get_value(login, Conf),
  Password = mc_utils:get_value(password, Conf),
  do_auth(Socket, Database, Login, Password).


%% @private
-spec do_auth(port(), binary(), binary() | undefined, binary() | undefined) -> boolean().
do_auth(_, _, Login, Pass) when Login == undefined; Pass == undefined -> true; %do nothing
do_auth(Socket, Database, Login, Password) ->
  {true, Res} = mongo:sync_command(Socket, Database, {getnonce, 1}),
  Nonce = bson:at(nonce, Res),
  case mongo:sync_command(Socket, Database, {authenticate, 1, user, Login, nonce, Nonce, key, mc_utils:pw_key(Nonce, Login, Password)}) of
    {true, _} -> true;
    {false, Reason} -> erlang:error(Reason)
  end.