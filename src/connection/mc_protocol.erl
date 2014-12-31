%%%-------------------------------------------------------------------
%%% @author tihon
%%% @copyright (C) 2014, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 29. Дек. 2014 15:40
%%%-------------------------------------------------------------------
-module(mc_protocol).
-author("tihon").

-include("mongo_protocol.hrl").

%% API
-export([auth/3, connect_to_database/1]).

%% Make connection to database and return socket
connect_to_database(Conf) ->
  Timeout = mc_utils:get_value(timeout, Conf, infinity),
  Host = mc_utils:get_value(host, Conf, "127.0.0.1"),
  Port = mc_utils:get_value(port, Conf, 27017),
  gen_tcp:connect(Host, Port, [binary, {active, false}, {packet, raw}], Timeout).

%% Authorize on database synchronously
auth(Socket, Conf, Database) ->
  Login = mc_utils:get_value(login, Conf),
  Password = mc_utils:get_value(password, Conf),
  do_auth(Socket, Database, Login, Password).


%% @private
do_auth(_, _, Login, Pass) when Login == undefined; Pass == undefined -> ok; %do nothing
do_auth(Socket, Database, Login, Password) ->
  Command1 = {getnonce, 1},
  {Doc} = mc_action_man:read_one_sync(Socket, Database, #'query'{
    collection = '$cmd',
    selector = Command1
  }),
  {true, Res} = mc_connection_man:process_reply(Doc, Command1),
  Nonce = bson:at(nonce, Res),
  Command2 = {authenticate, 1, user, Login, nonce, Nonce, key, mc_utils:pw_key(Nonce, Login, Password)},
  {Doc2} = mc_action_man:read_one_sync(Socket, Database, #'query'{
    collection = '$cmd',
    selector = Command2
  }),
  R = mc_connection_man:process_reply(Doc2, Command2),
  io:format("Res ~p~n", [R]),
  R.