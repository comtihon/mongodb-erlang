%%%-------------------------------------------------------------------
%%% @author tihon
%%% @copyright (C) 2014, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 30. Дек. 2014 22:54
%%%-------------------------------------------------------------------
-module(mc_utils).
-author("tihon").

%% API
-export([get_value/2, get_value/3, value_to_binary/1, pw_key/3, get_timeout/0]).

get_value(Key, List) -> get_value(Key, List, undefined).

get_value(Key, List, Default) ->
  case lists:keyfind(Key, 1, List) of
    {_, Value} -> Value;
    false -> Default
  end.

value_to_binary(Value) when is_integer(Value) ->
  bson:utf8(integer_to_list(Value));
value_to_binary(Value) when is_atom(Value) ->
  atom_to_binary(Value, utf8);
value_to_binary(Value) when is_binary(Value) ->
  Value;
value_to_binary(_Value) ->
  <<>>.

get_timeout() ->
  case application:get_env(mc_worker_call_timeout) of
    {ok, Time} -> Time;
    undefined -> infinity
  end.

pw_key(Nonce, Username, Password) ->
  bson:utf8(binary_to_hexstr(crypto:hash(md5, [Nonce, Username, pw_hash(Username, Password)]))).


%% @private
pw_hash(Username, Password) ->
  bson:utf8(binary_to_hexstr(crypto:hash(md5, [Username, <<":mongo:">>, Password]))).

%% @private
binary_to_hexstr(Bin) ->
  lists:flatten([io_lib:format("~2.16.0b", [X]) || X <- binary_to_list(Bin)]).