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
-export([
  get_value/2,
  get_value/3,
  value_to_binary/1,
  pw_key/3,
  pw_hash/2,
  get_timeout/0,
  encode_name/1,
  random_nonce/1,
  hmac/2,
  is_proplist/1,
  to_binary/1]).

get_value(Key, List) -> get_value(Key, List, undefined).

get_value(Key, List, Default) ->
  case lists:keyfind(Key, 1, List) of
    {_, Value} -> Value;
    false -> Default
  end.

-spec is_proplist(list() | any()) -> boolean().
is_proplist(List) ->
  Check = fun({X, _}) when is_atom(X) -> true;(_) -> false end,
  lists:all(Check, List).

-spec encode_name(binary()) -> binary().
encode_name(Name) ->
  Comma = re:replace(Name, <<"=">>, <<"=3D">>, [{return, binary}]),
  re:replace(Comma, <<",">>, <<"=2C">>, [{return, binary}]).

-spec random_nonce(integer()) -> binary().
random_nonce(TextLength) ->
	ByteLength = trunc(TextLength / 4 * 3),
	RandBytes = crypto:strong_rand_bytes(ByteLength),
	base64:encode(RandBytes).	

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

hmac(One, Two) -> crypto:hmac(sha, One, Two).

pw_key(Nonce, Username, Password) ->
  bson:utf8(binary_to_hexstr(crypto:hash(md5, [Nonce, Username, pw_hash(Username, Password)]))).

pw_hash(Username, Password) ->
  bson:utf8(binary_to_hexstr(crypto:hash(md5, [Username, <<":mongo:">>, Password]))).

-spec to_binary(string() | binary()) -> binary().
to_binary(Str) when is_list(Str) ->  list_to_binary(Str);
to_binary(Str) when is_binary(Str) ->  Str.


%% @private
binary_to_hexstr(Bin) ->
  lists:flatten([io_lib:format("~2.16.0b", [X]) || X <- binary_to_list(Bin)]).
