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

-include_lib("kernel/include/inet.hrl").

-define(OLD_CRYPTO_API, true).
-ifdef(OTP_RELEASE).
  -if(?OTP_RELEASE >= 23).
    -undef(OLD_CRYPTO_API).
  -endif.
-endif.

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
  to_binary/1,
  get_srv_seeds/1,
  use_legacy_protocol/1,
  get_connection_pid/1]).

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

-ifdef(OLD_CRYPTO_API).
hmac(One, Two) -> crypto:hmac(sha, One, Two).
-else.
hmac(One, Two) -> crypto:mac(hmac, sha, One, Two).
-endif.

pw_key(Nonce, Username, Password) ->
  bson:utf8(binary_to_hexstr(crypto:hash(md5, [Nonce, Username, pw_hash(Username, Password)]))).

pw_hash(Username, Password) ->
  bson:utf8(binary_to_hexstr(crypto:hash(md5, [Username, <<":mongo:">>, Password]))).

-spec to_binary(string() | binary()) -> binary().
to_binary(Str) when is_list(Str) -> list_to_binary(Str);
to_binary(Str) when is_binary(Str) -> Str.


%% @private
binary_to_hexstr(Bin) ->
  lists:flatten([io_lib:format("~2.16.0b", [X]) || X <- binary_to_list(Bin)]).

%% Returns a list of {Host, port} seeds for a given host. Performs
%% a DNS lookup of the SRV as described in the MongoDB reference manual
-spec get_srv_seeds(string() | binary()) -> {ok, [{Host :: string(), Port :: inet:port_number()}]} |
                                 {error, srv_lookup_failed | srv_insecure_endpoints}.
get_srv_seeds(Host) when is_binary(Host) ->
  get_srv_seeds(unicode:characters_to_list(Host));
get_srv_seeds(Host) ->
  Srv = "_mongodb._tcp." ++ Host,
  case inet_res:getbyname(Srv, srv) of
    {ok, #hostent{h_addr_list = Endpoints}} ->
      Seeds = [{H, P} || {_, _, P, H} <- Endpoints],
      case lists:all(fun ({H, _P}) ->
                           valid_endpoint(Host, H)
                     end,
                     Seeds)
        of
        true ->
          {ok, Seeds};
        false ->
          {error, srv_insecure_endpoints}
      end;
    {error, _Reason} ->
      {error, srv_lookup_failed}
  end.

valid_endpoint(Host, Srv) ->
  [_ | HostBaseDomain] = string:split(Host, "."),
  [_ | SrvBaseDomain] = string:split(Srv, "."),
  HostBaseDomain == SrvBaseDomain.

use_legacy_protocol(Connection) ->
  %% Latest MongoDB version that supported the non-op-msg based opcodes was
  %% 5.0.x (at the time of writing 5.0.14). The non-op-msg based opcodes were
  %% removed in MongoDB version 5.1.0. See
  %% https://www.mongodb.com/docs/manual/legacy-opcodes/
  case mc_worker_pid_info:get_protocol_type(Connection) of
    legacy -> true;
    op_msg -> false
  end.

get_connection_pid(Connection) when is_pid(Connection) ->
  Connection;
get_connection_pid(#{connection_pid := Pid}) ->
  Pid.
