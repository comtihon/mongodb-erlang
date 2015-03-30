%%%-------------------------------------------------------------------
%%% @author tihon
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 29. Mar 2015 6:50 PM
%%%-------------------------------------------------------------------
-module(mc_auth_logic).
-author("tihon").

-define(RANDOM_LENGTH, 24).
-define(GS2_HEADER, <<"n,,">>).

%% API
-export([mongodb_cr_auth/4, scram_sha_1_auth/4]).

-spec mongodb_cr_auth(port(), binary(), binary(), binary()) -> true.
mongodb_cr_auth(Socket, Database, Login, Password) ->
  {true, Res} = mongo:sync_command(Socket, Database, {getnonce, 1}),
  Nonce = bson:at(nonce, Res),
  case mongo:sync_command(Socket, Database, {authenticate, 1, user, Login, nonce, Nonce, key, mc_utils:pw_key(Nonce, Login, Password)}) of
    {true, _} -> true;
    {false, Reason} -> erlang:error(Reason)
  end.

scram_sha_1_auth(Socket, Database, Login, Password) ->
  scram_first_step(Socket, Database, Login, Password),
  ok.

scram_first_step(Socket, Database, Login, Password) ->
  UserName = <<<<"n=">>/binary, (mc_utils:encode_name(Login))/binary>>,
  RandomBString = list_to_binary(mc_utils:random_string(?RANDOM_LENGTH)),
  Nonce = <<<<"r=">>/binary, RandomBString/binary>>,
  FirstMessage = <<UserName/binary, <<",">>/binary, Nonce/binary>>,
  Message = <<?GS2_HEADER/binary, FirstMessage/binary>>,
  {true, Res} = mongo:sync_command(Socket, Database,
    {<<"saslStart">>, 1, <<"mechanism">>, <<"SCRAM-SHA-1">>, <<"payload">>, Message}),
  {ConversationId} = bson:lookup(conversationId, Res),
  {Payload} = bson:lookup(payload, Res),
  scram_second_step(Socket, Database, Login, Password, Payload, ConversationId, RandomBString, FirstMessage).

scram_second_step(Socket, Database, Login, Password, Payload, ConversationId, RandomBString, FirstMessage) -> %TODO refactor huge method
  Decoded = base64:decode(Payload),
  ParamList = parse_server_responce(Decoded),
  R = mc_utils:get_value(<<"r">>, ParamList),
  Nonce = <<<<"r=">>/binary, R/binary>>,
  {0, ?RANDOM_LENGTH} = binary:match(R, [RandomBString], []),
  S = mc_utils:get_value(<<"s">>, ParamList),
  I = binary_to_integer(mc_utils:get_value(<<"i">>, ParamList)),
  Pass = mc_utils:pw_hash(Login, Password),
  SaltedPassword = hi(Pass, base64:decode(S), I),
  {ClientKey, StoredKey} = compose_keys(SaltedPassword, <<"Client Key">>),
  ChannelBinding = <<<<"c=">>/binary, (base64:encode(?GS2_HEADER))/binary>>,
  ClientFinalMessageWithoutProof = <<ChannelBinding/binary, <<",">>/binary, Nonce/binary>>,
  AuthMessage = <<FirstMessage/binary, <<",">>/binary, Decoded/binary, <<",">>/binary, ClientFinalMessageWithoutProof/binary>>,
  Signature = mc_utils:hmac(StoredKey, AuthMessage),
  ClientProof = xorKeys(ClientKey, Signature, <<>>),
  ServerKey = mc_utils:hmac(SaltedPassword, "Server Key"),
  ServerSignature = mc_utils:hmac(ServerKey, AuthMessage),
  Proof = <<<<"p=">>/binary, (base64:encode(ClientProof))/binary>>,
  ClientFinalMessage = <<ClientFinalMessageWithoutProof/binary, <<",">>/binary, Proof/binary>>,
  {true, Res} = mongo:sync_command(Socket, Database, {<<"saslContinue">>, 1, <<"conversationId">>, ConversationId, <<"payload">>, ClientFinalMessage})
.


%% @private
hi(Password, Salt, Iterations) ->
  {ok, Key} = pbkdf2:pbkdf2(sha, Password, Salt, Iterations, 160), %20 bytes
  Key.

%% @private
compose_keys(Pass, Key) ->
  ClientKey = mc_utils:hmac(Pass, Key),
  {ClientKey, crypto:hash(sha, ClientKey)}.

%% @private
xorKeys(<<>>, _, Res) -> Res;
xorKeys(<<FA, RestA/binary>>, <<FB, RestB/binary>>, Res) ->
  xorKeys(RestA, RestB, <<Res/binary, <<(FA bxor FB)>>/binary>>).

%% @private
parse_server_responce(Responce) ->
  ParamList = binary:split(Responce, <<",">>, [global]),
  lists:foldl(
    fun(Param, Acc) ->
      [K, V] = binary:split(Param, <<"=">>),
      [{K, V} | Acc]
    end, [], ParamList).
