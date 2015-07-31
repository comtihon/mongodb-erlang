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

-include("mongo_protocol.hrl").

-define(RANDOM_LENGTH, 24).
-define(AUTH_CMD(Login, Nonce, Password),
  {
    <<"authenticate">>, 1,
    <<"user">>, Login,
    <<"nonce">>, Nonce,
    <<"key">>, mc_utils:pw_key(Nonce, Login, Password)
  }).

%% API
-export([mongodb_cr_auth/4, scram_sha_1_auth/4, compose_first_message/2, compose_second_message/5]).

-spec mongodb_cr_auth(port(), binary(), binary(), binary()) -> true.
mongodb_cr_auth(Socket, Database, Login, Password) ->
  {true, Res} = mongo:sync_command(Socket, Database, {<<"getnonce">>, 1}),
  Nonce = maps:find(<<"nonce">>, Res),
  case mongo:sync_command(Socket, Database, ?AUTH_CMD(Login, Nonce, Password)) of
    {true, _} -> true;
    {false, Reason} -> erlang:error(Reason)
  end.

-spec scram_sha_1_auth(port(), binary(), binary(), binary()) -> boolean().
scram_sha_1_auth(Socket, Database, Login, Password) ->
  try
    scram_first_step(Socket, Database, Login, Password)
  catch
    _:_ ->
      erlang:error(<<"Can't pass authentification">>)
  end.


%% @private
scram_first_step(Socket, Database, Login, Password) ->
  RandomBString = list_to_binary(mc_utils:random_string(?RANDOM_LENGTH)),
  FirstMessage = compose_first_message(Login, RandomBString),
  Message = base64:encode(<<?GS2_HEADER/binary, FirstMessage/binary>>), %TODO investigate need of encoding!
  {true, Res} = mongo:sync_command(Socket, Database,
    {<<"saslStart">>, 1, <<"mechanism">>, <<"SCRAM-SHA-1">>, <<"payload">>, Message}),
  ConversationId = maps:get(<<"conversationId">>, Res, {}),
  Payload = maps:find(<<"payload">>, Res),
  scram_second_step(Socket, Database, Login, Password, Payload, ConversationId, RandomBString, FirstMessage).

%% @private
scram_second_step(Socket, Database, Login, Password, Payload, ConversationId, RandomBString, FirstMessage) ->
  Decoded = base64:decode(Payload),
  {Signature, ClientFinalMessage} = compose_second_message(Decoded, Login, Password, RandomBString, FirstMessage),
  {true, Res} = mongo:sync_command(Socket, Database, {<<"saslContinue">>, 1, <<"conversationId">>, ConversationId, <<"payload">>, base64:encode(ClientFinalMessage)}),
  scram_third_step(base64:encode(Signature), Res).

%% @private
scram_third_step(ServerSignature, Responce) ->
  Payload = maps:find(<<"payload">>, Responce),
  ParamList = parse_server_responce(base64:decode(Payload)),
  ServerSignature = mc_utils:get_value(<<"v">>, ParamList).

%% Export for test purposes
compose_first_message(Login, RandomBString) ->
  UserName = <<<<"n=">>/binary, (mc_utils:encode_name(Login))/binary>>,
  Nonce = <<<<"r=">>/binary, RandomBString/binary>>,
  <<UserName/binary, <<",">>/binary, Nonce/binary>>.

%% Export for test purposes
compose_second_message(Payload, Login, Password, RandomBString, FirstMessage) ->
  ParamList = parse_server_responce(Payload),
  R = mc_utils:get_value(<<"r">>, ParamList),
  Nonce = <<<<"r=">>/binary, R/binary>>,
  {0, ?RANDOM_LENGTH} = binary:match(R, [RandomBString], []),
  S = mc_utils:get_value(<<"s">>, ParamList),
  I = binary_to_integer(mc_utils:get_value(<<"i">>, ParamList)),
  SaltedPassword = hi(mc_utils:pw_hash(Login, Password), base64:decode(S), I),
  ChannelBinding = <<<<"c=">>/binary, (base64:encode(?GS2_HEADER))/binary>>,
  ClientFinalMessageWithoutProof = <<ChannelBinding/binary, <<",">>/binary, Nonce/binary>>,
  AuthMessage = <<FirstMessage/binary, <<",">>/binary, Payload/binary, <<",">>/binary, ClientFinalMessageWithoutProof/binary>>,
  ServerSignature = generate_sig(SaltedPassword, AuthMessage),
  Proof = generate_proof(SaltedPassword, AuthMessage),
  {ServerSignature, <<ClientFinalMessageWithoutProof/binary, <<",">>/binary, Proof/binary>>}.

%% @private
generate_proof(SaltedPassword, AuthMessage) ->
  ClientKey = mc_utils:hmac(SaltedPassword, <<"Client Key">>),
  StoredKey = crypto:hash(sha, ClientKey),
  Signature = mc_utils:hmac(StoredKey, AuthMessage),
  ClientProof = xorKeys(ClientKey, Signature, <<>>),
  <<<<"p=">>/binary, (base64:encode(ClientProof))/binary>>.

%% @private
generate_sig(SaltedPassword, AuthMessage) ->
  ServerKey = mc_utils:hmac(SaltedPassword, "Server Key"),
  mc_utils:hmac(ServerKey, AuthMessage).

%% @private
hi(Password, Salt, Iterations) ->
  {ok, Key} = pbkdf2:pbkdf2(sha, Password, Salt, Iterations, 20),
  Key.

%% @private
xorKeys(<<>>, _, Res) -> Res;
xorKeys(<<FA, RestA/binary>>, <<FB, RestB/binary>>, Res) ->
  xorKeys(RestA, RestB, <<Res/binary, <<(FA bxor FB)>>/binary>>).

%% @private
parse_server_responce(Responce) ->
  ParamList = binary:split(Responce, <<",">>, [global]),
  lists:map(
    fun(Param) ->
      [K, V] = binary:split(Param, <<"=">>),
      {K, V}
    end, ParamList).
