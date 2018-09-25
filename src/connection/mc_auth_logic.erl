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

-ifdef(TEST).
-compile(export_all).
-endif.

-define(RANDOM_LENGTH, 24).
-define(AUTH_CMD(Login, Nonce, Password),
  {
    <<"authenticate">>, 1,
    <<"user">>, Login,
    <<"nonce">>, Nonce,
    <<"key">>, mc_utils:pw_key(Nonce, Login, Password)
  }).

%% API
-export([auth/5]).

%% Authorize on database synchronously
-spec auth(pid(), float(), database(), binary() | undefined, binary() | undefined) -> boolean().
auth(Connection, Version, Database, Login, Password) when Version > 2.7 ->  %new authorisation
  scram_sha_1_auth(Connection, Database, Login, Password);
auth(Connection, _, Database, Login, Password) ->   %old authorisation
  mongodb_cr_auth(Connection, Database, Login, Password).


%% @private
-spec mongodb_cr_auth(pid(), binary(), binary(), binary()) -> boolean().
mongodb_cr_auth(Connection, Database, Login, Password) ->
  {true, Res} = mc_connection_man:database_command(Connection, Database, {<<"getnonce">>, 1}),
  Nonce = maps:get(<<"nonce">>, Res),
  case mc_connection_man:database_command(Connection, Database, ?AUTH_CMD(Login, Nonce, Password)) of
    {true, _} -> true;
    {false, Reason} -> erlang:error(Reason)
  end.

%% @private
-spec scram_sha_1_auth(port(), binary(), binary(), binary()) -> boolean().
scram_sha_1_auth(Connection, Database, Login, Password) ->
  try
    scram_first_step(Connection, Database, Login, Password)
  catch
    _:_ ->
      erlang:error(<<"Can't pass authentification">>)
  end.

%% @private
scram_first_step(Connection, Database, Login, Password) ->
  RandomBString = mc_utils:random_nonce(?RANDOM_LENGTH),
  FirstMessage = compose_first_message(Login, RandomBString),
  Message = <<?GS2_HEADER/binary, FirstMessage/binary>>,
  {true, Res} = mc_connection_man:database_command(Connection, Database,
    {<<"saslStart">>, 1, <<"mechanism">>, <<"SCRAM-SHA-1">>, <<"payload">>, {bin, bin, Message}, <<"autoAuthorize">>, 1}),
  ConversationId = maps:get(<<"conversationId">>, Res, {}),
  Payload = maps:get(<<"payload">>, Res),
  scram_second_step(Connection, Database, Login, Password, Payload, ConversationId, RandomBString, FirstMessage).

%% @private
scram_second_step(Connection, Database, Login, Password, {bin, bin, Decoded} = _Payload, ConversationId, RandomBString, FirstMessage) ->
  {Signature, ClientFinalMessage} = compose_second_message(Decoded, Login, Password, RandomBString, FirstMessage),
  {true, Res} = mc_connection_man:database_command(Connection, Database, {<<"saslContinue">>, 1, <<"conversationId">>, ConversationId,
    <<"payload">>, {bin, bin, ClientFinalMessage}}),
  scram_third_step(Connection, base64:encode(Signature), Res, ConversationId, Database).

%% @private
scram_third_step(Connection, ServerSignature, Response, ConversationId, Database) ->
  {bin, bin, Payload} = maps:get(<<"payload">>, Response),
  Done = maps:get(<<"done">>, Response, false),
  ParamList = parse_server_responce(Payload),
  ServerSignature = mc_utils:get_value(<<"v">>, ParamList),
  scram_forth_step(Connection, Done, ConversationId, Database).

%% @private
scram_forth_step(_, true, _, _) -> true;
scram_forth_step(Connection, false, ConversationId, Database) ->
  {true, Res} = mc_connection_man:database_command(Connection, Database, {<<"saslContinue">>, 1, <<"conversationId">>,
    ConversationId, <<"payload">>, {bin, bin, <<>>}}),
  true = maps:get(<<"done">>, Res, false).

%% @private
compose_first_message(Login, RandomBString) ->
  UserName = <<<<"n=">>/binary, (mc_utils:encode_name(Login))/binary>>,
  Nonce = <<<<"r=">>/binary, RandomBString/binary>>,
  <<UserName/binary, <<",">>/binary, Nonce/binary>>.

%% @private
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
