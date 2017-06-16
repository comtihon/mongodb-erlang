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
-export([auth/6]).

%% Authorize on database synchronously
-spec auth(float(), port(), database(), binary() | undefined, binary() | undefined, module()) -> boolean().
auth(Version, Socket, Database, Login, Password, NetModule) when Version > 2.7 ->  %new authorisation
  scram_sha_1_auth(Socket, Database, Login, Password, NetModule);
auth(_, Socket, Database, Login, Password, NetModule) ->   %old authorisation
  mongodb_cr_auth(Socket, Database, Login, Password, NetModule).


%% @private
-spec mongodb_cr_auth(port(), binary(), binary(), binary(), module()) -> boolean().
mongodb_cr_auth(Socket, Database, Login, Password, SetOpts) ->
  {true, Res} = mc_worker_api:sync_command(Socket, Database, {<<"getnonce">>, 1}, SetOpts),
  Nonce = maps:get(<<"nonce">>, Res),
  case mc_worker_api:sync_command(Socket, Database, ?AUTH_CMD(Login, Nonce, Password), SetOpts) of
    {true, _} -> true;
    {false, Reason} -> erlang:error(Reason)
  end.

%% @private
-spec scram_sha_1_auth(port(), binary(), binary(), binary(), module()) -> boolean().
scram_sha_1_auth(Socket, Database, Login, Password, SetOpts) ->
  try
    scram_first_step(Socket, Database, Login, Password, SetOpts)
  catch
    _:_ ->
      erlang:error(<<"Can't pass authentification">>)
  end.

%% @private
scram_first_step(Socket, Database, Login, Password, SetOpts) ->
  RandomBString = mc_utils:random_nonce(?RANDOM_LENGTH),
  FirstMessage = compose_first_message(Login, RandomBString),
  Message = base64:encode(<<?GS2_HEADER/binary, FirstMessage/binary>>),
  {true, Res} = mc_worker_api:sync_command(Socket, Database,
    {<<"saslStart">>, 1, <<"mechanism">>, <<"SCRAM-SHA-1">>, <<"autoAuthorize">>, 1, <<"payload">>, Message}, SetOpts),
  ConversationId = maps:get(<<"conversationId">>, Res, {}),
  Payload = maps:get(<<"payload">>, Res),
  scram_second_step(Socket, Database, Login, Password, Payload, ConversationId, RandomBString, FirstMessage, SetOpts).

%% @private
scram_second_step(Socket, Database, Login, Password, Payload, ConversationId, RandomBString, FirstMessage, SetOpts) ->
  Decoded = base64:decode(Payload),
  {Signature, ClientFinalMessage} = compose_second_message(Decoded, Login, Password, RandomBString, FirstMessage),
  {true, Res} = mc_worker_api:sync_command(Socket, Database, {<<"saslContinue">>, 1, <<"conversationId">>, ConversationId,
    <<"payload">>, base64:encode(ClientFinalMessage)}, SetOpts),
  scram_third_step(base64:encode(Signature), Res, ConversationId, Socket, Database, SetOpts).

%% @private
scram_third_step(ServerSignature, Response, ConversationId, Socket, Database, SetOpts) ->
  Payload = maps:get(<<"payload">>, Response),
  Done = maps:get(<<"done">>, Response, false),
  ParamList = parse_server_responce(base64:decode(Payload)),
  ServerSignature = mc_utils:get_value(<<"v">>, ParamList),
  scram_forth_step(Done, ConversationId, Socket, Database, SetOpts).

%% @private
scram_forth_step(true, _, _, _, _) -> true;
scram_forth_step(false, ConversationId, Socket, Database, SetOpts) ->
  {true, Res} = mc_worker_api:sync_command(Socket, Database, {<<"saslContinue">>, 1, <<"conversationId">>,
    ConversationId, <<"payload">>, <<>>}, SetOpts),
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