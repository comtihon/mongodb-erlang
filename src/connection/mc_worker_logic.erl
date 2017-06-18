%%%-------------------------------------------------------------------
%%% @author tihon
%%% @copyright (C) 2014, <COMPANY>
%%% @doc  module for logic functions of worker, not to harden mc_worker module with them
%%%
%%% @end
%%% Created : 04. июн 2014 18:02
%%%-------------------------------------------------------------------
-module(mc_worker_logic).
-author("tihon").

-include("mongo_protocol.hrl").

%% API
-export([encode_request/2, decode_responses/1, process_responses/2, connect_to_database/1, get_version/3]).
-export([make_request/4, get_resp_fun/2, update_dbcoll/2, collection/1, ensure_index/3]).

%% Make connection to database and return socket
-spec connect_to_database(proplists:proplist()) -> {ok, port()} | {error, inet:posix()}.
connect_to_database(Conf) ->
  Timeout = mc_utils:get_value(timeout, Conf, infinity),
  Host = mc_utils:get_value(host, Conf, "127.0.0.1"),
  Port = mc_utils:get_value(port, Conf, 27017),
  SSL = mc_utils:get_value(ssl, Conf, false),
  SslOpts = mc_utils:get_value(ssl_opts, Conf, []),
  do_connect(Host, Port, Timeout, SSL, SslOpts).

%% Get server version. This is need to choose default authentication method.
-spec get_version(port(), binary(), module()) -> float().
get_version(Socket, Database, SetOpts) ->
  {true, #{<<"version">> := Version}} = mc_worker_api:sync_command(Socket, Database, {<<"buildinfo">>, 1}, SetOpts),
  {VFloat, _} = string:to_float(binary_to_list(Version)),
  VFloat.

-spec encode_request(mc_worker_api:database(), mongo_protocol:message() | list(mongo_protocol:message())) -> {iodata(), pos_integer()}.
encode_request(Database, Request) when is_list(Request) ->
  lists:foldl(fun(Message, {Bin, _}) ->
    {NewBin, NewId} = encode_request(Database, Message),
    {Bin ++ [NewBin], NewId}
  end, {[], 0}, Request);
  
encode_request(Database, Request) ->
  RequestId = mongo_id_server:request_id(),
  Payload = mongo_protocol:put_message(Database, Request, RequestId),
  {<<(byte_size(Payload) + 4):32/little, Payload/binary>>, RequestId}.

decode_responses(Data) ->
  decode_responses(Data, []).

-spec get_resp_fun(#query{} | #getmore{} | #insert{} | #update{} | #delete{}, pid()) -> fun().
get_resp_fun(Read, From) when is_record(Read, query); is_record(Read, getmore) ->
  fun(Response) -> gen_server:reply(From, Response) end;
get_resp_fun(Write, From) when is_record(Write, insert); is_record(Write, update); is_record(Write, delete) ->
  process_write_response(From).

-spec process_responses(Responses :: list(), RequestStorage :: map()) -> UpdStorage :: map().
process_responses(Responses, RequestStorage) ->
  lists:foldl(
    fun({Id, Response}, UReqStor) ->
      case maps:find(Id, UReqStor) of
        error ->
          UReqStor;
        {ok, Fun} ->
          UpdReqStor = maps:remove(Id, UReqStor),
          try Fun(Response) % call on-response function
          catch _:_ -> ok
          end,
          UpdReqStor
      end
    end, RequestStorage, Responses).

-spec make_request(gen_tcp:socket() | ssl:sslsocket(), atom(), mc_worker_api:database(), mongo_protocol:message() | list(mongo_protocol:message())) ->
  {ok | {error, any()}, integer(), pos_integer()}.
make_request(Socket, NetModule, Database, Request) ->
  {Packet, Id} = encode_request(Database, Request),
  {NetModule:send(Socket, Packet), iolist_size(Packet), Id}.

update_dbcoll({Db, _}, Coll) -> {Db, Coll};
update_dbcoll(_, Coll) -> Coll.

collection(#'query'{collection = Coll}) -> Coll;
collection(#'insert'{collection = Coll}) -> Coll;
collection(#'update'{collection = Coll}) -> Coll;
collection(#'delete'{collection = Coll}) -> Coll.

ensure_index(IndexSpec = #{<<"key">> := Key}, Database, Collection) ->
  do_ensure_index(IndexSpec, Database, Collection, Key);
ensure_index(IndexSpec, Database, Collection) when is_tuple(IndexSpec) ->
  Key = bson:lookup(<<"key">>, IndexSpec),
  do_ensure_index(IndexSpec, Database, Collection, Key).


%% @private
do_ensure_index(IndexSpec, Database, Collection, Key) ->
  Defaults = {<<"name">>, gen_index_name(Key), <<"unique">>, false, <<"dropDups">>, false},
  bson:update(<<"ns">>,
    mongo_protocol:dbcoll(Database, Collection),
    bson:merge(IndexSpec, Defaults)).

%% @private
gen_index_name(KeyOrder) ->
  bson:doc_foldl(
    fun(Label, Order, Acc) ->
      <<Acc/binary, $_, (mc_utils:value_to_binary(Label))/binary,
        $_, (mc_utils:value_to_binary(Order))/binary>>
    end, <<"i">>, KeyOrder).

%% @private
decode_responses(<<Length:32/signed-little, Data/binary>>, Acc) when byte_size(Data) >= (Length - 4) ->
  PayloadLength = Length - 4,
  <<Payload:PayloadLength/binary, Rest/binary>> = Data,
  {Id, Response, <<>>} = mongo_protocol:get_reply(Payload),
  decode_responses(Rest, [{Id, Response} | Acc]);
decode_responses(Data, Acc) ->
  {lists:reverse(Acc), Data}.

%% @private
%% Returns function for processing requests to From pid on write operations
-spec process_write_response(From :: pid()) -> fun().
process_write_response(From) ->
  fun(#reply{documents = [Doc]}) ->
    case maps:get(<<"err">>, Doc, undefined) of
      undefined -> gen_server:reply(From, ok);
      String ->
        case maps:get(<<"code">>, Doc) of
          10058 -> gen_server:reply(From, {error, {not_master, 10058}});
          Code -> gen_server:reply(From, {error, {write_failure, Code, String}})
        end
    end
  end.

%% @private
do_connect(Host, Port, Timeout, true, Opts) ->
  {ok, _} = application:ensure_all_started(ssl),
  ssl:connect(Host, Port, [binary, {active, true}, {packet, raw}] ++ Opts, Timeout);
do_connect(Host, Port, Timeout, false, _) ->
  gen_tcp:connect(Host, Port, [binary, {active, true}, {packet, raw}], Timeout).