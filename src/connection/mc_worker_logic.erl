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
-export([encode_requests/2, decode_responses/1, process_responses/2]).
-export([gen_index_name/1, make_request/4, get_resp_fun/2, update_dbcoll/2, collection/1]).

encode_requests(Database, Request) when not is_list(Request) ->
  encode_requests(Database, [Request]);
encode_requests(Database, Request) ->
  Build =
    fun(Message, {Bin, _}) ->
      RequestId = mongo_id_server:request_id(),
      Payload = mongo_protocol:put_message(Database, Message, RequestId),
      {<<Bin/binary, (byte_size(Payload) + 4):32/little, Payload/binary>>, RequestId}
    end,
  lists:foldl(Build, {<<>>, 0}, Request).

decode_responses(Data) ->
  decode_responses(Data, []).

-spec get_resp_fun(#query{} | #getmore{} | #insert{} | #update{} | #delete{}, pid()) -> fun().
get_resp_fun(Read, From) when is_record(Read, query); is_record(Read, getmore) ->
  fun(Response) -> gen_server:reply(From, Response) end;
get_resp_fun(Write, From) when is_record(Write, insert); is_record(Write, update); is_record(Write, delete) ->
  process_write_response(From).

-spec process_responses(Responses :: list(), RequestStorage :: dict:dict()) -> UpdStorage :: dict:dict().
process_responses(Responses, RequestStorage) ->
  lists:foldl(
    fun({Id, Response}, UReqStor) ->
      case dict:find(Id, UReqStor) of
        error -> % TODO: close any cursor that might be linked to this request ?
          UReqStor;
        {ok, Fun} ->
          UpdReqStor = dict:erase(Id, UReqStor),
          catch Fun(Response), % call on-response function
          UpdReqStor
      end
    end, RequestStorage, Responses).

gen_index_name(KeyOrder) ->
  bson:doc_foldl(
    fun(Label, Order, Acc) ->
      <<Acc/binary, $_, (mc_utils:value_to_binary(Label))/binary,
      $_, (mc_utils:value_to_binary(Order))/binary>>
    end, <<"i">>, KeyOrder).

make_request(Socket, NetModule, Database, Request) ->
  {Packet, Id} = encode_requests(Database, Request),
  {NetModule:send(Socket, Packet), Id}.

update_dbcoll({Db, _}, Coll) -> {Db, Coll};
update_dbcoll(_, Coll) -> Coll.

collection(#'query'{collection = Coll}) -> Coll;
collection(#'insert'{collection = Coll}) -> Coll;
collection(#'update'{collection = Coll}) -> Coll;
collection(#'delete'{collection = Coll}) -> Coll.


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