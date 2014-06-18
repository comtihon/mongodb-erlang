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
-export([encode_requests/2, decode_responses/1, process_responses/2, process_write_response/1]).
-export([gen_index_name/1, make_request/3]).

encode_requests(Database, Request) when not is_list(Request) -> encode_requests(Database, [Request]);
encode_requests(Database, Request) ->
	Build = fun(Message, {Bin, _}) ->
		RequestId = mongo_id_server:request_id(),
		Payload = mongo_protocol:put_message(Database, Message, RequestId),
		{<<Bin/binary, (byte_size(Payload) + 4):32/little, Payload/binary>>, RequestId} end,
	lists:foldl(Build, {<<>>, 0}, Request).

decode_responses(Data) ->
	decode_responses(Data, []).

decode_responses(<<Length:32/signed-little, Data/binary>>, Acc) when byte_size(Data) >= (Length - 4) ->
	PayloadLength = Length - 4,
	<<Payload:PayloadLength/binary, Rest/binary>> = Data,
	{Id, Response, <<>>} = mongo_protocol:get_reply(Payload),
	decode_responses(Rest, [{Id, Response} | Acc]);
decode_responses(Data, Acc) ->
	{lists:reverse(Acc), Data}.

%% Returns function for processing requests to From pid on write operations
-spec process_write_response(From :: pid()) -> fun().
process_write_response(From) ->
	fun(#reply{documents = [Doc]}) ->
		case bson:lookup(err, Doc, undefined) of
			undefined -> gen_server:reply(From, ok);
			String ->
				case bson:at(code, Doc) of
					10058 -> gen_server:reply(From, {error, {not_master, 10058}});
					Code -> gen_server:reply(From, {error, {write_failure, Code, String}})
				end
		end
	end.

process_responses(Responses, Ets) ->
	lists:foreach(
		fun({Id, Response}) ->
			case ets:lookup(Ets, Id) of
				[] -> % TODO: close any cursor that might be linked to this request ?
					ok;
				[{Id, Fun}] ->
					ets:delete(Ets, Id),
					catch Fun(Response) % call on-response function
			end
		end, Responses).

gen_index_name(KeyOrder) ->
	bson:doc_foldl(
		fun(Label, Order, Acc) ->
			<<Acc/binary, $_, (mc_connection_man:value_to_binary(Label))/binary,
			$_, (mc_connection_man:value_to_binary(Order))/binary>>
		end, <<"i">>, KeyOrder).

make_request(Socket, Database, Request) ->
	{Packet, Id} = encode_requests(Database, Request),
	{gen_tcp:send(Socket, Packet), Id}.