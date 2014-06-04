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

%% API
-export([encode_request/2, decode_responses/1, process_responses/2, process_write_response/1]).
-export([gen_index_name/1, make_request/3]).

encode_request(Database, Request) ->
	RequestId = mongo_id_server:request_id(), %TODO uuid:generate
	Payload = mongo_protocol:put_message(Database, Request),
	{<<(byte_size(Payload) + 4):32/little, Payload/binary>>, RequestId}.

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
	fun({0, [Ack | _]}) ->
		case bson:lookup(err, Ack, undefined) of
			undefined -> gen_server:reply(From, ok);
			String ->
				case bson:at(code, Ack) of
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
	bson:doc_foldl(fun(Label, Order, Acc) ->
		<<Acc/binary, $_, (value_to_binary(Label))/binary, $_, (value_to_binary(Order))/binary>>
	end, <<"i">>, KeyOrder).

make_request(Socket, Database, Request) ->
	{Packet, Id} = encode_request(Database, Request),
	{gen_tcp:send(Socket, Packet), Id}.

%% @private
value_to_binary(Value) when is_integer(Value) ->
	bson:utf8(integer_to_list(Value));
value_to_binary(Value) when is_atom(Value) ->
	atom_to_binary(Value, utf8);
value_to_binary(Value) when is_binary(Value) ->
	Value;
value_to_binary(_Value) ->
	<<>>.