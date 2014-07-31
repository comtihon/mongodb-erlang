%%%-------------------------------------------------------------------
%%% @author tihon
%%% @copyright (C) 2014, <COMPANY>
%%% @doc  mongo client connection manager
%%% processes request and response to|from database
%%% @end
%%% Created : 28. май 2014 18:37
%%%-------------------------------------------------------------------
-module(mc_connection_man).
-author("tihon").

-include("mongo_protocol.hrl").

%% API
-export([reply/1, request/2, pw_key/3, process_reply/2, value_to_binary/1]).

-spec request(pid(), term()) -> ok | {non_neg_integer(), [bson:document()]}.
request(Connection, Request) ->  %request to worker
	Timeout = case application:get_env(mc_worker_call_timeout) of
		          {ok, Time} -> Time;
		          undefined -> infinity
	          end,
	reply(gen_server:call(Connection, Request, Timeout)).

reply(ok) -> ok;
reply(#reply{cursornotfound = false, queryerror = false} = Reply) ->
	{Reply#reply.cursorid, Reply#reply.documents};
reply(#reply{cursornotfound = false, queryerror = true} = Reply) ->
	[Doc | _] = Reply#reply.documents,
	process_error(bson:at(code, Doc), Doc);
reply(#reply{cursornotfound = true, queryerror = false} = Reply) ->
	erlang:error({bad_cursor, Reply#reply.cursorid}).

process_reply(Doc, Command) ->
	case bson:lookup(ok, Doc) of
		{N} when N == 1 -> {true, bson:exclude([ok], Doc)};   %command succeed
		{N} when N == 0 -> {false, bson:exclude([ok], Doc)};  %command failed
		_Res -> erlang:error({bad_command, Doc}, [Command]) %unknown result
	end.

process_error(13435, _) ->
	erlang:error(not_master);
process_error(10057, _) ->
	erlang:error(unauthorized);
process_error(_, Doc) ->
	erlang:error({bad_query, Doc}).

pw_key(Nonce, Username, Password) ->
	bson:utf8(binary_to_hexstr(crypto:hash(md5, [Nonce, Username, pw_hash(Username, Password)]))).

%% @private
pw_hash(Username, Password) ->
	bson:utf8(binary_to_hexstr(crypto:hash(md5, [Username, <<":mongo:">>, Password]))).

%% @private
binary_to_hexstr(Bin) ->
	lists:flatten([io_lib:format("~2.16.0b", [X]) || X <- binary_to_list(Bin)]).

value_to_binary(Value) when is_integer(Value) ->
	bson:utf8(integer_to_list(Value));
value_to_binary(Value) when is_atom(Value) ->
	atom_to_binary(Value, utf8);
value_to_binary(Value) when is_binary(Value) ->
	Value;
value_to_binary(_Value) ->
	<<>>.