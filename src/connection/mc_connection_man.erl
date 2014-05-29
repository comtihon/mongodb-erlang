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
-export([reply/1, request_worker/1, free_worker/2, request/2]).

-spec request(pid() | term(), term()) -> ok | {non_neg_integer(), [bson:document()]}.
request({pool, PoolName}, Request) -> %request to pool
	reply(poolboy:transaction(PoolName,
		fun(Worker) ->
			gen_server:call(Worker, Request, infinity)
		end));
request(Connection, Request) ->  %request to worker
	reply(gen_server:call(Connection, Request, infinity)).

request_worker(PoolName) ->
	poolboy:checkout(PoolName).
free_worker(PoolName, Worker) ->
	poolboy:checkin(PoolName, Worker).

reply(ok) -> ok;
reply(#reply{cursornotfound = false, queryerror = false} = Reply) ->
	{Reply#reply.cursorid, Reply#reply.documents};
reply(#reply{cursornotfound = false, queryerror = true} = Reply) ->
	[Doc | _] = Reply#reply.documents,
	process_error(bson:at(code, Doc), Doc);
reply(#reply{cursornotfound = true, queryerror = false} = Reply) ->
	erlang:error({bad_cursor, Reply#reply.cursorid}).

process_error(13435, _) ->
	erlang:error(not_master);
process_error(10057, _) ->
	erlang:error(unauthorized);
process_error(_, Doc) ->
	erlang:error({bad_query, Doc}).