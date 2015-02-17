%%%-------------------------------------------------------------------
%%% @author tihon
%%% @copyright (C) 2014, <COMPANY>
%%% @doc  mongo connection action manager.
%%% It processes read requests from mongo api
%%% @end
%%% Created : 28. май 2014 18:27
%%%-------------------------------------------------------------------
-module(mc_action_man).
-author("tihon").

-include("mongo_protocol.hrl").

%% API
-export([read/2, read_one/2, read_one_sync/3]).

read(Connection, Request = #'query'{collection = Collection, batchsize = BatchSize} ) ->
	{Cursor, Batch} = mc_connection_man:request_async(Connection, Request),
	mc_cursor:create(Connection, Collection, Cursor, BatchSize, Batch).

read_one(Connection, Request) ->
	{0, Docs} = mc_connection_man:request_async(Connection, Request#'query'{batchsize = -1}),
	case Docs of
		[] -> {};
		[Doc | _] -> {Doc}
	end.

read_one_sync(Socket, Database, Request) ->
  {0, Docs} = mc_connection_man:request_sync(Socket, Database, Request#'query'{batchsize = -1}),
	case Docs of
		[] -> {};
		[Doc | _] -> {Doc}
	end.