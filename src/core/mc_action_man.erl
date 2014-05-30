%%%-------------------------------------------------------------------
%%% @author tihon
%%% @copyright (C) 2014, <COMPANY>
%%% @doc  mongo connection action manager.
%%% It processes all write|read|other requests from mongo api
%%% @end
%%% Created : 28. май 2014 18:27
%%%-------------------------------------------------------------------
-module(mc_action_man).
-author("tihon").

-include("mongo_protocol.hrl").

%% API
-export([read/2, read_one/2, do/5]).

%% @doc Execute mongo action under given write_mode, read_mode, connection, and database.
-spec do(connection(), write_mode(), read_mode(), database(), action(A)) -> A.
do({pool, PoolName}, WriteMode, ReadMode, Database, Action) ->
	Worker = mc_connection_man:request_worker(PoolName),
	try
		do(Worker, WriteMode, ReadMode, Database, Action)
	after mc_connection_man:free_worker(PoolName, Worker)
	end;

do(Connection, WriteMode, ReadMode, Database, Action) ->
	gen_server:call(Connection, #conn_state{database = Database, write_mode = WriteMode, read_mode = ReadMode}),
	Action(Connection).

read(Connection, Request = #'query'{collection = Collection, batchsize = BatchSize} ) ->
	{Cursor, Batch} = mc_connection_man:request(Connection, Request),
	mongo_cursor:create(Connection, Database, Collection, Cursor, BatchSize, Batch).  %TODO when pool Coonection will be {atom, PoolName}!! Resolv me
%TODO cursor, what is it 0_o
read_one(Connection, Request) ->
	{0, Docs} = mc_connection_man:request(Connection, Request#'query'{batchsize = -1}),
	case Docs of
		[] -> {};
		[Doc | _] -> {Doc}
	end.